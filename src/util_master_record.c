#include<wale/util_master_record.h>

#include<wale/block_io_ops_util.h>
#include<wale/crc32_util.h>

#include<serint/serial_int.h>

#include<stdlib.h>

int read_master_record(master_record* mr, const block_io_ops* block_io_functions, int* error)
{
	void* mr_serial = aligned_alloc(block_io_functions->block_size, block_io_functions->block_buffer_alignment);
	if(mr_serial == NULL)
	{
		(*error) = ALLOCATION_FAILED;
		return 0;
	}

	int io_success = block_io_functions->read_blocks(block_io_functions->block_io_ops_handle, mr_serial, 0, 1);
	if(!io_success)
	{
		(*error) = READ_IO_ERROR;
		free(mr_serial);
		return 0;
	}

	// deserialize
	mr->log_sequence_number_width = deserialize_uint32(mr_serial, sizeof(uint32_t));

	if(mr->log_sequence_number_width == 0 || mr->log_sequence_number_width > get_max_bytes_uint256())
	{
		(*error) = LOG_SEQUENCE_NUMBER_UNREPRESENTABLE;
		free(mr_serial);
		return 0;
	}

	mr->first_log_sequence_number = deserialize_uint256(mr_serial + sizeof(uint32_t), mr->log_sequence_number_width);
	mr->last_flushed_log_sequence_number = deserialize_uint256(mr_serial + sizeof(uint32_t) + mr->log_sequence_number_width, mr->log_sequence_number_width);
	mr->check_point_log_sequence_number = deserialize_uint256(mr_serial + sizeof(uint32_t) + 2 * mr->log_sequence_number_width, mr->log_sequence_number_width);
	mr->next_log_sequence_number = deserialize_uint256(mr_serial + sizeof(uint32_t) + 3 * mr->log_sequence_number_width, mr->log_sequence_number_width);

	uint32_t parsed_crc32 = deserialize_uint32(mr_serial + sizeof(uint32_t) + 4 * mr->log_sequence_number_width, sizeof(uint32_t));

	// calculate crc32 for master record, NOTE :: we can not calculate crc32 without reading the log_sequence_number_width
	uint32_t calculated_crc32 = crc32_init();
	calculated_crc32 = crc32_util(calculated_crc32, mr_serial, sizeof(uint32_t) + 4 * mr->log_sequence_number_width);

	free(mr_serial);

	if(calculated_crc32 != parsed_crc32)
	{
		(*error) = MASTER_RECORD_CORRUPTED;
		return 0;
	}

	return 1;
}

int write_and_flush_master_record(const master_record* mr, const block_io_ops* block_io_functions, int* error)
{
	void* mr_serial = aligned_alloc(block_io_functions->block_size, block_io_functions->block_buffer_alignment);
	if(mr_serial == NULL)
	{
		(*error) = ALLOCATION_FAILED;
		return 0;
	}

	// reset mr_serial bytes to 0s
	memory_set(mr_serial, 0, block_io_functions->block_size);

	// serialize
	serialize_uint32(mr_serial, sizeof(uint32_t), mr->log_sequence_number_width);
	serialize_uint256(mr_serial + sizeof(uint32_t), mr->log_sequence_number_width, mr->first_log_sequence_number);
	serialize_uint256(mr_serial + sizeof(uint32_t) + mr->log_sequence_number_width, mr->log_sequence_number_width, mr->last_flushed_log_sequence_number);
	serialize_uint256(mr_serial + sizeof(uint32_t) + 2 * mr->log_sequence_number_width, mr->log_sequence_number_width, mr->check_point_log_sequence_number);
	serialize_uint256(mr_serial + sizeof(uint32_t) + 3 * mr->log_sequence_number_width, mr->log_sequence_number_width, mr->next_log_sequence_number);

	// calculate crc32 for master record
	uint32_t calculated_crc32 = crc32_init();
	calculated_crc32 = crc32_util(calculated_crc32, mr_serial, sizeof(uint32_t) + 4 * mr->log_sequence_number_width);

	// write calculated_crc32 on the mr_serial
	serialize_uint32(mr_serial + sizeof(uint32_t) + 4 * mr->log_sequence_number_width, sizeof(uint32_t), calculated_crc32);

	int io_success = block_io_functions->write_blocks(block_io_functions->block_io_ops_handle, mr_serial, 0, 1)
						&& block_io_functions->flush_all_writes(block_io_functions->block_io_ops_handle);

	free(mr_serial);

	if(!io_success)
	{
		(*error) = WRITE_IO_ERROR;
		return 0;
	}

	return 1;
}

uint64_t read_latest_vacant_block_using_master_record(void* buffer, const master_record* mr, const block_io_ops* block_io_functions, int* error)
{
	// calculate file_offset of next_log_sequence_number
	uint64_t file_offset = get_file_offset_for_next_log_sequence_number(mr, block_io_functions, error);
	if(*error)
		return 0;

	uint64_t bytes_to_read = get_block_offset_from_file_offset(file_offset, block_io_functions);
	uint64_t block_id = get_block_id_from_file_offset(file_offset, block_io_functions);

	if(bytes_to_read > 0)
	{
		if(!block_io_functions->read_blocks(block_io_functions->block_io_ops_handle, buffer, block_id, 1))
		{
			(*error) = READ_IO_ERROR;
			return 0;
		}
	}

	return file_offset;
}

uint64_t get_file_offset_for_log_sequence_number(uint256 log_sequence_number, const master_record* mr, const block_io_ops* block_io_functions, int skip_flushed_checks, int* error)
{
	// if the wale has no records, OR the log_sequence_number is not within first and last_flushed log_sequence_number then fail
	if( are_equal_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER) ||
		(!skip_flushed_checks && are_equal_uint256(mr->first_log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER)) ||  // this check is to be done only if we are not allowed to skip_flush_checks
		compare_uint256(log_sequence_number, mr->first_log_sequence_number) < 0 ||
		(!skip_flushed_checks && compare_uint256(mr->last_flushed_log_sequence_number, log_sequence_number) < 0) // this check is to be done only if we are not allowed to skip_flush_checks
		)
	{
		(*error) = PARAM_INVALID;
		return 0;
	}

	// in this file for this master record
	// the LSN_assigned_from is either from first_log_sequence_number OR next_log_sequence_number, if the first_log_sequence_number is INVALID_LOG_SEQUENCE_NUMBER
	uint256 LSN_assigned_from = mr->first_log_sequence_number;
	if(are_equal_uint256(LSN_assigned_from, INVALID_LOG_SEQUENCE_NUMBER))
		LSN_assigned_from = mr->next_log_sequence_number;

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset; // = log_sequence_number - LSN_assigned_from + wale_p->block_io_functions.block_size;
	{
		uint256 temp;
		if(	!sub_underflow_safe_uint256(&temp, log_sequence_number, LSN_assigned_from) ||
			!add_overflow_safe_uint256(&temp, temp, get_uint256(block_io_functions->block_size), get_0_uint256()) ||
			!cast_to_uint64_from_uint256(&file_offset, temp))
		{
			// this case will not ever happen, but just so to handle it
			(*error) = PARAM_INVALID;
			return 0;
		}
	}

	return file_offset;
}

uint64_t get_file_offset_for_next_log_sequence_number(const master_record* mr, const block_io_ops* block_io_functions, int* error)
{
	// there are no log records on the disk, if the first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER
	// then next_log_sequence_number is at the file_offset
	if(are_equal_uint256(mr->first_log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
		return block_io_functions->block_size;

	// calculate file_offset of next_log_sequence_number
	// = next_log_sequence_number - first_log_sequence_number + block_size
	uint64_t file_offset;
	{
		uint256 temp; // = next_log_sequence_number - first_log_sequence_number + block_size
		if(	(!sub_underflow_safe_uint256(&temp, mr->next_log_sequence_number, mr->first_log_sequence_number)) ||
			(!add_overflow_safe_uint256(&temp, temp, get_uint256(block_io_functions->block_size), get_0_uint256())) ||
			(!cast_to_uint64_from_uint256(&file_offset, temp)) )
		{
			// this implies master record is corrupted
			(*error) = MASTER_RECORD_CORRUPTED;
			return 0;
		}
	}

	return file_offset;
}

int are_equal_master_records(const master_record* mr1, const master_record* mr2)
{
	return (mr1->log_sequence_number_width == mr2->log_sequence_number_width) &&
	are_equal_uint256(mr1->first_log_sequence_number, mr2->first_log_sequence_number) &&
	are_equal_uint256(mr1->last_flushed_log_sequence_number, mr2->last_flushed_log_sequence_number) &&
	are_equal_uint256(mr1->check_point_log_sequence_number, mr2->check_point_log_sequence_number) &&
	are_equal_uint256(mr1->next_log_sequence_number, mr2->next_log_sequence_number);
}
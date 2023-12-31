#include<util_master_record_io.h>

#include<crc32_util.h>

#include<serial_int.h>

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

	if(mr->log_sequence_number_width == 0 || mr->log_sequence_number_width > LARGE_UINT_MAX_BYTES)
	{
		(*error) = LOG_SEQUENCE_NUMBER_UNREPRESENTABLE;
		free(mr_serial);
		return 0;
	}

	mr->first_log_sequence_number = deserialize_large_uint(mr_serial + sizeof(uint32_t), mr->log_sequence_number_width);
	mr->last_flushed_log_sequence_number = deserialize_large_uint(mr_serial + sizeof(uint32_t) + mr->log_sequence_number_width, mr->log_sequence_number_width);
	mr->check_point_log_sequence_number = deserialize_large_uint(mr_serial + sizeof(uint32_t) + 2 * mr->log_sequence_number_width, mr->log_sequence_number_width);
	mr->next_log_sequence_number = deserialize_large_uint(mr_serial + sizeof(uint32_t) + 3 * mr->log_sequence_number_width, mr->log_sequence_number_width);

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

	// serialize
	serialize_uint32(mr_serial, sizeof(uint32_t), mr->log_sequence_number_width);
	serialize_large_uint(mr_serial + sizeof(uint32_t), mr->log_sequence_number_width, mr->first_log_sequence_number);
	serialize_large_uint(mr_serial + sizeof(uint32_t) + mr->log_sequence_number_width, mr->log_sequence_number_width, mr->last_flushed_log_sequence_number);
	serialize_large_uint(mr_serial + sizeof(uint32_t) + 2 * mr->log_sequence_number_width, mr->log_sequence_number_width, mr->check_point_log_sequence_number);
	serialize_large_uint(mr_serial + sizeof(uint32_t) + 3 * mr->log_sequence_number_width, mr->log_sequence_number_width, mr->next_log_sequence_number);

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
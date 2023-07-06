#include<util_master_record_io.h>

#include<stdlib.h>

#include<storage_byte_ordering.h>

int read_master_record(master_record* mr, const block_io_ops* block_io_functions)
{
	void* mr_serial = malloc(block_io_functions->block_size);
	if(mr_serial == NULL)
		return 0;

	int io_success = block_io_functions->read_blocks(block_io_functions->block_io_ops_handle, mr_serial, 0, 1);
	if(!io_success)
	{
		free(mr_serial);
		return 0;
	}

	// deserialize
	mr->first_log_sequence_number = deserialize_le_uint64(mr_serial);
	mr->last_flushed_log_sequence_number = deserialize_le_uint64(mr_serial + sizeof(uint64_t));
	mr->check_point_log_sequence_number = deserialize_le_uint64(mr_serial + 2 * sizeof(uint64_t));
	mr->next_log_sequence_number = deserialize_le_uint64(mr_serial + 3 * sizeof(uint64_t));

	free(mr_serial);
	return 1;
}

int write_and_flush_master_record(const master_record* mr, const block_io_ops* block_io_functions)
{
	void* mr_serial = malloc(block_io_functions->block_size);
	if(mr_serial == NULL)
		return 0;

	// serialize
	serialize_le_uint64(mr_serial, mr->first_log_sequence_number);
	serialize_le_uint64(mr_serial + sizeof(uint64_t), mr->last_flushed_log_sequence_number);
	serialize_le_uint64(mr_serial + 2 * sizeof(uint64_t), mr->check_point_log_sequence_number);
	serialize_le_uint64(mr_serial + 3 * sizeof(uint64_t), mr->next_log_sequence_number);

	int io_success = block_io_functions->write_blocks(block_io_functions->block_io_ops_handle, mr_serial, 0, 1)
						&& block_io_functions->flush_all_writes(block_io_functions->block_io_ops_handle);
	if(!io_success)
	{
		free(mr_serial);
		return 0;
	}

	free(mr_serial);
	return 1;
}
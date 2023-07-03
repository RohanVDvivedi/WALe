#include<wale.h>

#include<master_record_io.h>

#include<stdlib.h>

#include<cutlery_stds.h>

#define OS_PAGE_SIZE 4096

int initialize_wale(wale* wale_p, uint64_t next_log_sequence_number, pthread_mutex_t* external_lock, block_io_ops block_io_functions, uint64_t append_only_block_count)
{
	wale_p->has_internal_lock = (external_lock == NULL);

	if(wale_p->has_internal_lock)
		pthread_mutex_init(&(wale_p->internal_lock), NULL);
	else
		wale_p->external_lock = external_lock;

	wale_p->block_io_functions = block_io_functions;

	if(next_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
	{
		if(!read_master_record(&(wale_p->on_disk_master_record), &(wale_p->block_io_functions)))
			return 0;
	}
	else
	{
		wale_p->on_disk_master_record.first_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.check_point_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.next_log_sequence_number = next_log_sequence_number;

		if(!write_and_flush_master_record(&(wale_p->on_disk_master_record), &(wale_p->block_io_functions)))
			return 0;
	}

	wale_p->in_memory_master_record = wale_p->on_disk_master_record;

	wale_p->buffer = aligned_alloc(OS_PAGE_SIZE, (append_only_block_count * wale_p->block_io_functions.block_size));
	wale_p->buffer_block_count = append_only_block_count;

	if(wale_p->buffer == NULL)
		return 0;

	// there are no log records on the disk
	if(wale_p->in_memory_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
	{
		wale_p->append_offset = 0;
		wale_p->buffer_start_block_id = 1;
	}
	else // else read appropriate first block from memory
	{
		uint64_t file_offset_to_append_from = wale_p->in_memory_master_record.next_log_sequence_number - wale_p->in_memory_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		wale_p->append_offset = file_offset_to_append_from % wale_p->block_io_functions.block_size;
		wale_p->buffer_start_block_id = UINT_ALIGN_DOWN(file_offset_to_append_from, wale_p->block_io_functions.block_size) / wale_p->block_io_functions.block_size;

		if(wale_p->append_offset)
		{
			if(!wale_p->block_io_functions.read_blocks(wale_p->block_io_functions.block_io_ops_handle, wale_p->buffer, wale_p->buffer_start_block_id, 1))
			{
				free(wale_p->buffer);
				return 0;
			}
		}
	}

	return 1;
}

void deinitialize_wale(wale* wale_p)
{

}
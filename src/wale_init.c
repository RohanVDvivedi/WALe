#include<wale.h>

#include<master_record_io.h>

#include<stdlib.h>

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
		wale_p->on_disk_master_record.first_log_sequence_number = 0;
		wale_p->on_disk_master_record.last_flushed_log_sequence_number = 0;
		wale_p->on_disk_master_record.check_point_log_sequence_number = 0;
		wale_p->on_disk_master_record.next_log_sequence_number = next_log_sequence_number;

		if(!write_and_flush_master_record(&(wale_p->on_disk_master_record), &(wale_p->block_io_functions)))
			return 0;
	}

	wale_p->in_memory_master_record = wale_p->on_disk_master_record;

	wale_p->buffer = aligned_alloc(OS_PAGE_SIZE, (append_only_block_count * wale_p->block_io_functions.block_size));
	wale_p->buffer_block_count = append_only_block_count;

	if(wale_p->buffer == NULL)
		return 0;

	// read appropriate first block from memory
	wale_p->append_offset = 0;
	wale_p->buffer_start_block_id = 1;

	wale_p->scrolling_up_append_only_buffer_in_progress = 0;
	wale_p->flush_in_progress = 0;

	wale_p->count_of_ongoing_random_reads = 0;
	wale_p->count_of_ongoing_append_only_writes = 0;

	pthread_cond_init(&(wale_p->wait_for_flush_to_finish), NULL);
	wale_p->count_of_threads_waiting_for_flush_to_finish = 0;

	pthread_cond_init(&(wale_p->wait_for_ongoing_accesses_to_finish), NULL);
	wale_p->count_of_threads_waiting_for_accesses_to_finish = 0;

	return 1;
}

void deinitialize_wale(wale* wale_p)
{

}
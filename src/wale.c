#include<wale.h>

#include<wale_get_lock_util.h>
#include<storage_byte_ordering.h>
#include<util_random_read.h>
#include<util_scroll_append_only_buffer.h>
#include<util_master_record_io.h>

#include<rwlock.h>

#include<cutlery_stds.h>
#include<cutlery_math.h>

#include<stdlib.h>

static void prefix_to_acquire_flushed_log_records_reader_lock(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	read_lock(&(wale_p->flushed_log_records_lock), READ_PREFERRING, BLOCKING);

	pthread_mutex_unlock(get_wale_lock(wale_p));
}

static void suffix_to_release_flushed_log_records_reader_lock(wale* wale_p)
{
	pthread_mutex_lock(get_wale_lock(wale_p));

	read_unlock(&(wale_p->flushed_log_records_lock));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));
}

/*
	Every reader function must read only the flushed contents of the WALe file,
	i.e. after calling prefix_to_acquire_flushed_log_records_reader_lock() they can access only the on_disk_master_record and the file contents for the log_sequence_numbers between (and inclusive of) first_log_sequence_number and last_flushed_log_sequence_number
	and then call suffix_to_release_flushed_log_records_reader_lock() before quiting

	on_disk_master_record is just the cached structured copy of the master record on disk
*/

uint64_t get_first_log_sequence_number(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint64_t first_log_sequence_number = wale_p->on_disk_master_record.first_log_sequence_number;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return first_log_sequence_number;
}

uint64_t get_last_flushed_log_sequence_number(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint64_t last_flushed_log_sequence_number = wale_p->on_disk_master_record.last_flushed_log_sequence_number;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return last_flushed_log_sequence_number;
}

uint64_t get_check_point_log_sequence_number(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint64_t check_point_log_sequence_number = wale_p->on_disk_master_record.check_point_log_sequence_number;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return check_point_log_sequence_number;
}

uint64_t get_next_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint64_t next_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// if the wale has any records, and its first <= log_sequence_number < last, then read the size and add it to the log_sequence_number to get its next
	if(wale_p->on_disk_master_record.first_log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER &&
		wale_p->on_disk_master_record.first_log_sequence_number <= log_sequence_number && 
		log_sequence_number < wale_p->on_disk_master_record.last_flushed_log_sequence_number
		)
	{
		// calculate the offset in file of the log_record at log_sequence_number
		uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		// read its size
		char size_of_log_record_bytes[4];
		if(!random_read_at(size_of_log_record_bytes, 4, file_offset_of_log_record, &(wale_p->block_io_functions)))
			goto FAILED;

		uint32_t size_of_log_record = deserialize_le_uint32(size_of_log_record_bytes);

		// the next_log_sequence_number is right after this log_record
		next_log_sequence_number = log_sequence_number + ((uint64_t)size_of_log_record) + UINT64_C(8); // 8 for prefix and suffix size

		FAILED:;
	}

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return next_log_sequence_number;
}

uint64_t get_prev_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint64_t prev_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// if the wale has any records, and its first < log_sequence_number <= last, then read the size of the previous log record and substract it from the log_sequence_number to get its prev
	if(wale_p->on_disk_master_record.first_log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER &&
		wale_p->on_disk_master_record.first_log_sequence_number < log_sequence_number && 
		log_sequence_number <= wale_p->on_disk_master_record.last_flushed_log_sequence_number
		)
	{
		// calculate the offset in file of the log_record at log_sequence_number
		uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		// read size of its previous log_record
		char size_of_prev_log_record_bytes[4];
		if(!random_read_at(size_of_prev_log_record_bytes, 4, file_offset_of_log_record - 4, &(wale_p->block_io_functions)))
			goto FAILED;

		uint32_t size_of_prev_log_record = deserialize_le_uint32(size_of_prev_log_record_bytes);

		// the prev_log_sequence_number is right before this one
		prev_log_sequence_number = log_sequence_number - (((uint64_t)size_of_prev_log_record) + UINT64_C(8)); // 8 for prefix and suffix size of the previous log record

		FAILED:;
	}

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return prev_log_sequence_number;
}

void* get_log_record_at(wale* wale_p, uint64_t log_sequence_number, uint32_t* log_record_size)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to NULL, which is default result
	void* log_record = NULL;

	// if the wale has any records, and its first <= log_sequence_number <= last
	if(wale_p->on_disk_master_record.first_log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER &&
		wale_p->on_disk_master_record.first_log_sequence_number <= log_sequence_number && 
		log_sequence_number <= wale_p->on_disk_master_record.last_flushed_log_sequence_number
		)
	{
		// calculate the offset in file of the log_record at log_sequence_number
		uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		// read size of the log_record at given log_sequence_number
		char size_of_log_record_bytes[4];
		if(!random_read_at(size_of_log_record_bytes, 4, file_offset_of_log_record, &(wale_p->block_io_functions)))
			goto FAILED;

		(*log_record_size) = deserialize_le_uint32(size_of_log_record_bytes);

		log_record = malloc((*log_record_size));
		if(log_record == NULL)
			goto FAILED;

		// read data for log_record from the file, data size amounting to log_record_size
		if(!random_read_at(log_record, (*log_record_size), file_offset_of_log_record + 4, &(wale_p->block_io_functions)))
		{
			free(log_record);
			log_record = NULL;
			goto FAILED;
		}

		FAILED:;
	}

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return log_record;
}

static uint64_t get_file_offset_for_next_log_sequence_number_to_append(wale* wale_p)
{
	if(wale_p->in_memory_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
		return wale_p->block_io_functions.block_size;
	else
		return wale_p->in_memory_master_record.next_log_sequence_number - wale_p->in_memory_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;
}

static int is_file_offset_within_append_only_buffer(wale* wale_p, uint64_t file_offset)
{
	if(wale_p->buffer_start_block_id * wale_p->block_io_functions.block_size <= file_offset && file_offset < (wale_p->buffer_start_block_id + wale_p->buffer_block_count) * wale_p->block_io_functions.block_size)
		return 1;
	return 0;
}

static uint64_t get_log_sequence_number_for_next_log_record_and_advance_master_record(wale* wale_p, uint32_t log_record_size, int is_check_point)
{
	// compute the total slot size required by this new log record
	uint64_t total_log_record_slot_size = ((uint64_t)log_record_size) + 8; // 4 bytes for prefix size and 4 bytes for suffix size

	// its log sequence number will simply be the next log sequence number
	uint64_t log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number;

	// check for overflow of the next_log_sequence_number, upon alloting this slot
	uint64_t new_next_log_sequence_number = log_sequence_number + total_log_record_slot_size;
	if(new_next_log_sequence_number < log_sequence_number || new_next_log_sequence_number < total_log_record_slot_size)
		return INVALID_LOG_SEQUENCE_NUMBER;

	// if earlier there were no log records on the disk, then this will be the new first_log_sequence_number
	if(wale_p->in_memory_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
		wale_p->in_memory_master_record.first_log_sequence_number = log_sequence_number;

	// this will also be the new last_flushed_log_sequence_number
	wale_p->in_memory_master_record.last_flushed_log_sequence_number = log_sequence_number;

	// update check point if this is going to be a check_point log record
	if(is_check_point)
		wale_p->in_memory_master_record.check_point_log_sequence_number = log_sequence_number;

	// set the next_log_sequence_number to the new next_log_sequence_number
	wale_p->in_memory_master_record.next_log_sequence_number = new_next_log_sequence_number;

	return log_sequence_number;
}

// returns the number of bytes written,
// it will return lesser than data_size, only if a scroll fails in which case you must exit the application, since we can't receover from this
// append_slot is advanced by this function, suggesting a write
static uint64_t append_log_record_data(wale* wale_p, uint64_t* append_slot, const char* data, uint64_t data_size, uint64_t* total_bytes_to_write_for_this_log_record, int* error_in_scroll)
{
	uint64_t bytes_written = 0;
	while(bytes_written < data_size)
	{
		uint64_t bytes_to_write = min(wale_p->buffer_block_count * wale_p->block_io_functions.block_size - (*append_slot), data_size - bytes_written);
		memory_move(wale_p->buffer + (*append_slot), data + bytes_written, bytes_to_write);
		bytes_written += bytes_to_write;
		(*append_slot) += bytes_to_write;
		(*total_bytes_to_write_for_this_log_record) -= bytes_to_write;

		// if append slot is at the end of the wale's append only buffer, then attempt to scroll
		if((*append_slot) == wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
		{
			// scrolling needs global lock
			pthread_mutex_lock(get_wale_lock(wale_p));

			wale_p->scrolling_in_progress = 1;

			while(wale_p->append_only_writers_count > 1)
				pthread_cond_wait(&(wale_p->waiting_for_append_only_writers_to_exit), get_wale_lock(wale_p));

			(*error_in_scroll) = !scroll_append_only_buffer(wale_p);

			wale_p->scrolling_in_progress = 0;

			if(!(*error_in_scroll))
			{
				// take the probably the first slot and advance the append_offset to how much we can write at most
				(*append_slot) = wale_p->append_offset;
				wale_p->append_offset = min(wale_p->append_offset + (*total_bytes_to_write_for_this_log_record), wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

				// if there is space in the append_only_buffer then we wake other writers up
				if(wale_p->append_offset < wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
					pthread_cond_broadcast(&(wale_p->append_only_writers_waiting));
			}

			pthread_mutex_unlock(get_wale_lock(wale_p));

			// if scroll was a failure we break out of the loop
			if((*error_in_scroll))
				break;
		}
	}

	return bytes_written;
}

uint64_t append_log_record(wale* wale_p, const void* log_record, uint32_t log_record_size, int is_check_point)
{
	// return value defaults to an INVALID_LOG_SEQUENCE_NUMBER
	uint64_t log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	// share lock the append_only_buffer, snce we are reading the buffer_start_block_id
	shared_lock(&(wale_p->append_only_buffer_lock), WRITE_PREFERRING, BLOCKING);

	// we wait, while the offset for the next_log_sequence_number is not within the append only buffer
	while(!is_file_offset_within_append_only_buffer(wale_p, get_file_offset_for_next_log_sequence_number_to_append(wale_p))
		&& !wale_p->major_scroll_error)
	{
		shared_unlock(&(wale_p->append_only_buffer_lock));
		pthread_cond_wait(&(wale_p->wait_for_scroll), get_wale_lock(wale_p));
		shared_lock(&(wale_p->append_only_buffer_lock), WRITE_PREFERRING, BLOCKING);
	}

	if(wale_p->major_scroll_error)
		goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;

	// take slot if the next log sequence number is in the append only buffer
	log_sequence_number = get_log_sequence_number_for_next_log_record_and_advance_master_record(wale_p, log_record_size, is_check_point);

	// exit suggesting failure to allocate a log_sequence_number
	if(log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
		goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;

	// compute the total bytes we will write
	uint64_t total_bytes_to_write = ((uint64_t)log_record_size) + UINT64_C(8);

	// now take the slot in the append only buffer
	uint64_t append_slot = wale_p->append_offset;

	// advance the append_offset of the append only buffer
	wale_p->append_offset = min(wale_p->append_offset + total_bytes_to_write, wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

	// we have the slot in the append only buffer, and a log_sequence_number, now we don't need the global lock
	pthread_mutex_unlock(get_wale_lock(wale_p));

	// serialize log_record_size as a byte array ordered in little endian format
	char size_in_bytes[4];
	serialize_le_uint32(size_in_bytes, log_record_size);

	int scroll_error = 0;

	// write prefix
	append_log_record_data(wale_p, &append_slot, size_in_bytes, 4, &total_bytes_to_write, &scroll_error);
	if(scroll_error)
		goto SCROLL_FAIL;

	// write log record itself
	append_log_record_data(wale_p, &append_slot, log_record, log_record_size, &total_bytes_to_write, &scroll_error);
	if(scroll_error)
		goto SCROLL_FAIL;

	// write suffix
	append_log_record_data(wale_p, &append_slot, size_in_bytes, 4, &total_bytes_to_write, &scroll_error);
	if(scroll_error)
		goto SCROLL_FAIL;

	SCROLL_FAIL:;
	pthread_mutex_lock(get_wale_lock(wale_p));

	// this condition implies a fail to scroll the append only buffer
	if(scroll_error)
		log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT:;
	// share lock the append_only_buffer
	shared_unlock(&(wale_p->append_only_buffer_lock));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return log_sequence_number;
}

uint64_t flush_all_log_records(wale* wale_p)
{
	// return value defaults to INVALID_LOG_SEQUENCE_NUMBER
	uint64_t last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	// we can not flush if there has been a major scroll error
	if(wale_p->major_scroll_error)
		goto EXIT;

	// wait for any other ongoing flush to exit
	while(wale_p->flush_in_progress)
	{
		wale_p->flush_completion_waiting_count++;
		pthread_cond_wait(&(wale_p->flush_completion_waiting), get_wale_lock(wale_p));
		wale_p->flush_completion_waiting_count--;
	}

	// set to indicate that a flush is now in progress
	wale_p->flush_in_progress = 1;

	// wait for append only writers to exit, so that we can scroll
	wale_p->waiting_for_append_only_writers_to_exit_flag = 1;
	while(wale_p->append_only_writers_count > 0)
		pthread_cond_wait(&(wale_p->waiting_for_append_only_writers_to_exit), get_wale_lock(wale_p));
	wale_p->waiting_for_append_only_writers_to_exit_flag = 0;

	// perform a scroll
	wale_p->scrolling_in_progress = 1;
	int scroll_success = (wale_p->major_scroll_error == 0) && scroll_append_only_buffer(wale_p);
	wale_p->scrolling_in_progress = 0;

	// we do not need the writers to wait after all the records up til now have been written
	if(wale_p->append_only_writers_waiting_count > 0)
		pthread_cond_broadcast(&(wale_p->append_only_writers_waiting));

	if(!scroll_success)
	{
		wale_p->major_scroll_error = 1;
		goto FAILED_EXIT;
	}

	// copy the valid values for flushing the on disk master record, before we release the lock
	master_record new_on_disk_master_record = wale_p->in_memory_master_record;

	// we can allow the new append only writers from now on

	// release the lock
	pthread_mutex_unlock(get_wale_lock(wale_p));

	int flush_success = wale_p->block_io_functions.flush_all_writes(wale_p->block_io_functions.block_io_ops_handle) 
	&& write_and_flush_master_record(&new_on_disk_master_record, &(wale_p->block_io_functions));

	if(!flush_success)
	{
		pthread_mutex_lock(get_wale_lock(wale_p));
		goto FAILED_EXIT;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	// now we need to install the on_disk_master_record, so we need all readers to exit
	wale_p->waiting_for_random_readers_to_exit_flag = 1;
	while(wale_p->random_readers_count > 0)
		pthread_cond_wait(&(wale_p->waiting_for_append_only_writers_to_exit), get_wale_lock(wale_p));

	// once all readers have exited, we can install the new_on_disk_master_record to wale_p->on_disk_master_record ..
	// without the global lock held
	pthread_mutex_unlock(get_wale_lock(wale_p));

	// 
	wale_p->on_disk_master_record = new_on_disk_master_record;
	last_flushed_log_sequence_number = wale_p->on_disk_master_record.last_flushed_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	// wake up any readers that are waiting
	wale_p->waiting_for_random_readers_to_exit_flag = 0;
	if(wale_p->random_readers_waiting_count > 0)
		pthread_cond_broadcast(&(wale_p->random_readers_waiting));

	// 
	FAILED_EXIT:;

	// flush is now finish
	wale_p->flush_in_progress = 0;

	// wake up any thread waiting for flush to complete
	if(wale_p->flush_completion_waiting_count)
		pthread_cond_broadcast(&(wale_p->flush_completion_waiting));

	EXIT:;

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return last_flushed_log_sequence_number;
}

int truncate_log_records(wale* wale_p)
{
	int truncated_logs = 0;

	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	// we can not flush if there has been a major scroll error
	if(wale_p->major_scroll_error)
		goto EXIT;

	// wait for any other ongoing flush to exit
	while(wale_p->flush_in_progress)
	{
		wale_p->flush_completion_waiting_count++;
		pthread_cond_wait(&(wale_p->flush_completion_waiting), get_wale_lock(wale_p));
		wale_p->flush_completion_waiting_count--;
	}

	// set to indicate that a flush is now in progress
	wale_p->flush_in_progress = 1;

	// wait for append only writers to exit, so that we can safely reset the append only log and the in_memeory_master_record
	// this ensures that the next log sequence number is not advanced
	wale_p->waiting_for_append_only_writers_to_exit_flag = 1;
	while(wale_p->append_only_writers_count > 0)
		pthread_cond_wait(&(wale_p->waiting_for_append_only_writers_to_exit), get_wale_lock(wale_p));

	// if any of the prior writes caused major scroll error, then we fail exit
	if(wale_p->major_scroll_error)
		goto FAILED_EXIT;

	// the next_log_sequence_number is not advanced
	master_record new_master_record = {
		.first_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.check_point_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.next_log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number,
	};
	uint64_t new_append_offset = 0;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	truncated_logs = write_and_flush_master_record(&new_master_record, &(wale_p->block_io_functions));

	pthread_mutex_lock(get_wale_lock(wale_p));

	if(!truncated_logs)
		goto FAILED_EXIT;

	// now we need to install the on_disk_master_record, so we need all readers to exit, aswell
	wale_p->waiting_for_random_readers_to_exit_flag = 1;
	while(wale_p->random_readers_count > 0)
		pthread_cond_wait(&(wale_p->waiting_for_append_only_writers_to_exit), get_wale_lock(wale_p));

	// The below 3 lines can be done with global lock releases, but there is not point in releasing it
	// since neither readers, nor writers not even the any flush can be using wale at this state

	// update all of the current state
	wale_p->on_disk_master_record = new_master_record;
	wale_p->in_memory_master_record = new_master_record;
	wale_p->append_offset = new_append_offset;

	// wake up any readers that are waiting
	wale_p->waiting_for_random_readers_to_exit_flag = 0;
	if(wale_p->random_readers_waiting_count > 0)
		pthread_cond_broadcast(&(wale_p->random_readers_waiting));

	// 
	FAILED_EXIT:;

	// wake up any writers that are waiting
	wale_p->waiting_for_append_only_writers_to_exit_flag = 0;
	if(wale_p->append_only_writers_waiting_count > 0)
		pthread_cond_broadcast(&(wale_p->append_only_writers_waiting));

	// flush is now finish
	wale_p->flush_in_progress = 0;

	// wake up any thread waiting for flush to complete
	if(wale_p->flush_completion_waiting_count)
		pthread_cond_broadcast(&(wale_p->flush_completion_waiting));

	EXIT:;

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return truncated_logs;
}
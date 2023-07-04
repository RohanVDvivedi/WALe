#include<wale.h>

#include<wale_get_lock_util.h>
#include<storage_byte_ordering.h>
#include<random_read_util.h>
#include<append_only_write_util.h>
#include<master_record_io.h>

#include<cutlery_stds.h>
#include<cutlery_math.h>

#include<stdlib.h>

uint64_t get_first_log_sequence_number(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	while(wale_p->flush_waiting_for_random_readers_to_exit)
	{
		wale_p->random_readers_waiting_count++;
		pthread_cond_wait(&(wale_p->random_readers_waiting), get_wale_lock(wale_p));
		wale_p->random_readers_waiting_count--;
	}

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	uint64_t first_log_sequence_number = wale_p->on_disk_master_record.first_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count--;

	if(wale_p->random_readers_count == 0 && wale_p->flush_waiting_for_random_readers_to_exit)
		pthread_cond_broadcast(&(wale_p->waiting_for_random_readers_to_exit));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return first_log_sequence_number;
}

uint64_t get_last_flushed_log_sequence_number(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	while(wale_p->flush_waiting_for_random_readers_to_exit)
	{
		wale_p->random_readers_waiting_count++;
		pthread_cond_wait(&(wale_p->random_readers_waiting), get_wale_lock(wale_p));
		wale_p->random_readers_waiting_count--;
	}

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	uint64_t last_flushed_log_sequence_number = wale_p->on_disk_master_record.last_flushed_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count--;

	if(wale_p->random_readers_count == 0 && wale_p->flush_waiting_for_random_readers_to_exit)
		pthread_cond_broadcast(&(wale_p->waiting_for_random_readers_to_exit));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return last_flushed_log_sequence_number;
}

uint64_t get_check_point_log_sequence_number(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	while(wale_p->flush_waiting_for_random_readers_to_exit)
	{
		wale_p->random_readers_waiting_count++;
		pthread_cond_wait(&(wale_p->random_readers_waiting), get_wale_lock(wale_p));
		wale_p->random_readers_waiting_count--;
	}

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	uint64_t check_point_log_sequence_number = wale_p->on_disk_master_record.check_point_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count--;

	if(wale_p->random_readers_count == 0 && wale_p->flush_waiting_for_random_readers_to_exit)
		pthread_cond_broadcast(&(wale_p->waiting_for_random_readers_to_exit));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return check_point_log_sequence_number;
}

uint64_t get_next_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	while(wale_p->flush_waiting_for_random_readers_to_exit)
	{
		wale_p->random_readers_waiting_count++;
		pthread_cond_wait(&(wale_p->random_readers_waiting), get_wale_lock(wale_p));
		wale_p->random_readers_waiting_count--;
	}

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint64_t next_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// if the wale has any records, and its first <= log_sequence_number < last, then read the size and add it to the log_sequence_number to get its next
	if(wale_p->on_disk_master_record.first_log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER &&
		wale_p->on_disk_master_record.first_log_sequence_number <= log_sequence_number && 
		log_sequence_number < wale_p->on_disk_master_record.last_flushed_log_sequence_number
		)
	{
		uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		char size_of_log_record_bytes[4];
		if(!random_read_at(size_of_log_record_bytes, 4, file_offset_of_log_record, &(wale_p->block_io_functions)))
			goto FAILED;

		uint32_t size_of_log_record = deserialize_le_uint32(size_of_log_record_bytes);

		next_log_sequence_number = log_sequence_number + size_of_log_record + 8; // 8 for prefix and suffix size

		FAILED:;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count--;

	if(wale_p->random_readers_count == 0 && wale_p->flush_waiting_for_random_readers_to_exit)
		pthread_cond_broadcast(&(wale_p->waiting_for_random_readers_to_exit));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return next_log_sequence_number;
}

uint64_t get_prev_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	while(wale_p->flush_waiting_for_random_readers_to_exit)
	{
		wale_p->random_readers_waiting_count++;
		pthread_cond_wait(&(wale_p->random_readers_waiting), get_wale_lock(wale_p));
		wale_p->random_readers_waiting_count--;
	}

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint64_t prev_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// if the wale has any records, and its first < log_sequence_number <= last, then read the size of the previous log record and substract it from the log_sequence_number to get its prev
	if(wale_p->on_disk_master_record.first_log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER &&
		wale_p->on_disk_master_record.first_log_sequence_number < log_sequence_number && 
		log_sequence_number <= wale_p->on_disk_master_record.last_flushed_log_sequence_number
		)
	{
		uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		char size_of_prev_log_record_bytes[4];
		if(!random_read_at(size_of_prev_log_record_bytes, 4, file_offset_of_log_record - 4, &(wale_p->block_io_functions)))
			goto FAILED;

		uint32_t size_of_prev_log_record = deserialize_le_uint32(size_of_prev_log_record_bytes);

		prev_log_sequence_number = log_sequence_number - size_of_prev_log_record - 8; // 8 for prefix and suffix size of the previous log record

		FAILED:;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count--;

	if(wale_p->random_readers_count == 0 && wale_p->flush_waiting_for_random_readers_to_exit)
		pthread_cond_broadcast(&(wale_p->waiting_for_random_readers_to_exit));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return prev_log_sequence_number;
}

void* get_log_record_at(wale* wale_p, uint64_t log_sequence_number, uint32_t* log_record_size)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	while(wale_p->flush_waiting_for_random_readers_to_exit)
	{
		wale_p->random_readers_waiting_count++;
		pthread_cond_wait(&(wale_p->random_readers_waiting), get_wale_lock(wale_p));
		wale_p->random_readers_waiting_count--;
	}

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	// set it to NULL, which is default result
	void* log_record = NULL;

	// if the wale has any records, and its first <= log_sequence_number <= last
	if(wale_p->on_disk_master_record.first_log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER &&
		wale_p->on_disk_master_record.first_log_sequence_number <= log_sequence_number && 
		log_sequence_number <= wale_p->on_disk_master_record.last_flushed_log_sequence_number
		)
	{
		uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		char size_of_log_record_bytes[4];
		if(!random_read_at(size_of_log_record_bytes, 4, file_offset_of_log_record, &(wale_p->block_io_functions)))
			goto FAILED;

		(*log_record_size) = deserialize_le_uint32(size_of_log_record_bytes);

		void* log_record = malloc((*log_record_size));
		if(log_record == NULL)
			goto FAILED;

		if(!random_read_at(log_record, (*log_record_size), file_offset_of_log_record + 4, &(wale_p->block_io_functions)))
		{
			free(log_record);
			log_record = NULL;
			goto FAILED;
		}

		FAILED:;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count--;

	if(wale_p->random_readers_count == 0 && wale_p->flush_waiting_for_random_readers_to_exit)
		pthread_cond_broadcast(&(wale_p->waiting_for_random_readers_to_exit));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return log_record;
}

uint64_t append_log_record(wale* wale_p, const void* log_record, uint32_t log_record_size, int is_check_point)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	uint64_t total_bytes_to_write = ((uint64_t)log_record_size) + 8;

	// take slot if the next log sequence number is in the append only buffer
	while(1)
	{
		uint64_t file_offset_for_next_log_sequence_number;
		if(wale_p->in_memory_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
			file_offset_for_next_log_sequence_number = wale_p->block_io_functions.block_size;
		else
			file_offset_for_next_log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number - wale_p->in_memory_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

		if(wale_p->flush_waiting_for_append_only_writers_to_exit || file_offset_for_next_log_sequence_number - (wale_p->buffer_start_block_id * wale_p->block_io_functions.block_size) >= wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
		{
			wale_p->append_only_writers_waiting_count++;
			pthread_cond_wait(&(wale_p->append_only_writers_waiting), get_wale_lock(wale_p));
			wale_p->append_only_writers_waiting_count--;
		}
	}

	uint64_t log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number;

	if(wale_p->in_memory_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
		wale_p->in_memory_master_record.first_log_sequence_number = log_sequence_number;

	wale_p->in_memory_master_record.last_flushed_log_sequence_number = log_sequence_number;

	if(is_check_point)
		wale_p->in_memory_master_record.check_point_log_sequence_number = log_sequence_number;

	uint64_t append_slot = wale_p->append_offset;

	wale_p->in_memory_master_record.next_log_sequence_number = log_sequence_number + total_bytes_to_write;

	wale_p->append_offset = min(wale_p->append_offset + total_bytes_to_write, wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

	wale_p->append_only_writers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	char size_in_bytes[4];
	serialize_le_uint32(size_in_bytes, log_record_size);

	// write prefix
	const char* data = size_in_bytes;
	uint32_t data_size = 4;
	uint32_t bytes_written = 0;
	while(bytes_written < data_size)
	{
		uint32_t bytes_to_write = min(wale_p->buffer_block_count * wale_p->block_io_functions.block_size - append_slot, data_size - bytes_written);
		memory_move(wale_p->buffer + append_slot, data + bytes_written, bytes_to_write);
		bytes_written += bytes_to_write;
		append_slot += bytes_to_write;
		total_bytes_to_write -= bytes_to_write;

		if(append_slot == wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
		{
			pthread_mutex_lock(get_wale_lock(wale_p));

			scroll_append_only_buffer(wale_p);
			append_slot = wale_p->append_offset;
			wale_p->append_offset = min(wale_p->append_offset + total_bytes_to_write, wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

			if(wale_p->append_offset < wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
				pthread_cond_broadcast(&(wale_p->append_only_writers_waiting));

			pthread_mutex_unlock(get_wale_lock(wale_p));
		}
	}

	// write log record itself
	data = log_record;
	data_size = log_record_size;
	bytes_written = 0;
	while(bytes_written < data_size)
	{
		uint32_t bytes_to_write = min(wale_p->buffer_block_count * wale_p->block_io_functions.block_size - append_slot, data_size - bytes_written);
		memory_move(wale_p->buffer + append_slot, data + bytes_written, bytes_to_write);
		bytes_written += bytes_to_write;
		append_slot += bytes_to_write;
		total_bytes_to_write -= bytes_to_write;

		if(append_slot == wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
		{
			pthread_mutex_lock(get_wale_lock(wale_p));

			scroll_append_only_buffer(wale_p);
			append_slot = wale_p->append_offset;
			wale_p->append_offset = min(wale_p->append_offset + total_bytes_to_write, wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

			if(wale_p->append_offset < wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
				pthread_cond_broadcast(&(wale_p->append_only_writers_waiting));

			pthread_mutex_unlock(get_wale_lock(wale_p));
		}
	}

	// write suffix
	data = size_in_bytes;
	data_size = 4;
	bytes_written = 0;
	while(bytes_written < data_size)
	{
		uint32_t bytes_to_write = min(wale_p->buffer_block_count * wale_p->block_io_functions.block_size - append_slot, data_size - bytes_written);
		memory_move(wale_p->buffer + append_slot, data + bytes_written, bytes_to_write);
		bytes_written += bytes_to_write;
		append_slot += bytes_to_write;
		total_bytes_to_write -= bytes_to_write;

		if(append_slot == wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
		{
			pthread_mutex_lock(get_wale_lock(wale_p));

			scroll_append_only_buffer(wale_p);
			append_slot = wale_p->append_offset;
			wale_p->append_offset = min(wale_p->append_offset + total_bytes_to_write, wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

			if(wale_p->append_offset < wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
				pthread_cond_broadcast(&(wale_p->append_only_writers_waiting));

			pthread_mutex_unlock(get_wale_lock(wale_p));
		}
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->append_only_writers_count--;

	if(wale_p->append_only_writers_count == 0 && wale_p->flush_waiting_for_append_only_writers_to_exit)
		pthread_cond_broadcast(&(wale_p->waiting_for_append_only_writers_to_exit));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return log_sequence_number;
}

uint64_t flush_all_log_records(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	while(wale_p->flush_in_progress)
	{
		wale_p->flush_completion_waiting_count++;
		pthread_cond_wait(&(wale_p->flush_completion_waiting), get_wale_lock(wale_p));
		wale_p->flush_completion_waiting_count--;
	}

	wale_p->flush_in_progress = 1;

	scroll_append_only_buffer(wale_p);

	pthread_mutex_unlock(get_wale_lock(wale_p));

	wale_p->block_io_functions.flush_all_writes(wale_p->block_io_functions.block_io_ops_handle);

	write_and_flush_master_record(&(wale_p->in_memory_master_record), &(wale_p->block_io_functions));

	// do the below while there are no random readers and no append only writers in the system
	wale_p->on_disk_master_record = wale_p->in_memory_master_record;

	uint64_t last_flushed_log_sequence_number = wale_p->on_disk_master_record.last_flushed_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->flush_in_progress = 0;

	if(wale_p->flush_completion_waiting_count)
		pthread_cond_broadcast(&(wale_p->flush_completion_waiting));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return last_flushed_log_sequence_number;
}

int truncate_log_records(wale* wale_p);
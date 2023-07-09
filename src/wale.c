#include<wale.h>

#include<wale_get_lock_util.h>
#include<crc32_util.h>
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

typedef struct log_record_header log_record_header;
struct log_record_header
{
	uint32_t prev_log_record_size;
	uint32_t curr_log_record_size;
};

#define HEADER_SIZE UINT64_C(8)

// 1 is success, 0 is failure
static int parse_and_check_crc32_for_log_record_header_at(log_record_header* result, uint64_t file_offset, const block_io_ops* block_io_functions, int* error)
{
	char serial_header[HEADER_SIZE + 4];

	// attempt read for the header at the file_offset
	if(!random_read_at(serial_header, HEADER_SIZE + 4, file_offset, block_io_functions))
	{
		(*error) = READ_IO_ERROR;
		return 0;
	}

	// calculate crc32 of the first 8 bytes
	uint32_t calcuated_crc32 = crc32_init();
	calcuated_crc32 = crc32_util(calcuated_crc32, serial_header, HEADER_SIZE);

	// deserialize all the fields
	result->prev_log_record_size = deserialize_le_uint32(serial_header + 0);
	result->curr_log_record_size = deserialize_le_uint32(serial_header + 4);
	uint32_t parsed_crc32 = deserialize_le_uint32(serial_header + 8);

	// compare the parsed crc32 with the calculated one
	if(parsed_crc32 != calcuated_crc32)
	{
		(*error) = HEADER_CORRUPTED;
		return 0;
	}

	(*error) = NO_ERROR;
	return 1;
}

uint64_t get_next_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number, int* error)
{
	// initialize error to no error
	(*error) = NO_ERROR;

	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint64_t next_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// if the wale has no records, OR the log_sequence_number is not within first and last_flushed log_sequence_number then fail
	if(wale_p->on_disk_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER ||
		log_sequence_number < wale_p->on_disk_master_record.first_log_sequence_number || 
		wale_p->on_disk_master_record.last_flushed_log_sequence_number < log_sequence_number
		)
	{
		(*error) = PARAM_INVALID;
		goto EXIT;
	}

	// next of last_flushed_log_sequence_number does not exists
	if(log_sequence_number == wale_p->on_disk_master_record.last_flushed_log_sequence_number)
		goto EXIT;

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

	log_record_header hdr;
	if(!parse_and_check_crc32_for_log_record_header_at(&hdr, file_offset_of_log_record, &(wale_p->block_io_functions), error))
		goto EXIT;

	uint64_t total_size_curr_log_record = HEADER_SIZE + ((uint64_t)(hdr.curr_log_record_size)) + UINT64_C(8); // 4 for crc32 of the log record itself and 4 for crc32 of the header

	// check for overflow
	if(will_unsigned_sum_overflow(uint64_t, log_sequence_number, total_size_curr_log_record))
	{
		(*error) = HEADER_CORRUPTED;
		goto EXIT;
	}

	// the next_log_sequence_number is right after this log_record
	next_log_sequence_number = log_sequence_number + total_size_curr_log_record;

	// next_log_sequence_number can not be higher than the on_disk_master_record.last_flushed_log_sequence_number
	if(next_log_sequence_number > wale_p->on_disk_master_record.last_flushed_log_sequence_number)
	{
		(*error) = HEADER_CORRUPTED;
		goto EXIT;
	}

	EXIT:;
	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return next_log_sequence_number;
}

uint64_t get_prev_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number, int* error)
{
	// initialize error to no error
	(*error) = NO_ERROR;

	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint64_t prev_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// if the wale has no records, OR the log_sequence_number is not within first and last_flushed log_sequence_number then fail
	if(wale_p->on_disk_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER ||
		log_sequence_number < wale_p->on_disk_master_record.first_log_sequence_number || 
		wale_p->on_disk_master_record.last_flushed_log_sequence_number < log_sequence_number
		)
	{
		(*error) = PARAM_INVALID;
		goto EXIT;
	}

	// prev of first_log_sequence_number does not exists
	if(log_sequence_number == wale_p->on_disk_master_record.first_log_sequence_number)
		goto EXIT;

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

	log_record_header hdr;
	if(!parse_and_check_crc32_for_log_record_header_at(&hdr, file_offset_of_log_record, &(wale_p->block_io_functions), error))
		goto EXIT;

	uint64_t total_size_prev_log_record = HEADER_SIZE + ((uint64_t)(hdr.prev_log_record_size)) + UINT64_C(8); // 4 for crc32 of the previous log record and 4 for crc32 of its header

	// log_sequence_number must be greater than the total_size of the prev_log_record
	if(log_sequence_number <= total_size_prev_log_record)
	{
		(*error) = HEADER_CORRUPTED;
		goto EXIT;
	}

	// the prev_log_sequence_number is right before this one
	prev_log_sequence_number = log_sequence_number - total_size_prev_log_record;

	// previous log_sequence_number must be greater than or equal to the first_log_sequence_number
	if(prev_log_sequence_number < wale_p->on_disk_master_record.first_log_sequence_number)
	{
		(*error) = HEADER_CORRUPTED;
		goto EXIT;
	}

	EXIT:;
	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return prev_log_sequence_number;
}

void* get_log_record_at(wale* wale_p, uint64_t log_sequence_number, uint32_t* log_record_size, int* error)
{
	// initialize error to no error
	(*error) = NO_ERROR;

	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to NULL, which is default result
	void* log_record = NULL;

	// if the wale has no records, OR its log_sequence_number is not between first and last_flushed log_sequence_number
	if(wale_p->on_disk_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER ||
		log_sequence_number < wale_p->on_disk_master_record.first_log_sequence_number ||
		wale_p->on_disk_master_record.last_flushed_log_sequence_number < log_sequence_number
		)
	{
		(*error) = PARAM_INVALID;
		goto EXIT;
	}

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset_of_log_record = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;

	log_record_header hdr;
	if(!parse_and_check_crc32_for_log_record_header_at(&hdr, file_offset_of_log_record, &(wale_p->block_io_functions), error))
		goto EXIT;

	// make sure that we will not be reading past or at the offset of wale_p->on_disk_master_record.next_log_sequence_number
	uint64_t total_log_size = HEADER_SIZE + ((uint64_t)(hdr.curr_log_record_size)) + UINT64_C(8); // 8 for both the crc32-s

	// make sure that the next_log_sequence_number of this log_record does not overflow
	if(will_unsigned_sum_overflow(uint64_t, log_sequence_number, total_log_size))
	{
		(*error) = PARAM_INVALID;
		goto EXIT;
	}

	uint64_t next_log_sequence_number = log_sequence_number + total_log_size;

	// the next log_sequence number of this log_record can not be more than the log_sequence number of the on_disk_master_record
	if(next_log_sequence_number > wale_p->on_disk_master_record.next_log_sequence_number)
	{
		(*error) = PARAM_INVALID;
		goto EXIT;
	}

	// calculate the offset of the log_record and its size including the crc32
	uint64_t log_record_offset = file_offset_of_log_record + HEADER_SIZE + UINT64_C(4);

	// allocate memeory for log record
	(*log_record_size) = hdr.curr_log_record_size;
	log_record = malloc((*log_record_size));
	if(log_record == NULL)
	{
		(*error) = ALLOCATION_FAILED;
		goto EXIT;
	}

	// read data for log_record from the file, data size amounting to log_record_size
	if(!random_read_at(log_record, (*log_record_size), log_record_offset, &(wale_p->block_io_functions)))
	{
		(*error) = READ_IO_ERROR;
		free(log_record);
		log_record = NULL;
		goto EXIT;
	}

	// calculate crc32 for the log_record read
	uint32_t calculated_crc32 = crc32_init();
	calculated_crc32 = crc32_util(calculated_crc32, log_record, (*log_record_size));

	// read crc for log_record from the file, data size amounting to log_record_size
	char crc_read[4];
	if(!random_read_at(crc_read, UINT64_C(4), log_record_offset + (*log_record_size), &(wale_p->block_io_functions)))
	{
		(*error) = READ_IO_ERROR;
		free(log_record);
		log_record = NULL;
		goto EXIT;
	}

	uint32_t parsed_crc32 = deserialize_le_uint32(crc_read);
	if(parsed_crc32 != calculated_crc32)
	{
		(*error) = LOG_RECORD_CORRUPTED;
		free(log_record);
		log_record = NULL;
		goto EXIT;
	}

	EXIT:;
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

static uint64_t get_log_sequence_number_for_next_log_record_and_advance_master_record(wale* wale_p, uint32_t log_record_size, int is_check_point, uint32_t* prev_log_record_size)
{
	// compute the total slot size required by this new log record
	uint64_t total_log_record_slot_size = HEADER_SIZE + ((uint64_t)log_record_size) + UINT64_C(8); // 8 for crc32 of header and log record itself

	// its log sequence number will simply be the next log sequence number
	uint64_t log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number;

	// check for overflow of the next_log_sequence_number, upon alloting this slot
	uint64_t new_next_log_sequence_number = log_sequence_number + total_log_record_slot_size;
	if(new_next_log_sequence_number < log_sequence_number || new_next_log_sequence_number < total_log_record_slot_size)
		return INVALID_LOG_SEQUENCE_NUMBER;

	// if earlier there were no log records on the disk, then this will be the new first_log_sequence_number
	if(wale_p->in_memory_master_record.first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
		wale_p->in_memory_master_record.first_log_sequence_number = log_sequence_number;

	// if there was a last_flushed_log_sequence_number, then return also its size
	if(wale_p->in_memory_master_record.last_flushed_log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER)
		(*prev_log_record_size) = (wale_p->in_memory_master_record.next_log_sequence_number - wale_p->in_memory_master_record.last_flushed_log_sequence_number) - HEADER_SIZE - UINT64_C(8);
	else
		(*prev_log_record_size) = 0;

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

			// upgrade your shared lock on the append_only_buffer to exclusive lock while we scroll
			upgrade_lock(&(wale_p->append_only_buffer_lock), BLOCKING);

			// scroll and preserve the scroll error for the caller to see
			(*error_in_scroll) = !scroll_append_only_buffer(wale_p);

			// downgrade back to shared_lock on the append_only_buffer after the scroll
			downgrade_lock(&(wale_p->append_only_buffer_lock));

			if(!(*error_in_scroll))
			{
				// take the probably the first slot and advance the append_offset to how much we can write at most
				(*append_slot) = wale_p->append_offset;
				wale_p->append_offset = min(wale_p->append_offset + (*total_bytes_to_write_for_this_log_record), wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

				// if there is space in the append_only_buffer then we wake other writers to append_only_buffer, who are waiting for append_only_buffer to scroll to the next_log_sequence_number
				if(wale_p->append_offset < wale_p->buffer_block_count * wale_p->block_io_functions.block_size)
					pthread_cond_broadcast(&(wale_p->wait_for_scroll));
			}
			else
			{
				// in case of scroll error, wake up any threads waiting for a successfull scroll
				wale_p->major_scroll_error = 1;
				pthread_cond_broadcast(&(wale_p->wait_for_scroll));
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
	uint32_t prev_log_record_size;
	log_sequence_number = get_log_sequence_number_for_next_log_record_and_advance_master_record(wale_p, log_record_size, is_check_point, &prev_log_record_size);

	// exit suggesting failure to allocate a log_sequence_number
	if(log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
		goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;

	// compute the total bytes we will write
	uint64_t total_bytes_to_write = HEADER_SIZE + ((uint64_t)log_record_size) + UINT64_C(8); // 8 for the 2 crc32 values of the header and the log record each

	// now take the slot in the append only buffer
	uint64_t append_slot = wale_p->append_offset;

	// advance the append_offset of the append only buffer
	wale_p->append_offset = min(wale_p->append_offset + total_bytes_to_write, wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

	// we have the slot in the append only buffer, and a log_sequence_number, now we don't need the global lock
	pthread_mutex_unlock(get_wale_lock(wale_p));

	// serialize log_record_size as a byte array ordered in little endian format
	char size_in_bytes[4];
	uint32_t calculated_crc32 = crc32_init();

	int scroll_error = 0;

	// write prev_log_record_size
	serialize_le_uint32(size_in_bytes, prev_log_record_size);
	calculated_crc32 = crc32_util(calculated_crc32, size_in_bytes, 4);
	append_log_record_data(wale_p, &append_slot, size_in_bytes, 4, &total_bytes_to_write, &scroll_error);
	if(scroll_error)
		goto SCROLL_FAIL;

	// write log_record_size
	serialize_le_uint32(size_in_bytes, log_record_size);
	calculated_crc32 = crc32_util(calculated_crc32, size_in_bytes, 4);
	append_log_record_data(wale_p, &append_slot, size_in_bytes, 4, &total_bytes_to_write, &scroll_error);
	if(scroll_error)
		goto SCROLL_FAIL;

	// write calculated_crc32
	serialize_le_uint32(size_in_bytes, calculated_crc32);
	append_log_record_data(wale_p, &append_slot, size_in_bytes, 4, &total_bytes_to_write, &scroll_error);
	if(scroll_error)
		goto SCROLL_FAIL;

	// reinitialize the calculated_crc32
	calculated_crc32 = crc32_init();

	// write log record itself
	append_log_record_data(wale_p, &append_slot, log_record, log_record_size, &total_bytes_to_write, &scroll_error);
	calculated_crc32 = crc32_util(calculated_crc32, log_record, log_record_size);
	if(scroll_error)
		goto SCROLL_FAIL;

	// write calculated_crc32
	serialize_le_uint32(size_in_bytes, calculated_crc32);
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

	// get exclusive_lock on the append_only_buffer
	exclusive_lock(&(wale_p->append_only_buffer_lock), BLOCKING);

	// we can not flush if there has been a major scroll error
	if(wale_p->major_scroll_error)
	{
		// release exclusive lock and exit
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// perform a scroll
	int scroll_success = scroll_append_only_buffer(wale_p);

	// if scroll was a failure, set the 
	if(!scroll_success)
	{
		wale_p->major_scroll_error = 1;
		goto EXIT;
	}

	// wake up any thread that was waiting for scroll to finish (even if there is scroll_error, we need to wake them up to let them know about it)
	pthread_cond_broadcast(&(wale_p->wait_for_scroll));

	// copy the valid values for flushing the on disk master record, before we release the global mutex lock
	master_record new_on_disk_master_record = wale_p->in_memory_master_record;

	// we now need to write the new_on_disk_master_record to the on_disk_master_record and the actual on-disk master record, with respective flushes
	write_lock(&(wale_p->flushed_log_records_lock), BLOCKING);

	// release exclusive lock after the scroll is complete
	exclusive_unlock(&(wale_p->append_only_buffer_lock));

	// here the above locking order is essential, it ensures that the updates of flush are always installed in lock step order in the event of 2 ongoing flushes

	// As you can predict/observe/analyze, now from here on, other append only writers, scrollers and flushes can proceed with their task concurrently with this one

	// release the lock
	pthread_mutex_unlock(get_wale_lock(wale_p));

	int flush_success = wale_p->block_io_functions.flush_all_writes(wale_p->block_io_functions.block_io_ops_handle) 
	&& write_and_flush_master_record(&new_on_disk_master_record, &(wale_p->block_io_functions));

	if(flush_success)
	{
		// update the on_disk_master_record to the new value
		wale_p->on_disk_master_record = new_on_disk_master_record;

		// also set the return value
		last_flushed_log_sequence_number = new_on_disk_master_record.last_flushed_log_sequence_number;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	// release write lock on the flushed_log_records
	write_unlock(&(wale_p->flushed_log_records_lock));

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

	// take exclusive lock on the append only buffer_lock,
	// this ensures all appenders to the append only buffer have exited
	// their writes may be in the buffer and we are unconcerned with that
	exclusive_lock(&(wale_p->append_only_buffer_lock), BLOCKING);

	// we can not flush if there has been a major scroll error
	if(wale_p->major_scroll_error)
	{
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// next_log_sequence_number is not advanced
	master_record new_master_record = {
		.first_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.check_point_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.next_log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number,
	};
	uint64_t new_append_offset = 0;

	// now we also need write lock on the on_disk_master_record, so that we can update it, along with the actual ondisk master record
	write_lock(&(wale_p->flushed_log_records_lock), BLOCKING);

	pthread_mutex_unlock(get_wale_lock(wale_p));

	truncated_logs = write_and_flush_master_record(&new_master_record, &(wale_p->block_io_functions));

	if(truncated_logs)
	{
		// we can update the on_disk_master_record here since, we have write lock on flushed_log_records_lock
		wale_p->on_disk_master_record = new_master_record;

		// we can update the buffer_start_block_id here since, we have write lock on append_only_buffer_lock
		wale_p->buffer_start_block_id = 1;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	if(truncated_logs)
	{
		// below attributes are protected by the global mutex, hence must be updated within the mutex
		wale_p->in_memory_master_record = new_master_record;
		wale_p->append_offset = new_append_offset;
	}

	// release both the exclusive locks
	write_unlock(&(wale_p->flushed_log_records_lock));
	exclusive_unlock(&(wale_p->append_only_buffer_lock));

	EXIT:;
	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return truncated_logs;
}
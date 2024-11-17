#include<wale.h>

#include<wale_get_lock_util.h>
#include<crc32_util.h>
#include<util_random_read.h>
#include<util_append_only_buffer.h>
#include<util_master_record.h>
#include<block_io_ops_util.h>

#include<rwlock.h>

#include<serial_int.h>

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

uint32_t get_log_sequence_number_width(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint32_t log_sequence_number_width = wale_p->on_disk_master_record.log_sequence_number_width;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return log_sequence_number_width;
}

uint256 get_first_log_sequence_number(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint256 first_log_sequence_number = wale_p->on_disk_master_record.first_log_sequence_number;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return first_log_sequence_number;
}

uint256 get_last_flushed_log_sequence_number(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint256 last_flushed_log_sequence_number = wale_p->on_disk_master_record.last_flushed_log_sequence_number;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return last_flushed_log_sequence_number;
}

uint256 get_check_point_log_sequence_number(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint256 check_point_log_sequence_number = wale_p->on_disk_master_record.check_point_log_sequence_number;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return check_point_log_sequence_number;
}

uint256 get_next_log_sequence_number(wale* wale_p)
{
	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	uint256 next_log_sequence_number = wale_p->on_disk_master_record.next_log_sequence_number;

	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return next_log_sequence_number;
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
	result->prev_log_record_size = deserialize_uint32(serial_header + 0, sizeof(uint32_t));
	result->curr_log_record_size = deserialize_uint32(serial_header + 4, sizeof(uint32_t));
	uint32_t parsed_crc32 = deserialize_uint32(serial_header + 8, sizeof(uint32_t));

	// compare the parsed crc32 with the calculated one
	if(parsed_crc32 != calcuated_crc32)
	{
		(*error) = HEADER_CORRUPTED;
		return 0;
	}

	(*error) = NO_ERROR;
	return 1;
}

uint256 get_next_log_sequence_number_of(wale* wale_p, uint256 log_sequence_number, int skip_flushed_checks, int* error)
{
	// primary check, you may never provide INVALID_LOG_SEQUENCE_NUMBER
	if(are_equal_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
	{
		(*error) = PARAM_INVALID;
		return INVALID_LOG_SEQUENCE_NUMBER;
	}

	// initialize error to no error
	(*error) = NO_ERROR;

	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint256 next_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// next of last_flushed_log_sequence_number does not exists, only to be done, if we are not allowed to skip_flushed_checks
	if(!skip_flushed_checks)
		if(are_equal_uint256(log_sequence_number, wale_p->on_disk_master_record.last_flushed_log_sequence_number))
			goto EXIT;

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset_of_log_record = get_file_offset_for_log_sequence_number(log_sequence_number, &(wale_p->on_disk_master_record), &(wale_p->block_io_functions), error);
	if(*error)
		goto EXIT;

	log_record_header hdr;
	if(!parse_and_check_crc32_for_log_record_header_at(&hdr, file_offset_of_log_record, &(wale_p->block_io_functions), error))
		goto EXIT;

	// perform the check for validity of next_log_sequence_number only if we are not allowed to skip_flushed_checks
	if(!skip_flushed_checks)
	{
		uint64_t total_size_curr_log_record = HEADER_SIZE + ((uint64_t)(hdr.curr_log_record_size)) + UINT64_C(8); // 4 for crc32 of the log record itself and 4 for crc32 of the header

		// the next_log_sequence_number is right after this log_record
		if(!add_overflow_safe_uint256(&next_log_sequence_number, log_sequence_number, get_uint256(total_size_curr_log_record), wale_p->max_limit))
		{
			(*error) = HEADER_CORRUPTED;
			goto EXIT;
		}

		// next_log_sequence_number can not be higher than the on_disk_master_record.last_flushed_log_sequence_number
		if(compare_uint256(next_log_sequence_number, wale_p->on_disk_master_record.last_flushed_log_sequence_number) > 0)
		{
			(*error) = HEADER_CORRUPTED;
			goto EXIT;
		}
	}

	EXIT:;
	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return next_log_sequence_number;
}

uint256 get_prev_log_sequence_number_of(wale* wale_p, uint256 log_sequence_number, int skip_flushed_checks, int* error)
{
	// primary check, you may never provide INVALID_LOG_SEQUENCE_NUMBER
	if(are_equal_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
	{
		(*error) = PARAM_INVALID;
		return INVALID_LOG_SEQUENCE_NUMBER;
	}

	// initialize error to no error
	(*error) = NO_ERROR;

	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to INVALID_LOG_SEQUENCE_NUMBER, which is default result
	uint256 prev_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// prev of first_log_sequence_number does not exists
	if(are_equal_uint256(log_sequence_number, wale_p->on_disk_master_record.first_log_sequence_number))
		goto EXIT;

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset_of_log_record = get_file_offset_for_log_sequence_number(log_sequence_number, &(wale_p->on_disk_master_record), &(wale_p->block_io_functions), error);
	if(*error)
		goto EXIT;

	log_record_header hdr;
	if(!parse_and_check_crc32_for_log_record_header_at(&hdr, file_offset_of_log_record, &(wale_p->block_io_functions), error))
		goto EXIT;

	uint64_t total_size_prev_log_record = HEADER_SIZE + ((uint64_t)(hdr.prev_log_record_size)) + UINT64_C(8); // 4 for crc32 of the previous log record and 4 for crc32 of its header

	// the prev_log_sequence_number is right before this one
	// it can not be equal to the total_size_prev_record, else prev_log_sequence_number will become 0, i.e. INVALID_LOG_SEQUENCE_NUMBER
	if(are_equal_uint256(log_sequence_number, get_uint256(total_size_prev_log_record)) ||
		!sub_underflow_safe_uint256(&prev_log_sequence_number, log_sequence_number, get_uint256(total_size_prev_log_record)))
	{
		(*error) = HEADER_CORRUPTED;
		goto EXIT;
	}

	// previous log_sequence_number must be greater than or equal to the first_log_sequence_number
	if(compare_uint256(prev_log_sequence_number, wale_p->on_disk_master_record.first_log_sequence_number) < 0)
	{
		(*error) = HEADER_CORRUPTED;
		goto EXIT;
	}

	EXIT:;
	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return prev_log_sequence_number;
}

void* get_log_record_at(wale* wale_p, uint256 log_sequence_number, uint32_t* log_record_size, int skip_flushed_checks, int* error)
{
	// primary check, you may never provide INVALID_LOG_SEQUENCE_NUMBER
	if(are_equal_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
	{
		(*error) = PARAM_INVALID;
		return NULL;
	}

	// initialize error to no error
	(*error) = NO_ERROR;

	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// set it to NULL, which is default result
	void* log_record = NULL;

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset_of_log_record = get_file_offset_for_log_sequence_number(log_sequence_number, &(wale_p->on_disk_master_record), &(wale_p->block_io_functions), error);
	if(*error)
		goto EXIT;

	log_record_header hdr;
	if(!parse_and_check_crc32_for_log_record_header_at(&hdr, file_offset_of_log_record, &(wale_p->block_io_functions), error))
		goto EXIT;

	// perform the check for validity of next_log_sequence_number only if we are not allowed to skip_flushed_checks
	if(!skip_flushed_checks)
	{
		// make sure that we will not be reading past or at the offset of wale_p->on_disk_master_record.next_log_sequence_number
		uint64_t total_log_size = HEADER_SIZE + ((uint64_t)(hdr.curr_log_record_size)) + UINT64_C(8); // 8 for both the crc32-s

		// make sure that the next_log_sequence_number of this log_record does not overflow
		uint256 next_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		if(!add_overflow_safe_uint256(&next_log_sequence_number, log_sequence_number, get_uint256(total_log_size), wale_p->max_limit))
		{
			(*error) = PARAM_INVALID;
			goto EXIT;
		}

		// the next log_sequence number of this log_record can not be more than the next log_sequence number of the on_disk_master_record
		if(compare_uint256(next_log_sequence_number, wale_p->on_disk_master_record.next_log_sequence_number) > 0)
		{
			(*error) = PARAM_INVALID;
			goto EXIT;
		}
	}

	// calculate the offset of the log_record
	uint64_t log_record_offset = file_offset_of_log_record + HEADER_SIZE + UINT64_C(4);

	// allocate memory for log record
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

	uint32_t parsed_crc32 = deserialize_uint32(crc_read, sizeof(uint32_t));
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

int validate_log_record_at(wale* wale_p, uint256 log_sequence_number, uint32_t* log_record_size, int* error)
{
	// primary check, you may never provide INVALID_LOG_SEQUENCE_NUMBER
	if(are_equal_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
	{
		(*error) = PARAM_INVALID;
		return 0;
	}

	// initialize error to no error
	(*error) = NO_ERROR;

	prefix_to_acquire_flushed_log_records_reader_lock(wale_p);

	// default return valus
	int valid = 0;

	// calculate the offset in file of the log_record at log_sequence_number
	uint64_t file_offset_of_log_record = get_file_offset_for_log_sequence_number(log_sequence_number, &(wale_p->on_disk_master_record), &(wale_p->block_io_functions), error);
	if(*error)
		goto EXIT;

	log_record_header hdr;
	if(!parse_and_check_crc32_for_log_record_header_at(&hdr, file_offset_of_log_record, &(wale_p->block_io_functions), error))
		goto EXIT;

	// make sure that we will not be reading past or at the offset of wale_p->on_disk_master_record.next_log_sequence_number
	uint64_t total_log_size = HEADER_SIZE + ((uint64_t)(hdr.curr_log_record_size)) + UINT64_C(8); // 8 for both the crc32-s

	// make sure that the next_log_sequence_number of this log_record does not overflow
	uint256 next_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
	if(!add_overflow_safe_uint256(&next_log_sequence_number, log_sequence_number, get_uint256(total_log_size), wale_p->max_limit))
	{
		(*error) = PARAM_INVALID;
		goto EXIT;
	}

	// the next log_sequence number of this log_record can not be more than the next log_sequence number of the on_disk_master_record
	if(compare_uint256(next_log_sequence_number, wale_p->on_disk_master_record.next_log_sequence_number) > 0)
	{
		(*error) = PARAM_INVALID;
		goto EXIT;
	}

	// calculate the offset of the log_record
	uint64_t log_record_offset = file_offset_of_log_record + HEADER_SIZE + UINT64_C(4);

	// set the valid log_record_size
	(*log_record_size) = hdr.curr_log_record_size;

	// calculate crc32 for the log_record, block by block
	uint32_t calculated_crc32 = crc32_init();
	if(!crc32_at(&calculated_crc32, (*log_record_size), log_record_offset, &(wale_p->block_io_functions)))
	{
		(*error) = READ_IO_ERROR;
		goto EXIT;
	}

	// read crc for log_record from the file, data size amounting to log_record_size
	char crc_read[4];
	if(!random_read_at(crc_read, UINT64_C(4), log_record_offset + (*log_record_size), &(wale_p->block_io_functions)))
	{
		(*error) = READ_IO_ERROR;
		goto EXIT;
	}

	uint32_t parsed_crc32 = deserialize_uint32(crc_read, sizeof(uint32_t));
	if(parsed_crc32 != calculated_crc32)
	{
		(*error) = LOG_RECORD_CORRUPTED;
		goto EXIT;
	}

	valid = 1;

	EXIT:;
	suffix_to_release_flushed_log_records_reader_lock(wale_p);

	return valid;
}

int modify_append_only_buffer_block_count(wale* wale_p, uint64_t buffer_block_count, int* error)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	exclusive_lock(&(wale_p->append_only_buffer_lock), BLOCKING);

	int res = resize_append_only_buffer(wale_p, buffer_block_count, error);

	// if the buffer_block_count increased, i.e. now there is more space on it -> this is equivalent to a scroll
	// else if the buffer_block_count became 0 i.e. now wale is read only, then wake up anyone who is waiting for a scroll to let then know about it
	// there is also possibility that resize_append_only_buffer, scrolled the buffer (if the buffer size is to be decreased)
	// so in any case scroll the buffer
	// to be on safe side, we wake up all threads waiting for scroll, to inform them about the change in buffer_block_count
	// resizing the append only buffer is expectd to an infrequent operation, hence hopefully waking up all the threads is just fine
	pthread_cond_broadcast(&(wale_p->wait_for_scroll));

	exclusive_unlock(&(wale_p->append_only_buffer_lock));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return res;
}

static uint256 get_log_sequence_number_for_next_log_record_and_advance_master_record(wale* wale_p, uint32_t log_record_size, int is_check_point, uint32_t* prev_log_record_size, int* error)
{
	// compute the total slot size required by this new log record
	uint64_t total_log_record_slot_size = HEADER_SIZE + ((uint64_t)log_record_size) + UINT64_C(8); // 8 for crc32 of header and log record itself

	// its log sequence number will simply be the next log sequence number
	// check for overflow of the next_log_sequence_number, upon alloting this slot
	// we do not advance the master record, if the next_log_sequence_number overflows
	uint256 log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number;
	uint256 new_next_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
	if(!add_overflow_safe_uint256(&(new_next_log_sequence_number), wale_p->in_memory_master_record.next_log_sequence_number, get_uint256(total_log_record_slot_size), wale_p->max_limit))
	{
		(*error) = LOG_SEQUENCE_NUMBER_OVERFLOW;
		return INVALID_LOG_SEQUENCE_NUMBER;
	}

	// if there was a last_flushed_log_sequence_number, then return also its size
	if(!are_equal_uint256(wale_p->in_memory_master_record.last_flushed_log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
	{
		uint64_t prev_log_record_total_size;
		{
			uint256 temp;
			if(!sub_underflow_safe_uint256(&temp, wale_p->in_memory_master_record.next_log_sequence_number, wale_p->in_memory_master_record.last_flushed_log_sequence_number) ||
				!cast_to_uint64_from_uint256(&prev_log_record_total_size, temp))
				return INVALID_LOG_SEQUENCE_NUMBER;

		}
		(*prev_log_record_size) = prev_log_record_total_size - HEADER_SIZE - UINT64_C(8);
	}
	else
		(*prev_log_record_size) = 0;

	// if earlier there were no log records on the disk, then this will be the new first_log_sequence_number
	if(are_equal_uint256(wale_p->in_memory_master_record.first_log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
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
			// scrolling needs global lock and an exclusive lock on the wale_p->append_only_buffer_lock
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

uint256 append_log_record(wale* wale_p, const void* log_record, uint32_t log_record_size, int is_check_point, int* error)
{
	// initialize error to no error
	(*error) = NO_ERROR;

	// return value defaults to an INVALID_LOG_SEQUENCE_NUMBER
	uint256 log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	// compute the total bytes we will write
	uint64_t total_bytes_to_write = HEADER_SIZE + ((uint64_t)log_record_size) + UINT64_C(8); // 8 for the 2 crc32 values of the header and the log record each

	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	// share lock the append_only_buffer, inorder to write data into it at the wale_p->append_offset
	// we take this lock this early, because we do not want anyone to scroll the append only buffer, after we get a slot in the append only buffer
	shared_lock(&(wale_p->append_only_buffer_lock), WRITE_PREFERRING, BLOCKING);

	while(wale_p->buffer_block_count > 0)
	{
		uint64_t file_offset_for_next_log_sequence_number = get_file_offset_for_next_log_sequence_number(&(wale_p->in_memory_master_record), &(wale_p->block_io_functions), error);
		if(*error)
			goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;

		// this is the case when the file_offset_for_next_log_sequence_number is valid,
		// but it will become invalid for the log_record that goes after it, so we fail preemptively
		if(will_unsigned_sum_overflow(uint64_t, file_offset_for_next_log_sequence_number, total_bytes_to_write))
		{
			(*error) = FILE_OFFSET_OVERFLOW;
			goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;
		}

		if(!wale_p->major_scroll_error && wale_p->buffer_block_count > 0 &&
			!is_file_offset_within_append_only_buffer(wale_p, file_offset_for_next_log_sequence_number))
		{
			shared_unlock(&(wale_p->append_only_buffer_lock));
			pthread_cond_wait(&(wale_p->wait_for_scroll), get_wale_lock(wale_p));
			shared_lock(&(wale_p->append_only_buffer_lock), WRITE_PREFERRING, BLOCKING);
		}
		else
			break;
	}

	// if the buffer block count is 0, then WALe is not in writable state
	if(wale_p->buffer_block_count == 0)
	{
		(*error) = ZERO_BUFFER_BLOCK_COUNT;
		goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;
	}

	if(wale_p->major_scroll_error)
	{
		(*error) = MAJOR_SCROLL_ERROR;
		goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;
	}

	// take slot if the next log sequence number is in the append only buffer
	uint32_t prev_log_record_size = 0;
	log_sequence_number = get_log_sequence_number_for_next_log_record_and_advance_master_record(wale_p, log_record_size, is_check_point, &prev_log_record_size, error);

	// exit suggesting failure to allocate a log_sequence_number
	if(are_equal_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
		goto RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT;

	// now take the slot in the append only buffer
	uint64_t append_slot = wale_p->append_offset;

	// advance the append_offset of the append only buffer
	wale_p->append_offset = min(wale_p->append_offset + total_bytes_to_write, wale_p->buffer_block_count * wale_p->block_io_functions.block_size);

	// we have the slot in the append only buffer, and a log_sequence_number, now we don't need the global lock
	pthread_mutex_unlock(get_wale_lock(wale_p));

	// serialize log_record_size as a byte array ordered in little endian format
	char bytes_for_uint32[4];
	uint32_t calculated_crc32 = crc32_init();

	// write prev_log_record_size
	serialize_uint32(bytes_for_uint32, sizeof(uint32_t), prev_log_record_size);
	calculated_crc32 = crc32_util(calculated_crc32, bytes_for_uint32, 4);
	append_log_record_data(wale_p, &append_slot, bytes_for_uint32, 4, &total_bytes_to_write, error);
	if(*error)
		goto SCROLL_FAIL;

	// write log_record_size
	serialize_uint32(bytes_for_uint32, sizeof(uint32_t), log_record_size);
	calculated_crc32 = crc32_util(calculated_crc32, bytes_for_uint32, 4);
	append_log_record_data(wale_p, &append_slot, bytes_for_uint32, 4, &total_bytes_to_write, error);
	if(*error)
		goto SCROLL_FAIL;

	// write calculated_crc32
	serialize_uint32(bytes_for_uint32, sizeof(uint32_t), calculated_crc32);
	append_log_record_data(wale_p, &append_slot, bytes_for_uint32, 4, &total_bytes_to_write, error);
	if(*error)
		goto SCROLL_FAIL;

	// reinitialize the calculated_crc32
	calculated_crc32 = crc32_init();

	// write log record itself
	append_log_record_data(wale_p, &append_slot, log_record, log_record_size, &total_bytes_to_write, error);
	calculated_crc32 = crc32_util(calculated_crc32, log_record, log_record_size);
	if(*error)
		goto SCROLL_FAIL;

	// write calculated_crc32
	serialize_uint32(bytes_for_uint32, sizeof(uint32_t), calculated_crc32);
	append_log_record_data(wale_p, &append_slot, bytes_for_uint32, 4, &total_bytes_to_write, error);
	if(*error)
		goto SCROLL_FAIL;

	SCROLL_FAIL:;
	pthread_mutex_lock(get_wale_lock(wale_p));

	// this condition implies a fail to scroll the append only buffer
	if(*error)
		log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	RELEASE_SHARE_LOCK_ON_APPEND_ONLY_BUFFER_AND_EXIT:;
	// share_unlock the append_only_buffer
	shared_unlock(&(wale_p->append_only_buffer_lock));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return log_sequence_number;
}

uint256 flush_all_log_records(wale* wale_p, int* error)
{
	// initialize error to no error
	(*error) = NO_ERROR;

	// return value defaults to INVALID_LOG_SEQUENCE_NUMBER
	uint256 last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	// get exclusive_lock on the append_only_buffer
	// this waits only until, all append_log_record calls that were allotted be written to buffer (they may scroll if they will)
	exclusive_lock(&(wale_p->append_only_buffer_lock), BLOCKING);

	// if the buffer block count is 0, then WALe is not in writable state
	if(wale_p->buffer_block_count == 0)
	{
		(*error) = ZERO_BUFFER_BLOCK_COUNT;
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// we can not flush if there has been a major scroll error
	if(wale_p->major_scroll_error)
	{
		// release exclusive lock and exit
		(*error) = MAJOR_SCROLL_ERROR;
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// perform a scroll
	int scroll_success = scroll_append_only_buffer(wale_p);

	// if scroll was a failure, set the 
	if(!scroll_success)
	{
		wale_p->major_scroll_error = 1;
		(*error) = MAJOR_SCROLL_ERROR;
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// wake up any thread that was waiting for scroll to finish (even if there is scroll_error, we need to wake them up to let them know about it)
	pthread_cond_broadcast(&(wale_p->wait_for_scroll));

	// copy the valid values for flushing the on disk master record, before we release the global mutex lock
	const master_record new_on_disk_master_record = wale_p->in_memory_master_record;

	// we now need to write the new_on_disk_master_record to the on_disk_master_record and the actual on-disk master record, with respective flushes
	write_lock(&(wale_p->flushed_log_records_lock), BLOCKING);

	// release exclusive lock after the scroll is complete
	exclusive_unlock(&(wale_p->append_only_buffer_lock));

	// here the above locking order is essential, it ensures that the updates of flush are always installed in lock step order in the event of 2 ongoing flushes

	// As you can predict/observe/analyze, now from here on, other append only writers, scrollers and flushes can proceed with their task concurrently with this one

	// release the global lock
	pthread_mutex_unlock(get_wale_lock(wale_p));

	int flush_success = 0;
	if(are_equal_master_records(&new_on_disk_master_record, &(wale_p->on_disk_master_record))) // if the new_on_disk_master_record equals the current on_disk_master_record, then flushes need not be performed
		flush_success = 1;
	else
		flush_success = wale_p->block_io_functions.flush_all_writes(wale_p->block_io_functions.block_io_ops_handle) 
		&& write_and_flush_master_record(&new_on_disk_master_record, &(wale_p->block_io_functions), error);

	if(flush_success)
	{
		// update the on_disk_master_record to the new value
		wale_p->on_disk_master_record = new_on_disk_master_record;

		// also set the return value
		last_flushed_log_sequence_number = new_on_disk_master_record.last_flushed_log_sequence_number;
	}
	else
	{
		if(*error == NO_ERROR)
			(*error) = WRITE_IO_ERROR;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	// release write lock on the flushed_log_records
	write_unlock(&(wale_p->flushed_log_records_lock));

	EXIT:;
	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return last_flushed_log_sequence_number;
}

// NOTE :: this is an external function, not to be used by internal functions
void scroll_append_only_buffer_inside_wale(wale* wale_p, int* error)
{
	// initialize error to no error
	(*error) = NO_ERROR;

	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	// get exclusive_lock on the append_only_buffer
	// this waits only until, all append_log_record calls that were allotted be written to buffer (they may scroll if they will)
	exclusive_lock(&(wale_p->append_only_buffer_lock), BLOCKING);

	// if the buffer block count is 0, then WALe is not in writable state
	if(wale_p->buffer_block_count == 0)
	{
		(*error) = ZERO_BUFFER_BLOCK_COUNT;
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// we can not flush if there has been a major scroll error
	if(wale_p->major_scroll_error)
	{
		// release exclusive lock and exit
		(*error) = MAJOR_SCROLL_ERROR;
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// perform a scroll
	int scroll_success = scroll_append_only_buffer(wale_p);

	// if scroll was a failure, set the 
	if(!scroll_success)
	{
		wale_p->major_scroll_error = 1;
		(*error) = MAJOR_SCROLL_ERROR;
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// wake up any thread that was waiting for scroll to finish (even if there is scroll_error, we need to wake them up to let them know about it)
	pthread_cond_broadcast(&(wale_p->wait_for_scroll));

	// release exclusive lock after the scroll is complete
	exclusive_unlock(&(wale_p->append_only_buffer_lock));

	EXIT:;
	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));
}

uint256 discard_unflushed_log_records(wale* wale_p, int* error)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	// default return value, on failure
	uint256 last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;

	exclusive_lock(&(wale_p->append_only_buffer_lock), BLOCKING);

	// read new in_memory_master_record
	read_lock(&(wale_p->flushed_log_records_lock), READ_PREFERRING, BLOCKING);
	master_record new_in_memory_master_record = wale_p->on_disk_master_record;
	read_unlock(&(wale_p->flushed_log_records_lock));

	// if the buffer block count is 0, then WALe is not in writable state
	if(wale_p->buffer_block_count == 0)
	{
		(*error) = ZERO_BUFFER_BLOCK_COUNT;
		goto EXIT;
	}

	// we did not scroll, but if there was a major scroll error we may aswell exit with a failure
	if(wale_p->major_scroll_error)
	{
		(*error) = MAJOR_SCROLL_ERROR;
		goto EXIT;
	}

	// update the contents of the append_only_buffer, by reading the latest bytes of flushed records from the disk
	// we can release the global lock here, no worries
	pthread_mutex_unlock(get_wale_lock(wale_p));

	uint64_t file_offset_for_next_log_sequence_number = read_latest_vacant_block_using_master_record(wale_p->buffer, &new_in_memory_master_record, &(wale_p->block_io_functions), error);

	pthread_mutex_lock(get_wale_lock(wale_p));

	if(*error)
		goto EXIT;

	wale_p->in_memory_master_record = new_in_memory_master_record;
	wale_p->buffer_start_block_id = get_block_id_from_file_offset(file_offset_for_next_log_sequence_number, &(wale_p->block_io_functions));
	wale_p->append_offset = get_block_offset_from_file_offset(file_offset_for_next_log_sequence_number, &(wale_p->block_io_functions));
	
	// since after the above read call the append_only_buffer must contain more space to write, we will wake up any thread that is waiting for a scroll
	pthread_cond_broadcast(&(wale_p->wait_for_scroll));

	// return value
	last_flushed_log_sequence_number = wale_p->in_memory_master_record.last_flushed_log_sequence_number;

	EXIT:;
	exclusive_unlock(&(wale_p->append_only_buffer_lock));

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return last_flushed_log_sequence_number;
}

int truncate_log_records(wale* wale_p, int* error)
{
	// initialize error to no error
	(*error) = NO_ERROR;

	// return value, suggesting if the log was truncated
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
		(*error) = MAJOR_SCROLL_ERROR;
		exclusive_unlock(&(wale_p->append_only_buffer_lock));
		goto EXIT;
	}

	// next_log_sequence_number is not advanced
	const master_record new_master_record = {
		.log_sequence_number_width = wale_p->in_memory_master_record.log_sequence_number_width,
		.first_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.check_point_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER,
		.next_log_sequence_number = wale_p->in_memory_master_record.next_log_sequence_number,
	};
	uint64_t new_append_offset = 0;

	// now we also need write lock on the on_disk_master_record, so that we can update it, along with the actual ondisk master record
	write_lock(&(wale_p->flushed_log_records_lock), BLOCKING);

	// performing io with out the lock
	pthread_mutex_unlock(get_wale_lock(wale_p));

	int master_record_io_error = 0;
	truncated_logs = write_and_flush_master_record(&new_master_record, &(wale_p->block_io_functions), &master_record_io_error);

	pthread_mutex_lock(get_wale_lock(wale_p));

	if(truncated_logs)
	{
		// we can update the on_disk_master_record here since, we have write lock on flushed_log_records_lock
		wale_p->on_disk_master_record = new_master_record;

		// below attributes are protected by the global mutex, hence must be updated with the global mutex held
		wale_p->in_memory_master_record = new_master_record;
		wale_p->append_offset = new_append_offset;
		wale_p->buffer_start_block_id = 1;

		// no contents in the append_only_buffer, hence we can wake up any thread waiting for a scroll
		pthread_cond_broadcast(&(wale_p->wait_for_scroll));
	}

	// release both the exclusive locks
	write_unlock(&(wale_p->flushed_log_records_lock));
	exclusive_unlock(&(wale_p->append_only_buffer_lock));

	EXIT:;
	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return truncated_logs;
}
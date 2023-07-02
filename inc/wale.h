#ifndef WALE_H
#define WALE_H

#include<stdint.h>
#include<inttypes.h>

// 0 log sequence number will never show up in the wal file
#define INVALID_LOG_SEQUENCE_NUMBER UINT64_C(0)

typedef struct wale wale;
struct wale
{
	int has_internal_lock : 1;
	union
	{
		pthread_mutex_t* external_lock;
		pthread_mutex_t  internal_lock;
	};


	// --------------------------------------------------------

	// this is the contents of the block file, at block_id 0
	// it contains first_log_sequence_number, last_flushed_log_sequence_number and check_point_log_sequence_number
	// master_log_record is always block_io_functions.block_size sized
	void* master_log_record;

	// --------------------------------------------------------

	// last log sequence number that was appended
	// upon a flush the last_log_sequence_number becomes the last_flushed_log_sequence_number, in the master record
	uint64_t last_log_sequence_number;

	// the log sequence number of the next log record, to be inserted
	uint64_t next_log_sequence_number;

	// --------------------------------------------------------

	// Append only buffer

	// total memory at buffer = buffer_block_count * block_io_functions.block_size
	void* buffer;

	// append_offset is the number of bytes filled in the buffer
	// the next byte to be appended goes at append_offset
	uint64_t append_offset;

	// number of blocks pointed to by buffer
	uint64_t buffer_block_count;

	// buffer_start_block_id is the first block pointed to by the buffer
	// the first byte in the buffer is at buffer_start_block_id * block_io_functions.block_size
	uint64_t buffer_start_block_id;

	// --------------------------------------------------------

	// functions to perform contiguous block io
	block_io_ops block_io_functions;
}

// -------------------------------------------------------------
// initialize deinitialize functions for wale

int initialize_wale(wale* wale_p, pthread_mutex_t* external_lock, block_io_ops block_io_functions, uint64_t append_only_block_count);

void deinitialize_wale(wale* wale_p);

// -------------------------------------------------------------
// attributes of wale as stored in the master record

uint64_t get_first_log_sequence_number(wale* wale_p);

uint64_t get_last_flushed_log_sequence_number(wale* wale_p);

uint64_t get_check_point_log_sequence_number(wale* wale_p);

// -------------------------------------------------------------
// random reads in log file are required by the below functions
// the below functions may only be called for log sequence numbers between first_log_sequence_number and last_flushed_log_sequence_number
// and only while both of which are valid (!= INVALID_LOG_SEQUENCE_NUMBER)

uint64_t get_next_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number);

uint64_t get_prev_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number);

// you must free the returned memory
void* get_log_record_at(const logger_handle* handle, uint64_t log_sequence_number, uint32_t* log_record_size);

// -------------------------------------------------------------



#endif
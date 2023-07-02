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

#endif
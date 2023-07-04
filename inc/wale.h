#ifndef WALE_H
#define WALE_H

#include<stdint.h>
#include<inttypes.h>
#include<pthread.h>

#include<block_io_ops.h>

// 0 log sequence number will never show up in the wal file
#define INVALID_LOG_SEQUENCE_NUMBER UINT64_C(0)

/*
	Every log record is prefixed and suffixed with a uint32_t size, that allows us to mover backwards and forwards in the wale file
	The size in the suffix and prefix is the actual size of only the log record, and does not contain the size of the prefix and suffix
*/

typedef struct master_record master_record;
struct master_record
{
	// the log sequence number at offset block_io_functions.block_size in the block file
	uint64_t first_log_sequence_number;

	// the last log sequence number flushed to the disk
	uint64_t last_flushed_log_sequence_number;

	// log sequence number of the latest flushed checkpoint log record
	uint64_t check_point_log_sequence_number;

	// next log sequence number to allot
	uint64_t next_log_sequence_number;
};

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
	// functions to perform contiguous block io
	block_io_ops block_io_functions;

	// --------------------------------------------------------
	// cached structured copy of on disk persistent state of the wale's master record
	master_record on_disk_master_record;

	// --------------------------------------------------------
	// any updates are made here in memory, this master record must be flushed for changes to be persistent
	// any append_log_record will diverge the in_memory_master_record, from the on_disk_master_record
	master_record in_memory_master_record;

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

	// this bit will be set, when an unrecoverable scroll error occurs, this error needs a restart of your system
	// which will also mean a loss of certain wal log records 
	int major_scroll_error : 1;

	// --------------------------------------------------------

	// number of readers that are performing any io that needs a fixed value of on_disk_master_record
	uint64_t random_readers_count;

	// number of writers that need the append only buffer to not be scrolled
	uint64_t append_only_writers_count;

	// random_readers and append_only_writers do not block each other

	// a flag to be set to notify a flush is in progress
	// flush may not start until all the append_only_writers who have entered hve left the section
	// i.e. will not start until append_only_writers_count > 0, the random_readers_count may be any thing (>=0)
	int flush_in_progress : 1;

	// if the scrolling_in_progress is set then, there is some continuing writer that is scrolling the append only buffer
	// so any append only writer must wait
	int scrolling_in_progress : 1;

	int waiting_for_random_readers_to_exit_flag : 1;
	pthread_cond_t waiting_for_random_readers_to_exit;

	int waiting_for_append_only_writers_to_exit_flag : 1;
	pthread_cond_t waiting_for_append_only_writers_to_exit;

	// counter and condition variable to be used by random readers
	uint64_t random_readers_waiting_count;
	pthread_cond_t random_readers_waiting;

	// counter and condition variable to be used by append only writers
	uint64_t append_only_writers_waiting_count;
	pthread_cond_t append_only_writers_waiting;

	// threads wait here for completion of flush
	uint64_t flush_completion_waiting_count;
	pthread_cond_t flush_completion_waiting;
};

/*
** offset of any log record in file = log_sequence_number - first_log_sequence_number + block_io_functions.block_size
*/

// -------------------------------------------------------------
// initialize deinitialize functions for wale

// if next_log_sequence_number is INVALID_LOG_SEQUENCE_NUMBER
//   -> an existing wale file is initialized, on-disk master record is read from diskx
// else
//   -> a new wale file is initialized, a brand new master_record is written to disk

int initialize_wale(wale* wale_p, uint64_t next_log_sequence_number, pthread_mutex_t* external_lock, block_io_ops block_io_functions, uint64_t append_only_block_count);

void deinitialize_wale(wale* wale_p);

// -------------------------------------------------------------
// attributes of wale as stored in the on-disk master record

uint64_t get_first_log_sequence_number(wale* wale_p);

uint64_t get_last_flushed_log_sequence_number(wale* wale_p);

uint64_t get_check_point_log_sequence_number(wale* wale_p);

// -------------------------------------------------------------
// random reads in log file are required by the below functions
// the below functions may only be called for log sequence numbers between on-disk first_log_sequence_number and last_flushed_log_sequence_number
// and only while both of which are valid (!= INVALID_LOG_SEQUENCE_NUMBER)

uint64_t get_next_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number);

uint64_t get_prev_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number);

// you must free the returned memory
void* get_log_record_at(wale* wale_p, uint64_t log_sequence_number, uint32_t* log_record_size);

// -------------------------------------------------------------
// append functions

// returns the log record of the last log record inserted
// check_point is marked to be updated in the master record, if is_check_point is set
// the appended log_record is not permanent (neither is it's being checkpointed-ness) until a flush is successfull
// if the append was unsuccessfull INVALID_LOG_SEQUENCE_NUMBER will be returned, in such a situation it is best to exit the program
uint64_t append_log_record(wale* wale_p, const void* log_record, uint32_t log_record_size, int is_check_point);

// returns the last_flushed_log_sequence_number, after the flush
// it will first ensure that all the appended log records have been flushed and then it will rewrite the master record and flush it
// making it point to the new last_flushed_log_sequence_number, next_log_sequence_number and check_point_log_sequence_number
// if the flush was unsuccessfull INVALID_LOG_SEQUENCE_NUMBER will be returned, in such a situation, it is best to exit the program
uint64_t flush_all_log_records(wale* wale_p);

// truncates the log file logically, using only a write to the master record
// making first_log_sequence_number, last_flushed_log_sequence_number and check_point_log_sequence_number = 0
// next_log_sequence_number remains as it is, and that will be the next_log_sequence_number that will be alloted
int truncate_log_records(wale* wale_p);

#endif
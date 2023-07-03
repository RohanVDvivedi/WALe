#ifndef WALE_H
#define WALE_H

#include<stdint.h>
#include<inttypes.h>

// 0 log sequence number will never show up in the wal file
#define INVALID_LOG_SEQUENCE_NUMBER UINT64_C(0)

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

	// --------------------------------------------------------
	// all updates to the on_disk_master_record, in_memory_master_record and "scrolling up" of the append only buffer must be done while holding the global lock

	// --------------------------------------------------------

	int scrolling_up_append_only_buffer_in_progress : 1; // status to check for a scrolling up in progress

	int flush_in_progress : 1; // status to check if the flush has started or is in progress

	// all of random_reads of flushed records and append only writes can occur concurrently
	// a flush_all_log_records and truncate_log_records can only happen sequentially as both of them flush the master record

	uint64_t count_of_ongoing_random_reads;         // random readers of flushed records use this count
	uint64_t count_of_ongoing_append_only_writes;   // append_only writers use this count

	// wait for flush to finish
	pthread_cond_t wait_for_flush_to_finish;
	uint64_t count_of_threads_waiting_for_flush_to_finish;

	// wait for ongoing accesses (random reads of flushed log records and appends only writes) to finish
	pthread_cond_t wait_for_ongoing_accesses_to_finish;
	uint64_t count_of_threads_waiting_for_accesses_to_finish;

	// --------------------------------------------------------

	// functions to perform contiguous block io
	block_io_ops block_io_functions;
}

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
// if the append was unsuccessfull INVALID_LOG_SEQUENCE_NUMBER will be returned
uint64_t append_log_record(wale* wale_p, const void* log_record, uint32_t log_record_size, int is_check_point);

// returns the last_flushed_log_sequence_number, after the flush
// it will first ensure that all the appended log records have been flushed and then it will rewrite the master record and flush it
// making it point to the new last_flushed_log_sequence_number, next_log_sequence_number and check_point_log_sequence_number
// if the flush was unsuccessfull INVALID_LOG_SEQUENCE_NUMBER will be returned
uint64_t flush_all_log_records(wale* wale_p);

// truncates the log file logically, using only a write to the master record
// making first_log_sequence_number, last_flushed_log_sequence_number and check_point_log_sequence_number = 0
// next_log_sequence_number remains as it is, and that will be the next_log_sequence_number that will be alloted
int truncate_log_records(wale* wale_p);

#endif
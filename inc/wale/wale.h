#ifndef WALE_H
#define WALE_H

#include<stdint.h>
#include<inttypes.h>
#include<pthread.h>

#include<lockking/rwlock.h>

#include<wale/block_io_ops.h>
#include<serint/large_uints.h>

// 0 log sequence number will never show up in the wal file
#define INVALID_LOG_SEQUENCE_NUMBER get_0_uint256()

/*
	Each log record has the following format.
	all of the uint32_t's are in little endian format

	struct
	{
		// header
		uint32_t prev_log_record_size;
		uint32_t curr_log_record_size;
		uint32_t crc32_header;			// crc32 for only the above 2 fields

		// log record
		char log_record[curr_log_record_size];
		uint32_t crc32_log_record;		// crc32 for only the log_record
	};

	There is a different crc32 for the header and the log_record,
	This allows us to quickly traverse the log records in forward or backward direction using the information only in the header.
*/

typedef struct master_record master_record;
struct master_record
{
	// width of uint256 to use in bytes
	uint32_t log_sequence_number_width;

	// the log sequence number at offset block_io_functions.block_size in the block file
	uint256 first_log_sequence_number;

	// the last log sequence number flushed to the disk
	uint256 last_flushed_log_sequence_number;

	// log sequence number of the latest flushed checkpoint log record
	uint256 check_point_log_sequence_number;

	// next log sequence number to allot
	uint256 next_log_sequence_number;
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

	// below reader writer lock protects the on_disk_master_record and the flushed logs on the disk (which are considered read-only)
	rwlock flushed_log_records_lock;

	// --------------------------------------------------------

	// Append only buffer

	// total memory at buffer = buffer_block_count * block_io_functions.block_size
	// protected by the append_only_buffer_lock
	void* buffer;

	// number of blocks pointed to by buffer, this is fixed for most part, unless you call modify_append_only_buffer_block_count()
	// protected by the append_only_buffer_lock
	uint64_t buffer_block_count;

	// buffer_start_block_id is the first block pointed to by the buffer
	// the first byte in the buffer is at buffer_start_block_id * block_io_functions.block_size
	// protected by the append_only_buffer_lock
	uint64_t buffer_start_block_id;

	// a shared/exclusive lock for protecting the append only buffer
	rwlock append_only_buffer_lock;

	// --------------------------------------------------------

	//  below 4 attributes are protected by the global mutex lock (above, external or internal)
	// This lock ensures that we always read their consistent state
	// You must try to unlock the global mutex while performing io, to allow other reads and writes to pass through
	// This project ensures as much concurrency as possible

	// any updates to the master record are made here in memory, this master record must be flushed for changes to be persistent
	// any append_log_record will diverge the in_memory_master_record, from the on_disk_master_record
	// protected by global lock (get_wale_lock(wale_p))
	master_record in_memory_master_record;

	// append_offset is the number of bytes that is filled in the buffer
	// the next byte to be appended goes at append_offset
	// protected by global lock (get_wale_lock(wale_p))
	uint64_t append_offset;

	// in_memory_master_record and the append_offset must be accessed only while holding append_only_buffer_lock, either in shared or exclusive mode and the global lock (get_wale_lock(wale_p))
	// This allows us to release the global lock while performing io and then grab the global lock again, while still holding append_only_buffer_lock (doesn't matter shared or exclusive), thus ensuring that the above 2 fields would not have changed
	// Any modifications to append_offset or the in_memory_master_record must trigger the wait_for_scroll, hence any updates to them must happen inside the global mutex lock
	// Such elaborate locking scheme, allows us to release the global mutex lock, while performing read and write io-s

	// this bit will be set, when an unrecoverable scroll error occurs, this error needs a restart of your system
	// which will also mean a loss of certain wal log records
	// protected by global lock
	int major_scroll_error : 1;

	// wait on this condition variable for the next scroll after which append_only_buffer contains the first byte for in_memory_master_record.next_log_sequence_number
	// protected by global lock (get_wale_lock(wale_p))
	pthread_cond_t wait_for_scroll;


	// --------------------------------------------------------
	// functions to perform contiguous block io
	block_io_ops block_io_functions;

	// --------------------------------------------------------

	// no log_sequence_number addition must cross this limit, if it crosses then it is an overflow in all of the system
	uint256 max_limit;
};

/*
** offset of any log record in file = log_sequence_number - first_log_sequence_number + block_io_functions.block_size
*/

// -------------------------------------------------------------
// initialize deinitialize functions for wale

// if next_log_sequence_number is INVALID_LOG_SEQUENCE_NUMBER
//   -> an existing wale file is initialized, on-disk master record is read from disk
// else
//   -> a new wale file is initialized, a brand new master_record is written to disk

int initialize_wale(wale* wale_p, uint32_t log_sequence_number_width, uint256 next_log_sequence_number, pthread_mutex_t* external_lock, block_io_ops block_io_functions, uint64_t append_only_block_count, int* error);

void deinitialize_wale(wale* wale_p);

// -------------------------------------------------------------
// attributes of wale as stored in the on-disk master record

uint32_t get_log_sequence_number_width(wale* wale_p);

uint256 get_first_log_sequence_number(wale* wale_p);

uint256 get_last_flushed_log_sequence_number(wale* wale_p);

uint256 get_check_point_log_sequence_number(wale* wale_p);

uint256 get_next_log_sequence_number(wale* wale_p);

// -------------------------------------------------------------
// random reads in log file are done by the below functions
// the below functions may only be called for log sequence numbers between on-disk first_log_sequence_number and last_flushed_log_sequence_number
// and only while both of which are valid (!= INVALID_LOG_SEQUENCE_NUMBER)
// this implies: you can only read a log_record, if it has been flushed

// An additional parameter skip_flushed_checks has been added (on 17th Nov'2024), if this flag is set it allows you to read unflushed but scrolled log records if they exist in the file
// for all majiority of the operations skip_flushed_checks must be set to 0 for default operation, which is to allow reading only flushed log records

uint256 get_next_log_sequence_number_of(wale* wale_p, uint256 log_sequence_number, int skip_flushed_checks, int* error);

uint256 get_prev_log_sequence_number_of(wale* wale_p, uint256 log_sequence_number, int skip_flushed_checks, int* error);

// you must free the returned memory
void* get_log_record_at(wale* wale_p, uint256 log_sequence_number, uint32_t* log_record_size, int skip_flushed_checks, int* error);

// returns 1 if the log_record is not corrupted and passes all the crc checks (crc32 check for header and log_record itself)
int validate_log_record_at(wale* wale_p, uint256 log_sequence_number, uint32_t* log_record_size, int* error);

// On a failure of any of the above functions, error will be set to anyone of the below
// in the increasing order of severity, we consider data corruption as non-recoverable
#define NO_ERROR                             0
#define PARAM_INVALID                        1 // the passed log_sequence number is invalid
#define ZERO_BUFFER_BLOCK_COUNT              2 // this happens when you issue a append_log_record, flush_all_log_records or truncate_log_records to a wale which has 0 blocks for append only buffer
#define ALLOCATION_FAILED                    3
#define READ_IO_ERROR                        4
#define WRITE_IO_ERROR                       5
#define LOG_SEQUENCE_NUMBER_UNREPRESENTABLE  6 // the log_sequence_numbers on the existing file are too wide to be represented by uint256
#define FILE_OFFSET_OVERFLOW                 7 // appending log record could not succeed, because the file_offset for next log_record will overflow
#define LOG_SEQUENCE_NUMBER_OVERFLOW         8 // appending log record could not succeed, because the next_log_sequence_number will overflow
#define MAJOR_SCROLL_ERROR                   9 // major scroll error occurred while scrolling the appendonly buffer, this is an error you can not recover from, your only alternative is to close the WALe and re open it, and to loose some unflushed log records
#define HEADER_CORRUPTED                    10 // CRC-32 checksum of log header check failed
#define LOG_RECORD_CORRUPTED                11 // CRC-32 checksum of log record check failed
#define MASTER_RECORD_CORRUPTED             12 // CRC-32 checksum of master record check failed, OR the contents of master record are illogical

// -------------------------------------------------------------

// update the number of blocks in the append only buffer at run time
int modify_append_only_buffer_block_count(wale* wale_p, uint64_t buffer_block_count, int* error);

// -------------------------------------------------------------
// writer functions of WALe

// returns the log_sequence_number of the last log record inserted
// check_point is marked to be updated in the master record, if is_check_point is set
// the appended log_record is not permanent (neither is it's being checkpointed-ness) until a flush is successfull
// if the append was unsuccessfull INVALID_LOG_SEQUENCE_NUMBER will be returned, in such a situation it is best to exit the program
uint256 append_log_record(wale* wale_p, const void* log_record, uint32_t log_record_size, int is_check_point, int* error);

// returns the last_flushed_log_sequence_number, after the flush
// it will first ensure that all the appended log records have been flushed and then it will rewrite the master record and flush it
// making it point to the new last_flushed_log_sequence_number, next_log_sequence_number and check_point_log_sequence_number
// if the flush was unsuccessfull INVALID_LOG_SEQUENCE_NUMBER will be returned, in such a situation, it is best to exit the program
uint256 flush_all_log_records(wale* wale_p, int* error);

// the below function scrolls the append only buffer
// this allows reading unflushed log records AND also makes room for more log records in append only buffer
void scroll_append_only_buffer_inside_wale(wale* wale_p, int* error);

// returns the new last_flushed_log_sequence_number, after discarding all the unflushed records
uint256 discard_unflushed_log_records(wale* wale_p, int* error);

// truncates the log file logically, using only a write to the master record
// making first_log_sequence_number, last_flushed_log_sequence_number and check_point_log_sequence_number = 0
// next_log_sequence_number remains as it is, and that will be the next_log_sequence_number that will be alloted
int truncate_log_records(wale* wale_p, int* error);

#endif
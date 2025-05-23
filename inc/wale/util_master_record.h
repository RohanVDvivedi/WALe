#ifndef UTIL_MASTER_RECORD_IO_H
#define UTIL_MASTER_RECORD_IO_H

#include<wale/wale.h>

// no locks are acquired or released by the below functions
// as expected, since it does not even have reference to any of the wale locks

// it operates only with the master record provided, reading, writing and flushing it to the underlying disk using the block_io_functions

// must be called with atleast a read lock on wale_p->flushed_log_records_lock
int read_master_record(master_record* mr, const block_io_ops* block_io_functions, int* error);

// must be called with write lock lock on wale_p->flushed_log_records_lock
int write_and_flush_master_record(const master_record* mr, const block_io_ops* block_io_functions, int* error);

// reads the latest vacant block using the master_record and the block_io_functions into the buffer,
// this is the block where the first byte of the next log record will go
// it returns the file_offset of the next_log_sequence_number
uint64_t read_latest_vacant_block_using_master_record(void* buffer, const master_record* mr, const block_io_ops* block_io_functions, int* error);

// util functions that do not need io, but they work with master_record

// if the log_sequence_number is not between first_log_sequence_number and last_flushed_log_sequence_number OR if first_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER, then a PARAM_INVALID is returned
// else if any operation overflows, then MASTER_RECORD_CORRUPTED error is returned
// = log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;
uint64_t get_file_offset_for_log_sequence_number(uint256 log_sequence_number, const master_record* mr, const block_io_ops* block_io_functions, int skip_flushed_checks, int* error);

// returns MASTER_RECORD_CORRUPTED, if any operation overflows
// if first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER, then return wale_p->block_io_functions.block_size;
// else return log_sequence_number - wale_p->on_disk_master_record.first_log_sequence_number + wale_p->block_io_functions.block_size;
uint64_t get_file_offset_for_next_log_sequence_number(const master_record* mr, const block_io_ops* block_io_functions, int* error);

// returns true if mr1 and mr2 are equal
int are_equal_master_records(const master_record* mr1, const master_record* mr2);

#endif
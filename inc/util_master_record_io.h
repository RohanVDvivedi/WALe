#ifndef UTIL_MASTER_RECORD_IO_H
#define UTIL_MASTER_RECORD_IO_H

#include<wale.h>

// no locks are acquired or released by the below functions
// as expected, since it does not even have reference to any of the wale locks
// it operates only with the master record provided, reading, writing and flushing it to the underlying disk using the block_io_functions

// must be called with atleast a read lock on wale_p->flushed_log_records_lock
int read_master_record(master_record* mr, const block_io_ops* block_io_functions, int* error);

// must be called with write lock lock on wale_p->flushed_log_records_lock
int write_and_flush_master_record(const master_record* mr, const block_io_ops* block_io_functions, int* error);

// reads the latest vacant block using the master_record and the block_io_functions into the buffer,
// this is the block where the first byte of the next log record will go
// it returns the number of bytes read into the buffer, and the block number of this block in block_id
// an expected caller passes wale_p->buffer as buffer and assigns the return value to wale_p->append_offset
uint64_t read_latest_vacant_block_using_master_record(uint64_t* block_id, void* buffer, const master_record* mr, const block_io_ops* block_io_functions, int* error);

#endif
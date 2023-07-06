#ifndef UTIL_MASTER_RECORD_IO_H
#define UTIL_MASTER_RECORD_IO_H

#include<wale.h>

// no locks are acquired or released by the below functions
// as expected, since it does not even have reference to any of the wale locks
// it operated only with the master record provided, reading, writing and flushing it to the underlying disk using the block_io_functions

int read_master_record(master_record* mr, const block_io_ops* block_io_functions);

int write_and_flush_master_record(const master_record* mr, const block_io_ops* block_io_functions);

#endif
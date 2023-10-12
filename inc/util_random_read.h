#ifndef UTIL_RANDOM_READ_H
#define UTIL_RANDOM_READ_H

#include<stdint.h>

#include<block_io_ops.h>

// the below function does not acquire or release any of the wale locks,
// it directly works with the provided buffer, reading appropriate data into it from underlying disk using the block_io_functions

// both of the below functions must be called with atleast a shared lock held on wale_p->flushed_log_records_lock

// returns 1 on a successfull read, else returns 0
int random_read_at(void* buffer, uint64_t buffer_size, uint64_t file_offset, const block_io_ops* block_io_functions);

// returns 1 on a successfull crc32 calculation
// crc32 is an in-out parameter
int crc32_at(uint32_t* crc32, uint64_t data_size, uint64_t file_offset, const block_io_ops* block_io_functions);

#endif
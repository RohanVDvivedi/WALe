#ifndef RANDOM_READ_UTIL_H
#define RANDOM_READ_UTIL_H

#include<stdint.h>

#include<block_io_ops.h>

// returns 1 on a successfull read, else returns 0
int random_read_at(void* buffer, uint32_t buffer_size, uint64_t file_offset, const block_io_ops* block_io_functions);

#endif
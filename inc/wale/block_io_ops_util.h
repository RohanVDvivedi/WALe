#ifndef BLOCK_IO_OPS_UTIL_H
#define BLOCK_IO_OPS_UTIL_H

#include<wale/block_io_ops.h>

uint64_t get_block_id_from_file_offset(uint64_t file_offset, const block_io_ops* block_io_functions);

uint64_t get_block_offset_from_file_offset(uint64_t file_offset, const block_io_ops* block_io_functions);

uint64_t get_file_offset_from_block_id_and_block_offset(uint64_t block_id, uint64_t block_offset, const block_io_ops* block_io_functions, int* error);

#endif
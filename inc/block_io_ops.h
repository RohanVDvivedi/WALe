#ifndef BLOCK_IO_OPS_H
#define BLOCK_IO_OPS_H

#include<stdint.h>

// block_io_ops is a structure accepted by the wale, it is interface that defines how to read, write contiguous blocks to underlying storage
// This structure also instructs the wale about
//  * what block_size buffer to use
//  * alignment requirements of in memory buffer (to hold contiguous blocks and) to perform IO on contiguous blocks

typedef struct block_io_ops block_io_ops;
struct block_io_ops
{
	const void* block_io_ops_handle;

	// block size to be used with this block_io_ops_handle
	uint64_t block_size;

	// alignment requirements of in memeory buffers (src and dest) to perform read_blocks and write_blocks (IO operations)
	uint64_t block_buffer_alignment;

	// all the below functions return 1 on success and 0 on failure

	// read contiguous block_count number of blocks from disk starting at block_id into the memory pointed by dest
	int (*read_blocks)(const void* block_io_ops_handle, void* dest, uint64_t block_id, uint64_t block_count);

	// write contiguous block_count number of blocks to disk starting at block_id from memory pointed by src
	int (*write_blocks)(const void* block_io_ops_handle, const void* src, uint64_t block_id, uint64_t block_count);

	// flush all write to underlying disk, all writes are assumed to be persistent after this call returns successfully
	int (*flush_all_writes)(const void* block_io_ops_handle);
};

#endif
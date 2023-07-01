#ifndef BLOCK_IO_OPS_H
#define BLOCK_IO_OPS_H

// block_io_ops is a structure accepted by the bufferpool, it is interface how to read, write contiguous blocks to underlying storage

typedef struct block_io_ops block_io_ops;
struct block_io_ops
{
	const void* block_io_ops_handle;

	// block size to be used with this block_io_ops_handle
	uint64_t block_size;

	// all the below functions return 1 on success and 0 on failure

	// read contiguous block_count number of blocks from disk starting at block_id into the memory pointed by dest
	int (*read_blocks)(const void* block_io_ops_handle, void* dest, uint64_t block_id, uint64_t block_count);

	// write contiguous block_count number of blocks to disk starting at block_id from memory pointed by src
	int (*write_blocks)(const void* block_io_ops_handle, const void* src, uint64_t block_id, uint64_t block_count);

	// flush all write to underlying disk, all writes are assumed to be persistent after this call returns successfully
	int (*flush_all_writes)(const void* block_io_ops_handle);
};

#endif
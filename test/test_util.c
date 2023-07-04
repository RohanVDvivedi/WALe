#include<block_io.h>

#include<block_io_ops.h>

int read_blocks(const void* block_io_ops_handle, void* dest, uint64_t block_id, uint64_t block_count)
{
	return read_blocks_from_block_file(((const block_file*)block_io_ops_handle), dest, block_id, block_count);
}

int write_blocks(const void* block_io_ops_handle, const void* src, uint64_t block_id, uint64_t block_count)
{
	return write_blocks_to_block_file(((const block_file*)block_io_ops_handle), src, block_id, block_count);
}

int flush_all_writes(const void* block_io_ops_handle)
{
	return flush_all_writes_to_block_file(((const block_file*)block_io_ops_handle));
}

block_io_ops get_block_io_functions(const block_file* bf)
{
	return (block_io_ops){
		.block_io_ops_handle = bf;
		.block_size = get_block_size_for_block_file(bf);
		.read_blocks = read_blocks;
		.write_blocks = write_blocks;
		.flush_all_writes = flush_all_writes;
	};
}
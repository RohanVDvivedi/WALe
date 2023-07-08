#include<util_random_read.h>

#include<cutlery_stds.h>

#include<stdlib.h>

int random_read_at(void* buffer, uint64_t buffer_size, uint64_t file_offset, const block_io_ops* block_io_functions)
{
	if(buffer_size == 0)
		return 1;

	// calculate the end offset
	uint64_t end_offset = file_offset + buffer_size;

	// an end_offset of 0 is valid -> which means last offset is UINT64_MAX
	// if the end offset is lesser than the file_offset, then it is a uint64_t overflow
	if(end_offset != 0 && (end_offset < file_offset || end_offset < buffer_size))
		return 0;

	uint64_t first_block_id = UINT_ALIGN_DOWN(file_offset, block_io_functions->block_size) / block_io_functions->block_size;
	uint64_t end_block_id = UINT_ALIGN_UP(file_offset + buffer_size, block_io_functions->block_size) / block_io_functions->block_size;

	void* blocks_of_concern = aligned_alloc(block_io_functions->block_buffer_alignment, (end_block_id - first_block_id) * block_io_functions->block_size);
	if(blocks_of_concern == NULL)
		return 0;

	if(!block_io_functions->read_blocks(block_io_functions->block_io_ops_handle, blocks_of_concern, first_block_id, end_block_id - first_block_id))
	{
		free(blocks_of_concern);
		return 0;
	}

	memory_move(buffer, blocks_of_concern + (file_offset % block_io_functions->block_size), buffer_size);

	free(blocks_of_concern);
	return 1;
}
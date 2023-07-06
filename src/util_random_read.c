#include<util_random_read.h>

#include<cutlery_stds.h>

#include<stdlib.h>

#define OS_PAGE_SIZE 4096

int random_read_at(void* buffer, uint32_t buffer_size, uint64_t file_offset, const block_io_ops* block_io_functions)
{
	if(buffer_size == 0)
		return 1;

	uint64_t first_block_id = UINT_ALIGN_DOWN(file_offset, block_io_functions->block_size) / block_io_functions->block_size;
	uint64_t end_block_id = UINT_ALIGN_UP(file_offset + buffer_size, block_io_functions->block_size) / block_io_functions->block_size;

	void* blocks_of_concern = aligned_alloc(OS_PAGE_SIZE, (end_block_id - first_block_id) * block_io_functions->block_size);
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
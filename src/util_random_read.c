#include<wale/util_random_read.h>

#include<cutlery/cutlery_stds.h>
#include<cutlery/cutlery_math.h>

#include<stdlib.h>

int random_read_at(void* buffer, uint64_t buffer_size, uint64_t file_offset, const block_io_ops* block_io_functions)
{
	if(buffer_size == 0)
		return 1;

	// make sure that the last offset to be written does not overflow (as we know buffer_size != 0)
	// last_offset to be written = file_offset + (buffer_size - 1)
	if(will_unsigned_sum_overflow(uint64_t, file_offset, (buffer_size - 1)))
		return 0;

	// calculate the end offset
	uint64_t end_offset = file_offset + buffer_size;

	uint64_t first_block_id = UINT_ALIGN_DOWN(file_offset, block_io_functions->block_size) / block_io_functions->block_size;
	uint64_t end_block_id = UINT_ALIGN_UP(end_offset, block_io_functions->block_size) / block_io_functions->block_size;

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

#include<wale/crc32_util.h>

// this function is implemented with minimal memory usage
int crc32_at(uint32_t* crc, uint64_t data_size, uint64_t file_offset, const block_io_ops* block_io_functions)
{
	if(data_size == 0)
	{
		(*crc) = crc32_util((*crc), NULL, 0);
		return 1;
	}

	// make sure that the last offset to be crc-ed does not overflow (as we know buffer_size != 0)
	// last_offset to be crc-ed = file_offset + (buffer_size - 1)
	if(will_unsigned_sum_overflow(uint64_t, file_offset, (data_size - 1)))
		return 0;

	// calculate the end offset
	uint64_t end_offset = file_offset + data_size;

	uint64_t first_block_id = UINT_ALIGN_DOWN(file_offset, block_io_functions->block_size) / block_io_functions->block_size;
	uint64_t end_block_id = UINT_ALIGN_UP(end_offset, block_io_functions->block_size) / block_io_functions->block_size;

	void* block = aligned_alloc(block_io_functions->block_buffer_alignment, block_io_functions->block_size);
	if(block == NULL)
		return 0;

	for(uint64_t block_id = first_block_id; block_id != end_block_id; block_id++)
	{
		if(!block_io_functions->read_blocks(block_io_functions->block_io_ops_handle, block, block_id, 1))
		{
			free(block);
			return 0;
		}

		uint64_t start = max(file_offset, block_id * block_io_functions->block_size);
		uint64_t end = min(end_offset, (block_id + 1) * block_io_functions->block_size);

		(*crc) = crc32_util((*crc), block + (start % block_io_functions->block_size), end - start);
	}

	free(block);
	return 1;
}
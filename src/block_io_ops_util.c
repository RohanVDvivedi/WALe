#include<wale/block_io_ops_util.h>

#include<cutlery/cutlery_stds.h>
#include<cutlery/cutlery_math.h>

#include<wale/wale.h> // only to include errors

uint64_t get_block_id_from_file_offset(uint64_t file_offset, const block_io_ops* block_io_functions)
{
	return UINT_ALIGN_DOWN(file_offset, block_io_functions->block_size) / block_io_functions->block_size;
}

uint64_t get_block_offset_from_file_offset(uint64_t file_offset, const block_io_ops* block_io_functions)
{
	return file_offset % block_io_functions->block_size;
}

uint64_t get_file_offset_from_block_id_and_block_offset(uint64_t block_id, uint64_t block_offset, const block_io_ops* block_io_functions, int* error)
{
	if(will_unsigned_mul_overflow(uint64_t, block_id, block_io_functions->block_size) ||
		will_unsigned_sum_overflow(uint64_t, (block_id * block_io_functions->block_size), block_offset))
	{
		(*error) = FILE_OFFSET_OVERFLOW;
		return 0;
	}

	return (block_id * block_io_functions->block_size) + block_offset;
}
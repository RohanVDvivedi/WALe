#include<block_io_ops_util.h>

#include<cutlery_stds.h>

uint64_t get_block_id_from_file_offset(uint64_t file_offset, const block_io_ops* block_io_functions)
{
	return UINT_ALIGN_DOWN(file_offset, block_io_functions->block_size) / block_io_functions->block_size;
}

uint64_t get_block_offset_from_file_offset(uint64_t file_offset, const block_io_ops* block_io_functions)
{
	return file_offset % block_io_functions->block_size;
}
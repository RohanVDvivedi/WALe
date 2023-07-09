#include<util_scroll_append_only_buffer.h>

#include<wale_get_lock_util.h>

#include<cutlery_stds.h>

int scroll_append_only_buffer(wale* wale_p)
{
	uint64_t block_count_to_write = UINT_ALIGN_UP(wale_p->append_offset, wale_p->block_io_functions.block_size) / wale_p->block_io_functions.block_size;
	if(block_count_to_write == 0)
		return 1;

	// unlock the global lock while performing a write syscall
	pthread_mutex_unlock(get_wale_lock(wale_p));

	// write the current contents of the append only buffer to disk at its start offset
	if(!wale_p->block_io_functions.write_blocks(wale_p->block_io_functions.block_io_ops_handle, wale_p->buffer, wale_p->buffer_start_block_id, block_count_to_write))
	{
		pthread_mutex_lock(get_wale_lock(wale_p));
		return 0;
	}

	pthread_mutex_lock(get_wale_lock(wale_p));

	// perform the actual scrolling here
	uint64_t new_buffer_start_block_id = wale_p->buffer_start_block_id + UINT_ALIGN_DOWN(wale_p->append_offset, wale_p->block_io_functions.block_size) / wale_p->block_io_functions.block_size;
	uint64_t new_append_offset = wale_p->append_offset % wale_p->block_io_functions.block_size;

	memory_move(wale_p->buffer, wale_p->buffer + UINT_ALIGN_DOWN(wale_p->append_offset, wale_p->block_io_functions.block_size), new_append_offset);

	wale_p->buffer_start_block_id = new_buffer_start_block_id;
	wale_p->append_offset = new_append_offset;

	return 1;
}

int is_file_offset_within_append_only_buffer(wale* wale_p, uint64_t file_offset)
{
	return ((wale_p->buffer_start_block_id * wale_p->block_io_functions.block_size) <= file_offset && file_offset < ((wale_p->buffer_start_block_id + wale_p->buffer_block_count) * wale_p->block_io_functions.block_size));
}
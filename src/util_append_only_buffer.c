#include<util_append_only_buffer.h>

#include<wale_get_lock_util.h>
#include<util_master_record.h>
#include<block_io_ops_util.h>

#include<cutlery_stds.h>

#include<stdlib.h>

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

int resize_append_only_buffer(wale* wale_p, uint64_t new_buffer_block_count, int* error)
{
	(*error) = NO_ERROR;

	if(wale_p->buffer_block_count == new_buffer_block_count)
		return 1;

	// this is all if we want to make the buffer read only
	if(new_buffer_block_count == 0)
	{
		wale_p->buffer_block_count = 0;
		free(wale_p->buffer);
		wale_p->buffer = NULL;
		return 1;
	}

	uint64_t old_buffer_block_count = wale_p->buffer_block_count;

	void* new_buffer = aligned_alloc(wale_p->block_io_functions.block_buffer_alignment, (new_buffer_block_count * wale_p->block_io_functions.block_size));
	memory_set(new_buffer, 0, (new_buffer_block_count * wale_p->block_io_functions.block_size));

	// failed memory allocation
	if(new_buffer == NULL)
	{
		(*error) = ALLOCATION_FAILED;
		return 0;
	}

	if(old_buffer_block_count != 0)
	{
		// if the current content size of the append only buffer is greater than what the new_buffer can accomodate
		// then scroll the append only buffer
		if(wale_p->append_offset > new_buffer_block_count * wale_p->block_io_functions.block_size)
		{
			// scroll and replace
			int scroll_error = !scroll_append_only_buffer(wale_p);

			if(scroll_error)
			{
				(*error) = MAJOR_SCROLL_ERROR;
				free(new_buffer);
				return 0;
			}
		}

		memory_move(new_buffer, wale_p->buffer, wale_p->append_offset);

		free(wale_p->buffer);
		wale_p->buffer = new_buffer;
		wale_p->buffer_block_count = new_buffer_block_count;

		return 1;
	}
	else // this implies that the buffer_block_count was previously 0, hence to stay updated we need to read contents from the on_disk_master_record
	{
		read_lock(&(wale_p->flushed_log_records_lock), READ_PREFERRING, BLOCKING);

		master_record new_in_memory_master_record = wale_p->on_disk_master_record;

		read_unlock(&(wale_p->flushed_log_records_lock));

		// we can unlock the global lock here while we perform the read of a single block from disk
		pthread_mutex_unlock(get_wale_lock(wale_p));

		uint64_t file_offset_for_next_log_sequence_number = read_latest_vacant_block_using_master_record(new_buffer, &new_in_memory_master_record, &(wale_p->block_io_functions), error);

		pthread_mutex_lock(get_wale_lock(wale_p));

		if(*error)
		{
			free(new_buffer);
			return 0;
		}

		wale_p->in_memory_master_record = new_in_memory_master_record;
		wale_p->buffer_start_block_id = get_block_id_from_file_offset(file_offset_for_next_log_sequence_number, &(wale_p->block_io_functions));
		wale_p->append_offset = get_block_offset_from_file_offset(file_offset_for_next_log_sequence_number, &(wale_p->block_io_functions));
		wale_p->buffer = new_buffer;
		wale_p->buffer_block_count = new_buffer_block_count;

		return 1;
	}
}
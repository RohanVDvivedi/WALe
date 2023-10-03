#include<wale.h>

#include<wale_get_lock_util.h>
#include<util_master_record_io.h>

#include<stdlib.h>

#include<cutlery_stds.h>

int initialize_wale(wale* wale_p, uint32_t log_sequence_number_width, log_seq_nr next_log_sequence_number, pthread_mutex_t* external_lock, block_io_ops block_io_functions, uint64_t append_only_block_count)
{
	wale_p->has_internal_lock = (external_lock == NULL);

	if(wale_p->has_internal_lock)
		pthread_mutex_init(&(wale_p->internal_lock), NULL);
	else
		wale_p->external_lock = external_lock;

	wale_p->block_io_functions = block_io_functions;

	if(compare_log_seq_nr(next_log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER) == 0)
	{
		if(!read_master_record(&(wale_p->on_disk_master_record), &(wale_p->block_io_functions)))
			return 0;
	}
	else
	{
		// log_sequence_number width must be in range [1, LOG_SEQ_NR_MAX_BYTES], both inclusive
		if(log_sequence_number_width == 0 || log_sequence_number_width > LOG_SEQ_NR_MAX_BYTES)
			return 0;

		wale_p->on_disk_master_record.log_sequence_number_width = log_sequence_number_width;
		wale_p->on_disk_master_record.first_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.check_point_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.next_log_sequence_number = next_log_sequence_number;

		if(!write_and_flush_master_record(&(wale_p->on_disk_master_record), &(wale_p->block_io_functions)))
			return 0;
	}

	wale_p->in_memory_master_record = wale_p->on_disk_master_record;

	wale_p->buffer = aligned_alloc(wale_p->block_io_functions.block_buffer_alignment, (append_only_block_count * wale_p->block_io_functions.block_size));
	wale_p->buffer_block_count = append_only_block_count;

	if(wale_p->buffer == NULL)
		return 0;

	// there are no log records on the disk, if the first_log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER
	if(compare_log_seq_nr(wale_p->in_memory_master_record.first_log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER) == 0)
	{
		wale_p->append_offset = 0;
		wale_p->buffer_start_block_id = 1;
	}
	else // else read appropriate first block from memory
	{
		// calculate file_offset to start appending from
		uint64_t file_offset_to_append_from;
		{
			log_seq_nr temp; // = next_log_sequence_number - first_log_sequence_number + block_size
			if(	(!sub_log_seq_nr(&temp, wale_p->in_memory_master_record.next_log_sequence_number, wale_p->in_memory_master_record.first_log_sequence_number)) ||
				(!add_log_seq_nr(&temp, temp, get_log_seq_nr(wale_p->block_io_functions.block_size), LOG_SEQ_NR_MIN)) ||
				(!cast_to_uint64(&file_offset_to_append_from, temp)) )
			{
				// this implies master record is corrupted
				free(wale_p->buffer);
				return 0;
			}
		}

		wale_p->append_offset = file_offset_to_append_from % wale_p->block_io_functions.block_size;
		wale_p->buffer_start_block_id = UINT_ALIGN_DOWN(file_offset_to_append_from, wale_p->block_io_functions.block_size) / wale_p->block_io_functions.block_size;

		if(wale_p->append_offset)
		{
			if(!wale_p->block_io_functions.read_blocks(wale_p->block_io_functions.block_io_ops_handle, wale_p->buffer, wale_p->buffer_start_block_id, 1))
			{
				free(wale_p->buffer);
				return 0;
			}
		}
	}

	pthread_cond_init(&(wale_p->wait_for_scroll), NULL);
	initialize_rwlock(&(wale_p->flushed_log_records_lock), get_wale_lock(wale_p));
	initialize_rwlock(&(wale_p->append_only_buffer_lock), get_wale_lock(wale_p));

	return 1;
}

void deinitialize_wale(wale* wale_p)
{
	free(wale_p->buffer);

	if(wale_p->has_internal_lock)
		pthread_mutex_destroy(&(wale_p->internal_lock));

	pthread_cond_destroy(&(wale_p->wait_for_scroll));
	deinitialize_rwlock(&(wale_p->flushed_log_records_lock));
	deinitialize_rwlock(&(wale_p->append_only_buffer_lock));
}
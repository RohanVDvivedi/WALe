#include<wale.h>

#include<wale_get_lock_util.h>
#include<util_master_record.h>
#include<block_io_ops_util.h>

#include<stdlib.h>

#include<cutlery_stds.h>

int initialize_wale(wale* wale_p, uint32_t log_sequence_number_width, uint256 next_log_sequence_number, pthread_mutex_t* external_lock, block_io_ops block_io_functions, uint64_t append_only_block_count, int* error)
{
	wale_p->has_internal_lock = (external_lock == NULL);

	if(wale_p->has_internal_lock)
		pthread_mutex_init(&(wale_p->internal_lock), NULL);
	else
		wale_p->external_lock = external_lock;

	wale_p->block_io_functions = block_io_functions;

	if(are_equal_uint256(next_log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER))
	{
		if(!read_master_record(&(wale_p->on_disk_master_record), &(wale_p->block_io_functions), error))
			return 0;
	}
	else
	{
		// log_sequence_number width must be in range [1, LOG_SEQ_NR_MAX_BYTES], both inclusive
		if(log_sequence_number_width == 0 || log_sequence_number_width > get_max_bytes_uint256())
		{
			(*error) = PARAM_INVALID;
			return 0;
		}

		wale_p->on_disk_master_record.log_sequence_number_width = log_sequence_number_width;
		wale_p->on_disk_master_record.first_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.last_flushed_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.check_point_log_sequence_number = INVALID_LOG_SEQUENCE_NUMBER;
		wale_p->on_disk_master_record.next_log_sequence_number = next_log_sequence_number;

		if(!write_and_flush_master_record(&(wale_p->on_disk_master_record), &(wale_p->block_io_functions), error))
			return 0;
	}

	wale_p->in_memory_master_record = wale_p->on_disk_master_record;

	pthread_cond_init(&(wale_p->wait_for_scroll), NULL);
	initialize_rwlock(&(wale_p->flushed_log_records_lock), get_wale_lock(wale_p));
	initialize_rwlock(&(wale_p->append_only_buffer_lock), get_wale_lock(wale_p));

	wale_p->max_limit = get_0_uint256();
	set_bit_in_uint256(&(wale_p->max_limit), wale_p->in_memory_master_record.log_sequence_number_width * CHAR_BIT);

	if(append_only_block_count == 0) // WALe is opened only for reading
	{
		wale_p->buffer = NULL;
		wale_p->buffer_block_count = 0;
	}
	else
	{
		wale_p->buffer = aligned_alloc(wale_p->block_io_functions.block_buffer_alignment, (append_only_block_count * wale_p->block_io_functions.block_size));
		wale_p->buffer_block_count = append_only_block_count;

		if(wale_p->buffer == NULL)
		{
			(*error) = ALLOCATION_FAILED;
			return 0;
		}

		uint64_t file_offset_for_next_log_sequence_number = read_latest_vacant_block_using_master_record(wale_p->buffer, &(wale_p->in_memory_master_record), &(wale_p->block_io_functions), error);
		if(*error)
		{
			free(wale_p->buffer);
			return 0;
		}

		wale_p->buffer_start_block_id = get_block_id_from_file_offset(file_offset_for_next_log_sequence_number, &(wale_p->block_io_functions));
		wale_p->append_offset = get_block_offset_from_file_offset(file_offset_for_next_log_sequence_number, &(wale_p->block_io_functions));
	}

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
#include<wale.h>

#include<wale_get_lock_util.h>

uint64_t get_first_log_sequence_number(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	uint64_t first_log_sequence_number = wale_p->on_disk_master_record.first_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count++;

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return first_log_sequence_number;
}

uint64_t get_last_flushed_log_sequence_number(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	uint64_t last_flushed_log_sequence_number = wale_p->on_disk_master_record.last_flushed_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count++;

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return last_flushed_log_sequence_number;
}

uint64_t get_check_point_log_sequence_number(wale* wale_p)
{
	if(wale_p->has_internal_lock)
		pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count++;

	pthread_mutex_unlock(get_wale_lock(wale_p));

	uint64_t check_point_log_sequence_number = wale_p->on_disk_master_record.check_point_log_sequence_number;

	pthread_mutex_lock(get_wale_lock(wale_p));

	wale_p->random_readers_count++;

	if(wale_p->has_internal_lock)
		pthread_mutex_unlock(get_wale_lock(wale_p));

	return check_point_log_sequence_number;
}

uint64_t get_next_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number);

uint64_t get_prev_log_sequence_number_of(wale* wale_p, uint64_t log_sequence_number);

void* get_log_record_at(wale* wale_p, uint64_t log_sequence_number, uint32_t* log_record_size);

uint64_t append_log_record(wale* wale_p, const void* log_record, uint32_t log_record_size, int is_check_point);

uint64_t flush_all_log_records(wale* wale_p);

int truncate_log_records(wale* wale_p);
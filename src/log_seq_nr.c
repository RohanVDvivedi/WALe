#include<log_seq_nr.h>

int compare_log_seg_nr(log_seq_nr a, log_seq_nr b);

uint64_t add_log_seq_nr_overflow_unsafe(log_seq_nr* res, log_seq_nr a, log_seq_nr b);

int add_log_seq_nr(log_seq_nr* res, log_seq_nr a, log_seq_nr b, log_seq_nr max_limit);

uint64_t sub_log_seq_nr_underflow_unsafe(log_seq_nr* res, log_seq_nr a, log_seq_nr b);

int sub_log_seq_nr(log_seq_nr* res, log_seq_nr a, log_seq_nr b);

int set_bit_in_log_seq_nr(log_seq_nr* res, uint32_t bit_index);

void serialize_log_seq_nr(void* bytes, uint32_t bytes_size, log_seq_nr l);

log_seq_nr deserialize_le_uint64(const char* bytes, uint32_t bytes_size);

void print_log_seq_nr(log_seq_nr l);
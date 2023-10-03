#ifndef LOG_SEQ_NR_H
#define LOG_SEQ_NR_H

#include<inttypes.h>

#include<cutlery_stds.h>

// type of each digit in log_seg_nr
#define limb_type uint64_t

// this can be any value, but 2 is just right for all practical purposes
// if you modify this, be sure to modify LOG_SEQ_NR_MAX
#define LOG_SEQ_NR_LIMBS_COUNT 2

#define LOG_SEQ_NR_MAX_BYTES   (LOG_SEQ_NR_LIMBS_COUNT * sizeof(limb_type))

typedef struct log_seq_nr log_seq_nr;
struct log_seq_nr
{
	limb_type limbs[LOG_SEQ_NR_LIMBS_COUNT];
};

#define get_log_seq_nr(val) ((log_seq_nr){.limbs = {val}})

#define LOG_SEQ_NR_MIN ((log_seq_nr){.limbs = {}})
#define LOG_SEQ_NR_MAX ((log_seq_nr){.limbs = {UNSIGNED_MAX_VALUE_OF(limb_type), UNSIGNED_MAX_VALUE_OF(limb_type)}})

int compare_log_seg_nr(log_seq_nr a, log_seq_nr b);

// adds 2 log_seq_nr-s, does not check for overflow, returns carry bit
uint64_t add_log_seq_nr_overflow_unsafe(log_seq_nr* res, log_seq_nr a, log_seq_nr b);

// res will be set with addition (a + b), on success (i.e. return 1)
// failure happens in case of an overflow OR if the result is greater than or equal to max_limit (max_limit is checked only if it is non zero)
int add_log_seq_nr(log_seq_nr* res, log_seq_nr a, log_seq_nr b, log_seq_nr max_limit);

// subtracts 2 log_seq_nr-s, does not check for underflow, returns carry bit
uint64_t sub_log_seq_nr_underflow_unsafe(log_seq_nr* res, log_seq_nr a, log_seq_nr b);

// res will be set with subtraction, on success (i.e. return 1)
// failure happens in case of an underflow, when (a < b)
int sub_log_seq_nr(log_seq_nr* res, log_seq_nr a, log_seq_nr b);

// set an explicit bit in log_seq_nr
int set_bit_in_log_seq_nr(log_seq_nr* res, uint32_t bit_index);

// serialize and deserialize log_seq_nr-s
void serialize_log_seq_nr(void* bytes, uint32_t bytes_size, log_seq_nr l);
log_seq_nr deserialize_log_seq_nr(const char* bytes, uint32_t bytes_size);

// print log_seq_nr
void print_log_seq_nr(log_seq_nr l);

#endif
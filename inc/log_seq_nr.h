#ifndef LOG_SEQ_NR_H
#define LOG_SEQ_NR_H

#include<inttypes.h>

#include<cutlery_stds.h>

// this can be any value, but 2 is just right for all practical purposes
// if you modify this, be sure to modify LOG_SEQ_NR_MAX
#define LOG_SEQ_NR_LIMBS_COUNT 2

#define LOG_SEQ_NR_MAX_BYTES   (LOG_SEQ_NR_LIMBS_COUNT * sizeof(uint64_t))

typedef struct log_seq_nr log_seq_nr;
struct log_seq_nr
{
	uint64_t limbs[LOG_SEQ_NR_LIMBS_COUNT];
};

#define get_log_seq_nr(val) ((log_seq_nr){.limbs = {val}})

#define LOG_SEQ_NR_MIN ((log_seq_nr){.limbs = {}})
#define LOG_SEQ_NR_MAX ((log_seq_nr){.limbs = {UNSIGNED_MAX_VALUE_OF(uint64_t), UNSIGNED_MAX_VALUE_OF(uint64_t)}})

int compare_log_seq_nr(log_seq_nr a, log_seq_nr b);

// check if two log_seq_nr-s are equal
int are_equal_log_seq_nr(log_seq_nr a, log_seq_nr b);

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

// returns true, if the given log_seq_nr, can fit on a single uint64_t, the value will be set with the value of a
// else a 0 (false) is returned, with value unset
int cast_to_uint64(uint64_t* value, log_seq_nr a);

// serialize and deserialize log_seq_nr-s
void serialize_log_seq_nr(void* bytes, uint32_t bytes_size, log_seq_nr l);
log_seq_nr deserialize_log_seq_nr(const char* bytes, uint32_t bytes_size);

// print log_seq_nr
void print_log_seq_nr(log_seq_nr l);

#endif
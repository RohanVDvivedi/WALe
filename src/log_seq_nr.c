#include<log_seq_nr.h>

#include<cutlery_math.h>

int compare_log_seg_nr(log_seq_nr a, log_seq_nr b)
{
	int res = 0;
	for(uint32_t i = LOG_SEQ_NR_LIMBS_COUNT; i > 0 && res == 0;)
	{
		i--;
		res = compare_numbers(a.limbs[i], b.limbs[i]);
	}
	return res;
}

// carry_in must be 0 or 1 only
static uint64_t add_log_seq_nr_overflow_unsafe_with_carry(log_seq_nr* res, log_seq_nr a, log_seq_nr b, uint64_t carry)
{
	carry = !!carry;
	for(uint32_t i = 0; i < LOG_SEQ_NR_LIMBS_COUNT; i++)
	{
		uint64_t carry_in = carry;
		res->limbs[i] = a.limbs[i] + b.limbs[i] + carry_in;
		carry = will_unsigned_sum_overflow(uint64_t, a.limbs[i], b.limbs[i]) || will_unsigned_sum_overflow(uint64_t, a.limbs[i] + b.limbs[i], carry_in);
	}
	return carry;
}

uint64_t add_log_seq_nr_overflow_unsafe(log_seq_nr* res, log_seq_nr a, log_seq_nr b)
{
	return add_log_seq_nr_overflow_unsafe_with_carry(res, a, b, 0);
}

int add_log_seq_nr(log_seq_nr* res, log_seq_nr a, log_seq_nr b, log_seq_nr max_limit)
{
	log_seq_nr res_temp;
	if(add_log_seq_nr_overflow_unsafe(&res_temp, a, b)) // carry out implies overflow
		return 0;

	// if max_limit is not 0, i.e. max_limit exists, and res_temp >= max_limit, then fail
	if(compare_log_seg_nr(max_limit, LOG_SEQ_NR_MIN) != 0 && compare_log_seg_nr(res_temp, max_limit) >= 0)
		return 0;

	(*res) = res_temp;
	return 1;
}

static log_seq_nr bitwise_not(log_seq_nr a)
{
	log_seq_nr res;
	for(uint32_t i = 0; i < LOG_SEQ_NR_LIMBS_COUNT; i++)
		res.limbs[i] = ~a.limbs[i];
	return res;
}

uint64_t sub_log_seq_nr_underflow_unsafe(log_seq_nr* res, log_seq_nr a, log_seq_nr b)
{
	log_seq_nr not_b = bitwise_not(b);
	return add_log_seq_nr_overflow_unsafe_with_carry(res, a, not_b, 1);
}

int sub_log_seq_nr(log_seq_nr* res, log_seq_nr a, log_seq_nr b)
{
	// can not subtract if a < b
	if(compare_log_seg_nr(a, b) < 0)
		return 0;

	sub_log_seq_nr_underflow_unsafe(res, a, b);
	return 1;
}

int set_bit_in_log_seq_nr(log_seq_nr* res, uint32_t bit_index)
{
	if(bit_index >= LOG_SEQ_NR_MAX_BYTES * CHAR_BIT)
		return 0;
	res->limbs[bit_index / (sizeof(uint64_t) * CHAR_BIT)] |= (((uint64_t)1) << (bit_index % (sizeof(uint64_t) * CHAR_BIT)));
	return 1;
}

#include<storage_byte_ordering.h>

void serialize_log_seq_nr(void* bytes, uint32_t bytes_size, log_seq_nr l)
{
	bytes_size = min(bytes_size, LOG_SEQ_NR_MAX_BYTES);

	uint32_t limb_index = 0;
	while(bytes_size > 0)
	{
		uint32_t bytes_to_write = min(sizeof(uint64_t), bytes_size);

		serialize_le_uint64(bytes, bytes_to_write, l.limbs[limb_index++]);

		bytes_size -= bytes_to_write;
		bytes += bytes_to_write;
	}
}

log_seq_nr deserialize_log_seq_nr(const char* bytes, uint32_t bytes_size)
{
	log_seq_nr res = {};

	bytes_size = min(bytes_size, LOG_SEQ_NR_MAX_BYTES);

	uint32_t limb_index = 0;
	while(bytes_size > 0)
	{
		uint32_t bytes_to_read = min(sizeof(uint64_t), bytes_size);

		res.limbs[limb_index++] = deserialize_le_uint64(bytes, bytes_to_read);

		bytes_size -= bytes_to_read;
		bytes += bytes_to_read;
	}

	return res;
}

int cast_to_uint64(uint64_t* value, log_seq_nr a)
{
	uint32_t limbs_required = LOG_SEQ_NR_LIMBS_COUNT;
	while(limbs_required > 0 && a.limbs[limbs_required-1] == 0)
		limbs_required--;
	
	if(limbs_required <= 1)
	{
		(*value) = a.limbs[0];
		return 1;
	}

	return 0;
}

#include<stdio.h>

void print_log_seq_nr(log_seq_nr l)
{
	for(uint32_t i = LOG_SEQ_NR_LIMBS_COUNT; i > 0;)
	{
		i--;
		printf("%llu", (unsigned long long int)(l.limbs[i]));
		if(i > 0)
			printf(":");
	}
}
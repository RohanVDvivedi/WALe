#include<stdio.h>
#include<stdlib.h>

#include<blockio/block_io.h>

#include<wale/wale.h>

#include<string.h>
#include<errno.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#include<prwrite_specs.h>

#define LOG_FORMAT "thread_id=<%d> log_number=<%d>"

block_io_ops get_block_io_functions(const block_file* bf);

wale walE;

int main()
{
	int new_file = 0;

	block_file bf;
	if(!open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS))
	{
		printf("failed to create block file\n");
		return -1;
	}

	int init_error = 0;
	if(!initialize_wale(&walE, 0, INVALID_LOG_SEQUENCE_NUMBER, NULL, get_block_io_functions(&bf), 0, &init_error))
	{
		printf("failed to create wale instance wale_erro = %d (error = %d on fd = %d)\n", init_error, errno, bf.file_descriptor);
		close_block_file(&bf);
		return -1;
	}

	int next_log_to_see[THREAD_COUNT] = {};

	uint256 log_sequence_number = get_first_log_sequence_number(&walE);
	int error = 0;
	while(compare_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER) != 0)
	{
		uint32_t log_record_size;
		char* log_record = (char*) get_log_record_at(&walE, log_sequence_number, &log_record_size, 0, &error);
		if(error != NO_ERROR && error != PARAM_INVALID)
		{
			printf("error = %d\n", error);
			exit(-1);
		}
		int thread_id = -1;
		int log_number = -1;
		sscanf(log_record, LOG_FORMAT, &thread_id, &log_number);
		if(thread_id < THREAD_COUNT && log_number < LOGS_PER_THREAD && next_log_to_see[thread_id] == log_number)
			next_log_to_see[thread_id]++;
		else
		{
			printf("error at log_sequence_number = "); print_uint256(log_sequence_number); printf(" seen -> thread_id=%d log_number=%d\n", thread_id, log_number);
			exit(-1);
		}
		log_sequence_number = get_next_log_sequence_number_of(&walE, log_sequence_number, 0, &error);
		if(error != NO_ERROR && error != PARAM_INVALID)
		{
			printf("error = %d\n", error);
			exit(-1);
		}
	}

	for(int i = 0; i < THREAD_COUNT; i++)
	{
		if(next_log_to_see[i] != LOGS_PER_THREAD)
		{
			printf("error we saw only %d logs for thread_id %d\n", next_log_to_see[i], i);
			exit(-1);
		}
	}

	printf("no error found - prwrite test cases were successfull\n");

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
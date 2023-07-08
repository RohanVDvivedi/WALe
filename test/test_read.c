#include<stdio.h>
#include<stdlib.h>

#include<block_io.h>

#include<wale.h>

#include<string.h>
#include<errno.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#define APPEND_ONLY_BUFFER_COUNT 1

block_io_ops get_block_io_functions(const block_file* bf);

wale walE;

void print_all_flushed_logs()
{
	int error = 0;
	uint64_t log_sequence_number = get_first_log_sequence_number(&walE);
	printf("first_log_sequence_numbers = %"PRIu64"\n", log_sequence_number);
	printf("check_point_log_sequence_numbers = %"PRIu64"\n", get_check_point_log_sequence_number(&walE));
	while(log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER)
	{
		uint32_t log_record_size;
		char* log_record = (char*) get_log_record_at(&walE, log_sequence_number, &log_record_size, &error);
		if(error)
			printf("error = %d\n", error);
		uint64_t next_log_sequence_number = get_next_log_sequence_number_of(&walE, log_sequence_number, &error);
		if(error)
			printf("error = %d\n", error);
		uint64_t prev_log_sequence_number = get_prev_log_sequence_number_of(&walE, log_sequence_number, &error);
		if(error)
			printf("error = %d\n", error);
		printf("(log_sequence_number=%"PRIu64") (prev=%"PRIu64") (next=%"PRIu64") (size = %u): <%s>\n", log_sequence_number, prev_log_sequence_number, next_log_sequence_number, log_record_size, log_record);
		free(log_record);
		log_sequence_number = next_log_sequence_number;
	}
	printf("last_flushed_log_sequence_numbers = %"PRIu64"\n\n", get_last_flushed_log_sequence_number(&walE));
}

int main()
{
	int new_file = 0;

	block_file bf;
	if(!open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS))
	{
		printf("failed to create block file\n");
		return -1;
	}

	if(!initialize_wale(&walE, INVALID_LOG_SEQUENCE_NUMBER, NULL, get_block_io_functions(&bf), APPEND_ONLY_BUFFER_COUNT))
	{
		printf("failed to create wale instance (error = %d on fd = %d)\n", errno, bf.file_descriptor);
		close_block_file(&bf);
		return -1;
	}

	print_all_flushed_logs();

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
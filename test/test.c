#include<stdio.h>
#include<stdlib.h>

#include<block_io.h>

#include<wale.h>

#include<string.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#define APPEND_ONLY_BUFFER_COUNT 1


#define NUMBERS "0123456789-10111213141516171819-20212223242526272829"
#define LOG_FORMAT "\nThis is log with <%*.s> random number = <%d>\n"

block_io_ops get_block_io_functions(const block_file* bf);

char log_buffer[4096];

#define APPENDS_TO_PERFORM 512

uint64_t log_sequences_numbers_count = 0;
uint64_t log_sequences_numbers[APPENDS_TO_PERFORM];

int main()
{
	int new_file = 0;

	block_file bf;
	if(!(new_file = create_and_open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS)) && !open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS | O_TRUNC))
	{
		printf("failed to create block file\n");
		return -1;
	}

	wale walE;
	if(!initialize_wale(&walE, (new_file ? 7 : INVALID_LOG_SEQUENCE_NUMBER), NULL, get_block_io_functions(&bf), APPEND_ONLY_BUFFER_COUNT))
	{
		printf("failed to create wale instance\n");
		close_block_file(&bf);
		return -1;
	}

	sprintf(log_buffer, LOG_FORMAT, (((unsigned int)rand()) % 52), NUMBERS, rand());
	log_sequences_numbers[log_sequences_numbers_count++] = append_log_record(&walE, log_buffer, strlen(log_buffer) + 1, 0);
	printf("log sequence number written = %" PRIu64 " : %s\n", log_sequences_numbers[log_sequences_numbers_count-1], log_buffer);

	sprintf(log_buffer, LOG_FORMAT, (((unsigned int)rand()) % 52), NUMBERS, rand());
	log_sequences_numbers[log_sequences_numbers_count++] = append_log_record(&walE, log_buffer, strlen(log_buffer) + 1, 0);
	printf("log sequence number written = %" PRIu64 " : %s\n", log_sequences_numbers[log_sequences_numbers_count-1], log_buffer);

	printf("flushed until = %" PRIu64 "\n", flush_all_log_records(&walE));

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
#include<stdio.h>
#include<stdlib.h>

#include<block_io.h>

#include<wale.h>

#include<string.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#define APPEND_ONLY_BUFFER_COUNT 1


#define NUMBERS "0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859()0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859"
#define LOG_FORMAT "\nThis is log with size = <%d> <%.*s>\n"

block_io_ops get_block_io_functions(const block_file* bf);

#define APPENDS_TO_PERFORM 512

wale walE;

void append_test_log()
{
	char log_buffer[4096];
	uint32_t ls = (((unsigned int)rand()) % strlen(NUMBERS));
	sprintf(log_buffer, LOG_FORMAT, ls, ls, NUMBERS);
	uint64_t log_sequence_number = append_log_record(&walE, log_buffer, strlen(log_buffer) + 1, 0);
	printf("log sequence number written = %" PRIu64 " : %s\n", log_sequence_number, log_buffer);
}

void print_all_flushed_logs()
{
	uint64_t log_sequence_number = get_first_log_sequence_number(&walE);
	printf("first_log_sequence_numbers = %"PRIu64"\n", log_sequence_number);
	printf("check_point_log_sequence_numbers = %"PRIu64"\n", get_check_point_log_sequence_number(&walE));
	while(log_sequence_number != INVALID_LOG_SEQUENCE_NUMBER)
	{
		uint32_t log_record_size;
		char* log_record = (char*) get_log_record_at(&walE, log_sequence_number, &log_record_size);
		printf("%" PRIu64 " : <%s>\n", log_sequence_number, log_record);
		free(log_record);
		log_sequence_number = get_next_log_sequence_number_of(&walE, log_sequence_number);
	}
	printf("last_flushed_log_sequence_numbers = %"PRIu64"\n\n", get_last_flushed_log_sequence_number(&walE));
}

int main()
{
	int new_file = 0;

	block_file bf;
	if(!(new_file = create_and_open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS)) && !open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS | O_TRUNC))
	{
		printf("failed to create block file\n");
		return -1;
	}

	if(!initialize_wale(&walE, (new_file ? 7 : INVALID_LOG_SEQUENCE_NUMBER), NULL, get_block_io_functions(&bf), APPEND_ONLY_BUFFER_COUNT))
	{
		printf("failed to create wale instance\n");
		close_block_file(&bf);
		return -1;
	}

	append_test_log();
	append_test_log();

	printf("flushed until = %" PRIu64 "\n\n", flush_all_log_records(&walE));

	append_test_log();

	print_all_flushed_logs();

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
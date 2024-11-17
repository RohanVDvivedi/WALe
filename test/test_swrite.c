#include<stdio.h>
#include<stdlib.h>

#include<block_io.h>

#include<wale.h>

#include<string.h>
#include<errno.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#define APPEND_ONLY_BUFFER_COUNT 1


#define NUMBERS "0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859()0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859"
#define LOG_FORMAT "This is log with size = <%d> <%.*s>"

block_io_ops get_block_io_functions(const block_file* bf);

wale walE;

void append_test_log()
{
	char log_buffer[4096];
	uint32_t ls = (((unsigned int)rand()) % strlen(NUMBERS));
	sprintf(log_buffer, LOG_FORMAT, ls, ls, NUMBERS);
	int error = 0;
	uint256 log_sequence_number = append_log_record(&walE, log_buffer, strlen(log_buffer) + 1, 0, &error);
	printf("log sequence number written = "); print_uint256(log_sequence_number); printf(" : %s : error -> %d\n\n", log_buffer, error);
}

void print_all_flushed_logs()
{
	int error = 0;
	uint256 log_sequence_number = get_first_log_sequence_number(&walE);
	printf("first_log_sequence_numbers = "); print_uint256(log_sequence_number); printf("\n");
	printf("check_point_log_sequence_numbers = "); print_uint256(get_check_point_log_sequence_number(&walE)); printf("\n");
	while(compare_uint256(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER) != 0)
	{
		uint32_t log_record_size;
		char* log_record = (char*) get_log_record_at(&walE, log_sequence_number, &log_record_size, 0, &error);
		if(error)
			printf("error = %d\n", error);
		uint256 next_log_sequence_number = get_next_log_sequence_number_of(&walE, log_sequence_number, 0, &error);
		if(error)
			printf("error = %d\n", error);
		uint256 prev_log_sequence_number = get_prev_log_sequence_number_of(&walE, log_sequence_number, 0, &error);
		if(error)
			printf("error = %d\n", error);
		printf("(log_sequence_number="); print_uint256(log_sequence_number); printf(") (prev="); print_uint256(prev_log_sequence_number); printf(") (next="); print_uint256(next_log_sequence_number); printf(") (size = %u): <%s>\n", log_record_size, log_record);
		free(log_record);
		log_sequence_number = next_log_sequence_number;
	}
	printf("last_flushed_log_sequence_numbers = "); print_uint256(get_last_flushed_log_sequence_number(&walE)); printf("\n\n");
}

int main()
{
	int new_file = 0;

	block_file bf;
	if(!(new_file = create_and_open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS)) && !open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS))
	{
		printf("failed to create block file\n");
		return -1;
	}

	int init_error = 0;
	if(!initialize_wale(&walE, 3, (new_file ? get_uint256(7) : INVALID_LOG_SEQUENCE_NUMBER), NULL, get_block_io_functions(&bf), APPEND_ONLY_BUFFER_COUNT, &init_error))
	{
		printf("failed to create wale instance wale_erro = %d (error = %d on fd = %d)\n", init_error, errno, bf.file_descriptor);
		close_block_file(&bf);
		return -1;
	}

	append_test_log();
	append_test_log();

	int error = 0;
	printf("flushed until = "); print_uint256(flush_all_log_records(&walE, &error)); printf(" : error -> %d\n\n", error);

	append_test_log();

	print_all_flushed_logs();

	printf("flushed until = "); print_uint256(flush_all_log_records(&walE, &error)); printf(" : error -> %d\n\n", error);

	print_all_flushed_logs();

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
#include<stdio.h>
#include<stdlib.h>

#include<block_io.h>

#include<wale.h>

#include<string.h>
#include<errno.h>

#include<executor.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#define APPEND_ONLY_BUFFER_COUNT 1

#define NUMBERS "0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859()0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859"
#define LOG_FORMAT "thread_id=<%d> log_number=<%d> size=<%d> content=<%.*s>"

#define LOGS_PER_THREAD 64
#define FLUSH_EVERY_LOGS_PER_THREAD 9

#define THREAD_COUNT 6

block_io_ops get_block_io_functions(const block_file* bf);

wale walE;

int append_test_log(int thread_id, int log_number)
{
	char log_buffer[4096];
	uint32_t ls = (((unsigned int)rand()) % strlen(NUMBERS));
	sprintf(log_buffer, LOG_FORMAT, thread_id, log_number, ls, ls, NUMBERS);
	uint64_t log_sequence_number = append_log_record(&walE, log_buffer, strlen(log_buffer) + 1, 0);
	printf("log_sequence_number = %" PRIu64 " ::: %s\n\n", log_sequence_number, log_buffer);
	if(log_sequence_number == INVALID_LOG_SEQUENCE_NUMBER)
		return 0;
	return 1;
}

void* append_logs(void* tid)
{
	int thread_id = *((int*)(tid));
	for(int log_number = 0; log_number < LOGS_PER_THREAD; log_number++)
	{
		if(!append_test_log(thread_id, log_number))
			return NULL;

		// sleep for random time

		if(log_number % FLUSH_EVERY_LOGS_PER_THREAD == 0)
			printf("flushed until = %" PRIu64 " by %d\n\n", flush_all_log_records(&walE), thread_id);
	}
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

	if(!initialize_wale(&walE, (new_file ? 7 : INVALID_LOG_SEQUENCE_NUMBER), NULL, get_block_io_functions(&bf), APPEND_ONLY_BUFFER_COUNT))
	{
		printf("failed to create wale instance (error = %d on fd = %d)\n", errno, bf.file_descriptor);
		close_block_file(&bf);
		return -1;
	}

	executor* exe = new_executor(FIXED_THREAD_COUNT_EXECUTOR, THREAD_COUNT, THREAD_COUNT + 32, 0, NULL, NULL, NULL);

	int thread_ids[THREAD_COUNT];
	for(int tid = 0; tid < THREAD_COUNT; tid++)
	{
		thread_ids[tid] = tid;
		submit_job(exe, append_logs, &(thread_ids[tid]), NULL, 0);
	}

	shutdown_executor(exe, 0);
	wait_for_all_threads_to_complete(exe);
	delete_executor(exe);

	printf("flushed until = %" PRIu64 "\n\n", flush_all_log_records(&walE));

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
#include<stdio.h>
#include<stdlib.h>

#include<block_io.h>

#include<wale.h>

#include<string.h>
#include<errno.h>

#include<executor.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#define APPEND_ONLY_BUFFER_COUNT 32

#define NUMBERS "0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859()0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859+0123456789-10111213141516171819-20212223242526272829-30313233343536373839-40414243444546474849-50515253545556575859"
#define LOG_FORMAT "thread_id=<%d> log_number=<%d> size=<%d> content=<%.*s>"

//#define DEBUG_PRINT_LOG_BUFFER

#include<prwrite_specs.h>

block_io_ops get_block_io_functions(const block_file* bf);

wale walE;

int append_test_log(int thread_id, int log_number)
{
	char log_buffer[4096];
	uint32_t ls = (((unsigned int)rand()) % strlen(NUMBERS));
	sprintf(log_buffer, LOG_FORMAT, thread_id, log_number, ls, ls, NUMBERS);
	int error = 0;
	log_seq_nr log_sequence_number = append_log_record(&walE, log_buffer, strlen(log_buffer) + 1, 0, &error);
	#ifdef DEBUG_PRINT_LOG_BUFFER
		printf("log_sequence_number = "); print_log_seq_nr(log_sequence_number); printf(" ::: %s : error -> %d\n\n", log_buffer, error);
	#endif
	if(compare_log_seq_nr(log_sequence_number, INVALID_LOG_SEQUENCE_NUMBER) == 0)
	{
		printf("failed to append to wale\n");
		exit(-1);
	}
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
		{
			int error = 0;
			log_seq_nr flushed_until = flush_all_log_records(&walE, &error);
			#ifdef DEBUG_PRINT_LOG_BUFFER
				printf("flushed until = "); print_log_seq_nr(flushed_until); printf(" by %d : error -> %d\n\n", thread_id, error);
			#endif
			if(compare_log_seq_nr(flushed_until, INVALID_LOG_SEQUENCE_NUMBER) == 0)
			{
				printf("failed to flush logs from wale\n");
				exit(-1);
			}
		}
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

	int init_error = 0;
	if(!initialize_wale(&walE, 12, (new_file ? get_log_seq_nr(7) : INVALID_LOG_SEQUENCE_NUMBER), NULL, get_block_io_functions(&bf), APPEND_ONLY_BUFFER_COUNT, &init_error))
	{
		printf("failed to create wale instance wale_erro = %d (error = %d on fd = %d)\n", init_error, errno, bf.file_descriptor);
		close_block_file(&bf);
		return -1;
	}

	executor* exe = new_executor(FIXED_THREAD_COUNT_EXECUTOR, THREAD_COUNT, THREAD_COUNT + 32, 0, NULL, NULL, NULL);

	int thread_ids[THREAD_COUNT];
	for(int tid = 0; tid < THREAD_COUNT; tid++)
	{
		thread_ids[tid] = tid;
		submit_job_executor(exe, append_logs, &(thread_ids[tid]), NULL, NULL, 0);
	}

	shutdown_executor(exe, 0);
	wait_for_all_executor_workers_to_complete(exe);
	delete_executor(exe);

	int error = 0;
	printf("flushed until = "); print_log_seq_nr(flush_all_log_records(&walE, &error)); printf(" : error -> %d\n\n", error);

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
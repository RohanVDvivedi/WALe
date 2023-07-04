#include<stdio.h>

#include<block_io.h>

#include<wale.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

#define APPEND_ONLY_BUFFER_COUNT 1

block_io_ops get_block_io_functions(const block_file* bf);

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

	deinitialize_wale(&walE);

	close_block_file(&bf);

	return 0;
}
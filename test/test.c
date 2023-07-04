#include<stdio.h>

#include<wale.h>

#define ADDITIONAL_FLAGS	0 //| O_DIRECT | O_SYNC
#define FILENAME			"test.log"

int main()
{
	block_file bf;
	if(!create_and_open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS) && !open_block_file(&bf, FILENAME, ADDITIONAL_FLAGS | O_TRUNC))
	{
		printf("failed to create block file\n");
		return -1;
	}

	bloc_io_ops block_io_functions = get_block_io_functions(&bf);


	close_block_file(&bf);

	return 0;
}
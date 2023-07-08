#include<crc32_util.h>

#include<cutlery_math.h>

#include<zlib.h>

uint32_t crc32_util(uint32_t crc, const void* data, uint64_t data_size)
{
	uint64_t data_processed = 0;
	while(data_processed < data_size)
	{
		uint64_t data_to_process = min(UINT_MAX, data_size - data_processed);
		crc = crc32(crc, data + data_processed, data_to_process);
		data_processed += data_to_process;
	}
	return crc;
}
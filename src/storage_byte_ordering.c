#include<storage_byte_ordering.h>

#include<cutlery_math.h>

void serialize_le_uint64(char* bytes, uint32_t bytes_size, uint64_t val)
{
	bytes_size = min(bytes_size, sizeof(uint64_t));
	for(int i = 0; i < bytes_size; i++, val >>= CHAR_BIT)
		bytes[i] = val & UINT64_C(0xff);
}

void serialize_le_uint32(char* bytes, uint32_t bytes_size, uint32_t val)
{
	bytes_size = min(bytes_size, sizeof(uint32_t));
	for(int i = 0; i < bytes_size; i++, val >>= CHAR_BIT)
		bytes[i] = val & UINT32_C(0xff);
}

uint64_t deserialize_le_uint64(const char* bytes, uint32_t bytes_size)
{
	bytes_size = min(bytes_size, sizeof(uint64_t));
	uint64_t val = 0;
	for(uint32_t i = bytes_size; i > 0;)
	{
		--i;
		val = val | ((bytes[i] & UINT64_C(0xff)) << (i * CHAR_BIT));
	}
	return val;
}

uint32_t deserialize_le_uint32(const char* bytes, uint32_t bytes_size)
{
	bytes_size = min(bytes_size, sizeof(uint32_t));
	uint32_t val = 0;
	for(uint32_t i = bytes_size; i > 0;)
	{
		--i;
		val = val | ((bytes[i] & UINT32_C(0xff)) << (i * CHAR_BIT));
	}
	return val;
}
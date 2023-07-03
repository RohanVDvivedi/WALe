#include<storage_byte_ordering.h>

void serialize_le_uint64(char* bytes, uint64_t val)
{
	for(int i = 0; i < 8; i++, val >>= 8)
		bytes[i] = val & UINT64_C(0xff);
}

void serialize_le_uint32(char* bytes, uint32_t val)
{
	for(int i = 0; i < 4; i++, val >>= 8)
		bytes[i] = val & UINT32_C(0xff);
}

uint64_t deserialize_le_uint64(const char* bytes)
{
	uint64_t val = 0;
	for(int i = 8; i > 0;)
	{
		--i;
		val = val | ((bytes[i] & UINT64_C(0xff)) << (i * 8));
	}
	return val;
}

uint32_t deserialize_le_uint32(const char* bytes)
{
	uint64_t val = 0;
	for(int i = 4; i > 0;)
	{
		--i;
		val = val | ((bytes[i] & UINT32_C(0xff)) << (i * 8));
	}
	return val;
}
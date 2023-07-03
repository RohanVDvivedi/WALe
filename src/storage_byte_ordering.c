#include<storage_byte_ordering.h>

void serialize_uint64(char bytes* bytes, uint64_t val);
void serialize_uint32(char bytes* bytes, uint32_t val);

uint64_t deserialize_uint64(const char* bytes);
uint32_t deserialize_uint32(const char* bytes);
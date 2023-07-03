#ifndef STORAGE_BYTE_ORDERING_H
#define STORAGE_BYTE_ORDERING_H

#include<inttypes.h>

// serialize and deserialize unaligned bytes from their little endian to host byte ordering

void serialize_le_uint64(char* bytes, uint64_t val);
void serialize_le_uint32(char* bytes, uint32_t val);

uint64_t deserialize_le_uint64(const char* bytes);
uint32_t deserialize_le_uint32(const char* bytes);

#endif
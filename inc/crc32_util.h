#ifndef CRC32_UTIL_H
#define CRC32_UTIL_H

#include<stdint.h>

uint32_t crc32_util(uint32_t crc, const void* data, uint64_t data_size);

#endif
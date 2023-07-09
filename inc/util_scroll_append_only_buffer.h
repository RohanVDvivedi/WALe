#ifndef UTIL_SCROLL_APPEND_ONLY_BUFFER_H
#define UTIL_SCROLL_APPEND_ONLY_BUFFER_H

#include<wale.h>

// returns 1, if the buffer was scrolled up and that there is still space in the buffer for any write
// returns 0 on a failure
// it will unlock the global mutex while performing the write IO, no other locks will be released or acquired during this call
int scroll_append_only_buffer(wale* wale_p);

// to check if the byte in the file at file_offset is in the append_only_buffer
int is_file_offset_within_append_only_buffer(wale* wale_p, uint64_t file_offset);

#endif
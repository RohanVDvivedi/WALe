#ifndef UTIL_APPEND_ONLY_BUFFER_H
#define UTIL_APPEND_ONLY_BUFFER_H

#include<wale/wale.h>


// below function must be called with global lock (get_wale_lock(wale_p)) and a exclusive lock on the wale_p->append_only_buffer_lock held
// returns 1, if the buffer was scrolled up and that there is still space in the buffer for any write
// returns 0 on a failure
// it will unlock the global mutex while performing the write IO, no other locks will be released or acquired during this call
int scroll_append_only_buffer(wale* wale_p);

// below function must be called with the global lock (get_wale_lock(wale_p)) held
// to check if the byte in the file at file_offset is in the append_only_buffer
int is_file_offset_within_append_only_buffer(wale* wale_p, uint64_t file_offset);

// below function must be called with global lock (get_wale_lock(wale_p)) and a exclusive lock on the wale_p->append_only_buffer_lock held
// returns 1, if the append_only_buffer was resized
// returns 0 on a failure
// it may call scroll_append_only_buffer, i.e. it also may result in unlocking of the global mutex
int resize_append_only_buffer(wale* wale_p, uint64_t buffer_block_count, int* error);

#endif
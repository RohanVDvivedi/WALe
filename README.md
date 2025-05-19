# WALe
This is a library implementation of a Write Ahead Log (WAL) for use in building a database storage engine.

This library supports variable length log_sequence_numbers (predetermined before initializing WALe instance), upto 32 bytes. i.e. with a 10,000 MBps disk drive, you do not need to worry about the overflow for about 10^59 years.

It supports, crc32 check for each one your log records inserted.

*Information on ARIES support :*
 * after you perform an append_log_record, the append_only_buffer_lock is share-released and the you get the log_sequence_number of the new log record with global lock held (if using external lock).
 * so no one is allowed to enter and flush the log records, until you release the global lock.
 * this allows you to update the dirty page table (updating the page_id->recLSN map) and set the pageLSN on this newly dirty page, while global lock is still held.
 * this ensures that the log is persistent, only after the dirty page table is updated, and that the append of the log record is atomic with its update to the dirty page table, and the update of the pageLSN of the dirty page.
 * This remains true for even the flush_all_log_records function, immediately after its return with the global external lock held you can update the flushedLSN, this will allow you to further implement logic in bufferpool to only flush dirty pages, that have their pageLSN greater than or equal to the global flushedLSN.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [PosixUtils](https://github.com/RohanVDvivedi/PosixUtils)
 * [SerializableInteger](https://github.com/RohanVDvivedi/SerializableInteger)
 * [LockKing](https://github.com/RohanVDvivedi/LockKing)
 * [zlib](https://github.com/madler/zlib)      ($ sudo apt install zlib1g-dev)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/WALe.git`

**Build from source :**
 * `cd WALe`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lwale -llockking -lpthread -lcutlery -lz` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<wale/wale.h>`
   * `#include<wale/block_io_ops.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd WALe`
 * `sudo make uninstall`

## Third party acknowledgements
 * *crc32 implementation, internally supported by [zlib](https://github.com/madler/zlib) checkout their website [here](https://zlib.net/).*

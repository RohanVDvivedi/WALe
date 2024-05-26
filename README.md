# WALe
This is a library implementation of a Write Ahead Log (WAL) for use in building a database storage engine.

This library supports variable length log_sequence_numbers (predetermined before initializing WALe instance), upto 32 bytes. i.e. with a 10,000 MBps disk drive, you do not need to worry about the overflow for about 10^59 years.

It supports, crc32 check for each one your log records inserted.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [SerializableInteger](https://github.com/RohanVDvivedi/SerializableInteger)
 * [ReaderWriterLock](https://github.com/RohanVDvivedi/ReaderWriterLock)
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
 * add `-lwale -lserint -lrwlock -lpthread -lcutlery -lz` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<wale.h>`
   * `#include<block_io_ops.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd WALe`
 * `sudo make uninstall`

## Third party acknowledgements
 * *crc32 implementation, internally supported by [zlib](https://github.com/madler/zlib) checkout their website [here](https://zlib.net/).*

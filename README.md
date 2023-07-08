# WALe
This is a library implementation of a Write Ahead Log (WAL) for use in building a database storage engine.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
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
 * add `-lwale -lrwlock -lpthread -lcutlery` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<wale.h>`
   * `#include<block_io_ops.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd WALe`
 * `sudo make uninstall`

## Third party acknowledgements
 * *crc32 implementation, internally supported by [zlib](https://github.com/madler/zlib) checkout their website [here](https://zlib.net/).*
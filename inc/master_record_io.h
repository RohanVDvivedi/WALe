#ifndef MASTER_RECORD_IO_H
#define MASTER_RECORD_IO_H

#include<wale.h>

int read_master_record(master_record* mr, const block_io_ops* block_io_functions);

int write_and_flush_master_record(const master_record* mr, const block_io_ops* block_io_functions);

#endif
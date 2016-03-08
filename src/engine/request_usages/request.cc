// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Request.

#include "engine/request_usages/request.h"

namespace firmament {

Request::Request(uint64_t ram_cap, uint64_t disk_bw, uint64_t disk_cap) :
  ram_cap(ram_cap),
  disk_bw(disk_bw),
  disk_cap(disk_cap) {
}

} // namespace firmament
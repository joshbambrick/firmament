// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Request.

#ifndef ENGINE_REQUEST_USAGES_REQUEST_H
#define ENGINE_REQUEST_USAGES_REQUEST_H

#include <stdint.h>

namespace firmament {

class Request {
  public:
    Request(uint64_t ram_cap, uint64_t disk_bw, uint64_t disk_cap);
    static const uint32_t number_of_dimensions = 3;
    const double ram_cap;
    const double disk_bw;
    const double disk_cap;
};

} // namespace firmament

#endif  // ENGINE_REQUEST_USAGES_REQUEST_H
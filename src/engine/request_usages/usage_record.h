// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// UsageRecord.

#ifndef ENGINE_REQUEST_USAGES_USAGE_RECORD_H
#define ENGINE_REQUEST_USAGES_USAGE_RECORD_H

#include <stdint.h>
#include "engine/request_usages/request.h"

namespace firmament {

class UsageRecord {
  public:
    UsageRecord(bool is_valid,
                uint64_t usage_ram_cap,
                uint64_t usage_disk_bw,
                uint64_t usage_disk_cap);
    const bool is_valid;
    const uint64_t usage_ram_cap;
    const uint64_t usage_disk_bw;
    const uint64_t usage_disk_cap;
};

} // namespace firmament

#endif  // ENGINE_REQUEST_USAGES_USAGE_RECORD_H
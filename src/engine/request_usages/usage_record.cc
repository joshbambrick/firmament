// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// UsageRecord.

#include "engine/request_usages/usage_record.h"

namespace firmament {

UsageRecord::UsageRecord(
    bool is_valid,
    uint64_t usage_ram_cap,
    uint64_t usage_disk_bw,
    uint64_t usage_disk_cap)
  : is_valid(is_valid),
    usage_ram_cap(usage_ram_cap),
    usage_disk_bw(usage_disk_bw),
    usage_disk_cap(usage_disk_cap) {
}

} // namespace firmament
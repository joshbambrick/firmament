// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// UsageRecordList.

#include "engine/request_usages/usage_record_list.h"

namespace firmament {

UsageRecordList::UsageRecordList(
    const vector<UsageRecord>& record_list,
    const vector<uint64_t>& equivalence_classes,
    uint32_t timeslice_duration_ms,
    ResourceID_t machine_id,
    Request request)
  : record_list(record_list),
    equivalence_classes(equivalence_classes),
    timeslice_duration_ms(timeslice_duration_ms),
    machine_id(machine_id),
    request(request) {
}

} // namespace firmament
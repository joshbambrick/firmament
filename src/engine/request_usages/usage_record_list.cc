// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// UsageRecordList.

#include "engine/request_usages/usage_record_list.h"

namespace firmament {

UsageRecordList::UsageRecordList(
    vector<UsageRecord> record_list,
    uint32_t timeslice_duration_ms,
    uint64_t equivalence_class,
    uint64_t machine_id,
    Request request)
  : record_list(record_list),
    timeslice_duration_ms(timeslice_duration_ms),
    equivalence_class(equivalence_class),
    machine_id(machine_id),
    request(request) {
}

} // namespace firmament
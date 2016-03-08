// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// UsageRecordList.

#ifndef ENGINE_REQUEST_USAGES_USAGE_RECORD_LIST_H
#define ENGINE_REQUEST_USAGES_USAGE_RECORD_LIST_H

#include <vector>
#include <stdint.h>
#include "engine/request_usages/usage_record.h"

using namespace std;

namespace firmament {

class UsageRecordList {
  public:
    UsageRecordList(vector<UsageRecord> record_list,
                    uint32_t timeslice_duration_ms,
                    uint64_t equivalence_class,
                    uint64_t machine_id,
                    Request request);
    const vector<UsageRecord> record_list;
    const uint32_t timeslice_duration_ms;
    const uint64_t equivalence_class;
    const uint64_t machine_id;
    const Request request;
};

} // namespace firmament

#endif  // ENGINE_REQUEST_USAGES_USAGE_RECORD_LIST_H
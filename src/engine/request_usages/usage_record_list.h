// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// UsageRecordList.

#ifndef ENGINE_REQUEST_USAGES_USAGE_RECORD_LIST_H
#define ENGINE_REQUEST_USAGES_USAGE_RECORD_LIST_H

#include <vector>
#include <stdint.h>
#include "base/types.h"
#include "engine/request_usages/usage_record.h"

using namespace std;

namespace firmament {

class UsageRecordList {
  public:
    UsageRecordList(const vector<UsageRecord>& record_list,
                    const vector<EquivClass_t>& equivalence_classes,
                    uint32_t timeslice_duration_ms,
                    ResourceID_t machine_id,
                    Request request);
    const vector<UsageRecord> record_list;
    const vector<EquivClass_t> equivalence_classes;
    const uint32_t timeslice_duration_ms;
    const ResourceID_t machine_id;
    const Request request;
};

} // namespace firmament

#endif  // ENGINE_REQUEST_USAGES_USAGE_RECORD_LIST_H
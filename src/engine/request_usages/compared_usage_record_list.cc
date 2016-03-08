// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// ComparedUsageRecordList.

#include "engine/request_usages/compared_usage_record_list.h"

namespace firmament {

ComparedUsageRecordList::ComparedUsageRecordList(UsageRecordList base_list,
                                                 double distance)
  : UsageRecordList(base_list.record_list,
                    base_list.equivalence_classes,
                    base_list.timeslice_duration_ms,
                    base_list.machine_id,
                    base_list.request),
    distance(distance) {
}

} // namespace firmament
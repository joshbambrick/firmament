// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// ComparedUsageRecordList.

#ifndef ENGINE_REQUEST_USAGES_COMPARED_USAGE_RECORD_LIST_H
#define ENGINE_REQUEST_USAGES_COMPARED_USAGE_RECORD_LIST_H

#include "engine/request_usages/usage_record_list.h"

using namespace std;

namespace firmament {

class ComparedUsageRecordList : public UsageRecordList {
  public:
    ComparedUsageRecordList(UsageRecordList base_list, double distance);
    const double distance;
};

} // namespace firmament

#endif  // ENGINE_REQUEST_USAGES_COMPARED_USAGE_RECORD_LIST_H
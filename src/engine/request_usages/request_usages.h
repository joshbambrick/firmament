// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Request usages.

#ifndef ENGINE_REQUEST_USAGES_REQUEST_USAGES_H
#define ENGINE_REQUEST_USAGES_REQUEST_USAGES_H

#include <ANN/ANN.h>
#include <list>
#include <vector>
#include "engine/request_usages/usage_record_list.h"
#include "engine/request_usages/compared_usage_record_list.h"
#include "engine/request_usages/request.h"

using std::string;
using std::vector;
using std::list;


namespace firmament {

class RequestUsages {
  public:
    RequestUsages(uint32_t k,
                  double error_bound,
                  double rebuild_threshold,
                  uint32_t max_number_of_points);
    ~RequestUsages();

    void LookUpTree(Request request,
                    vector<ComparedUsageRecordList>* record_lists);
    void AddToTree(UsageRecordList new_record_list);

  protected:
    void DeleteTree();
    ANNpoint CreateRequestPoint(Request request);
    
    const uint32_t k_;
    const double error_bound_;
    const double rebuild_threshold_;
    const ANNidxArray neighbour_indices_;
    const ANNdistArray neighbour_distances_;
    const uint32_t max_number_of_points_;
    const uint32_t number_of_dimensions_ = Request::number_of_dimensions;

    ANNpointArray request_data_points_;
    vector<UsageRecordList*> usage_record_lists_in_tree_;
    list<UsageRecordList> usage_record_lists_;
    uint32_t number_of_points_ = 0;
    uint32_t record_list_size_ = 0;
    ANNkd_tree* tree_ = NULL;
    double mean_ram_cap_ = 0;
    double mean_disk_bw_ = 0;
    double mean_disk_cap_ = 0;
};
} // namespace firmament

#endif  // ENGINE_REQUEST_USAGES_REQUEST_USAGES_H
// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Request usages.

#include <string>
#include <cstdlib>
#include <stdlib.h>
#include "engine/request_usages/request_usages.h"

using namespace std;

namespace firmament {

RequestUsages::RequestUsages(
    uint32_t k,
    double error_bound,
    double rebuild_threshold,
    uint32_t max_number_of_points)
  : k_(k),
    error_bound_(error_bound),
    rebuild_threshold_(rebuild_threshold),
    neighbour_indices_(new ANNidx[k]),
    neighbour_distances_(new ANNdist[k]),
    max_number_of_points_(max_number_of_points),
    request_data_points_(annAllocPts(max_number_of_points_,
                                     number_of_dimensions_)) {
}

RequestUsages::~RequestUsages() {
  DeleteTree();
  delete [] neighbour_indices_;
  delete [] neighbour_distances_;
  annClose();
}

void RequestUsages::DeleteTree() {
  if (tree_) {
    delete tree_;
  }
}

void RequestUsages::LookUpTree(Request request,
                               vector<ComparedUsageRecordList>* record_lists) {
  if (tree_) {
    tree_->annkSearch(CreateRequestPoint(request),
                      k_,
                      neighbour_indices_,
                      neighbour_distances_,
                      error_bound_);

    for (uint32_t i = 0; i < k_; ++i) {
      record_lists->push_back(ComparedUsageRecordList(
          *usage_record_lists_in_tree_[neighbour_indices_[i]],
          sqrt(neighbour_distances_[i])));
    }
  }
}

void RequestUsages::AddToTree(UsageRecordList new_record_list) {
  UsageRecordList new_ann_record_list(new_record_list);
  usage_record_lists_.push_front(new_ann_record_list);
  record_list_size_++;

  bool rebuild_tree = false;
  if (record_list_size_ >= k_
      && record_list_size_ >= number_of_points_ * rebuild_threshold_) {
    rebuild_tree = true;
    uint32_t new_list_size = min(max_number_of_points_, record_list_size_);
    uint32_t record_lists_to_remove = record_list_size_ - new_list_size;

    for (uint32_t i = 0; i < record_lists_to_remove; ++i) {
      usage_record_lists_.pop_back();
    }
    number_of_points_ = new_list_size;
    record_list_size_ = new_list_size;
  }

  if (rebuild_tree) {
    usage_record_lists_in_tree_.clear();
    uint64_t sum_ram_cap = 0;
    uint64_t sum_disk_bw = 0;
    uint64_t sum_disk_cap = 0;
    for (auto& usage_record : usage_record_lists_) {
      sum_ram_cap += usage_record.request.ram_cap;
      sum_disk_bw += usage_record.request.disk_bw;
      sum_disk_cap += usage_record.request.disk_cap;
    }
    mean_ram_cap_ = static_cast<double>(sum_ram_cap)
                    / static_cast<double>(record_list_size_);
    mean_ram_cap_ = max(mean_ram_cap_, 1.0);
    mean_disk_bw_ = static_cast<double>(sum_disk_bw)
                    / static_cast<double>(record_list_size_);
    mean_disk_bw_ = max(mean_disk_bw_, 1.0);
    mean_disk_cap_ = static_cast<double>(sum_disk_cap)
                    / static_cast<double>(record_list_size_);
    mean_disk_cap_ = max(mean_disk_cap_, 1.0);

    uint32_t i = 0;
    for (auto& usage_record : usage_record_lists_) {
      request_data_points_[i] = CreateRequestPoint(usage_record.request);
      usage_record_lists_in_tree_.push_back(&usage_record);
      i++;
    }

    DeleteTree();
    tree_ = new ANNkd_tree(request_data_points_,
                           number_of_points_,
                           number_of_dimensions_);
  }
}

ANNpoint RequestUsages::CreateRequestPoint(Request request) {
  ANNpoint point = annAllocPt(number_of_dimensions_);
  point[0] = request.ram_cap / mean_ram_cap_;
  point[1] = request.disk_bw / mean_disk_bw_;
  point[2] = request.disk_cap / mean_disk_cap_;
  return point;
}

void RequestUsages::UpdateK(uint32_t k) {
    k_ = k;
}

} // namespace firmament
// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Container disk usage tracker.

#ifndef FIRMAMENT_MISC_CONTAINER_DISK_USAGE_TRACKER_H
#define FIRMAMENT_MISC_CONTAINER_DISK_USAGE_TRACKER_H

#include "base/common.h"
using std::string;

namespace firmament {

class ContainerDiskUsageTracker {
  public:
    ContainerDiskUsageTracker(string base_dir);
    void Update();
    uint64_t GetFullDiskUsage();
    bool IsInitialized();
    uint64_t UpdateDiskIOUsage(uint64_t new_usage);
    uint64_t UpdateDiskIOTime(uint64_t new_time);

  protected:
    uint64_t CalculateDirSize(string dir);

    bool is_initialized_ = false;
    uint64_t root_size_ = 0;
    uint64_t delta0_size_ = 0;
    uint64_t disk_io_usage = 0;
    uint64_t disk_io_time_ns = 0;
    string base_dir_;
    const string lower_dir_ = "rootfs";
    const string upper_dir_ = "delta0";
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_CONTAINER_DISK_USAGE_TRACKER_H

// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Container disk usage tracker.

#include "misc/container_disk_usage_tracker.h"
#include <boost/filesystem/operations.hpp>
using std::string;

namespace firmament {

ContainerDiskUsageTracker::ContainerDiskUsageTracker(string base_dir) 
  : base_dir_(base_dir) {
}

void ContainerDiskUsageTracker::Update() {
  if (!is_initialized_) {
    root_size_ = CalculateDirSize(base_dir_ + lower_dir_);
    is_initialized_ = true;
  }

  delta0_size_ = CalculateDirSize(base_dir_ + upper_dir_);
}

uint64_t ContainerDiskUsageTracker::GetFullDiskUsage() {
  return root_size_ + delta0_size_;
}

uint64_t ContainerDiskUsageTracker::CalculateDirSize(string dir) {
  uint64_t size = 0;
  try {
    for (boost::filesystem::recursive_directory_iterator it(dir);
         it != boost::filesystem::recursive_directory_iterator();
         ++it) {
      if (boost::filesystem::is_regular_file(*it)
          && !boost::filesystem::is_symlink(*it)) {
        size += boost::filesystem::file_size(*it);
      }
    }
  } catch (boost::filesystem::filesystem_error e) {
    LOG(ERROR) << e.what();
  }
  return size;
}

bool ContainerDiskUsageTracker::IsInitialized() {
  return is_initialized_;
}

uint64_t ContainerDiskUsageTracker::UpdateDiskIOUsage(uint64_t new_usage) {
  uint64_t old_usage = disk_io_usage;
  disk_io_usage = new_usage;
  return disk_io_usage - old_usage;
}

uint64_t ContainerDiskUsageTracker::UpdateDiskIOTime(uint64_t new_time) {
  uint64_t old_time = disk_io_time_ns;
  disk_io_time_ns = new_time;
  return disk_io_time_ns - old_time;
}

}  // namespace firmament

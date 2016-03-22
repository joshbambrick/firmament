// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Container monitor.

#ifndef FIRMAMENT_MISC_CONTAINER_MONITOR_H
#define FIRMAMENT_MISC_CONTAINER_MONITOR_H

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#else
#error Boost not available!
#endif

#include <string>
#include "base/common.h"
#include "base/resource_vector.pb.h"
#include "misc/utils.h"
#include "misc/container_disk_usage_tracker.h"

namespace firmament {

class ContainerMonitor {
  public:
    ContainerMonitor(
        int port, const string& monitor_host, const string& container_name,
        const string& disk_base_dir,
        bool track_disk_cap, bool init_disk_cap_sync);

    static void StartContainerMonitor(int port);

    bool CreateResourceVector(ResourceVector* rv);

  protected:
    bool CreateApiResourceVector(string json_input, ResourceVector* rv);
    string GetHttpResponse();
    bool AddDiskCapacity(ResourceVector* rv);
    void UpdateTaskDiskTrackerSync();
    static size_t WriteIncomingHttpData(void* content,
                                        size_t data_size,
                                        size_t mb_count,
                                        void* data);
    void UpdateTaskDiskTrackerAsync();

    ContainerDiskUsageTracker disk_tracker_;
    const string container_name_;
    const string url_;
    const bool track_disk_cap_;
    const bool init_disk_cap_sync_;
    boost::shared_mutex disk_tracker_mutex_;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_CONTAINER_MONITOR_H

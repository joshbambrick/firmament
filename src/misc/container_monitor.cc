// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Container monitor.

#include <string>
#include <cstdlib>
#include <stdlib.h>
#include <jansson.h>
#include <curl/curl.h>
#include "misc/container_monitor.h"
#include "base/common.h"
#include "base/units.h"

using namespace std;

namespace firmament {

void ContainerMonitor::StartContainerMonitor(int port) {
  string command = string("sudo docker run \
    --volume=/:/rootfs:ro \
    --volume=/var/run:/var/run:rw \
    --volume=/sys:/sys:ro \
    --volume=/var/lib/docker/:/var/lib/docker:ro \
    --publish=") + to_string(port) + string(":8080 \
    --detach=true \
    google/cadvisor:latest");

  if (system(command.c_str()) != 0) {
    LOG(ERROR) << "Could not start cAdvisor";
  }
}

ContainerMonitor::ContainerMonitor(
    int port, const string& monitor_host, const string& container_name,
    const string& disk_base_dir, bool track_disk_cap, bool init_disk_cap_sync)
  : disk_tracker_(disk_base_dir),
    container_name_("/lxc/" + container_name),
    url_(monitor_host + ":" + to_string(port)
         + "/api/v2.0/stats" + container_name_),
    track_disk_cap_(track_disk_cap),
    init_disk_cap_sync_(init_disk_cap_sync) {
}

size_t ContainerMonitor::WriteIncomingHttpData(
    void* content, size_t data_size, size_t mb_count, void* data) {
  static_cast<string*>(data)->append(static_cast<char*>(content),
                                       data_size * mb_count);
  return data_size * mb_count;
}

string ContainerMonitor::GetHttpResponse() {
  CURL* curl_ptr;
  CURLcode response;
  string buffer;

  curl_ptr = curl_easy_init();
  if (curl_ptr) {
    curl_easy_setopt(curl_ptr, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl_ptr, CURLOPT_WRITEFUNCTION, WriteIncomingHttpData);
    curl_easy_setopt(curl_ptr, CURLOPT_WRITEDATA, &buffer);
    response = curl_easy_perform(curl_ptr);
    curl_easy_cleanup(curl_ptr);
  }

  return buffer;
}


bool ContainerMonitor::CreateApiResourceVector(string json_input,
                                               ResourceVector* rv) {
  ResourceVector read_usage;

  if (json_input == "") return false;

  json_error_t error;
  json_t *root = json_loads(json_input.c_str(), 0, &error);

  if (!root) return false;

  json_t* container_events = json_object_get(root, container_name_.c_str());
  if (!json_is_array(container_events)) return false;


  uint64_t events = json_array_size(container_events);
  if (events == 0) return false;

  json_t* latest_event = json_array_get(container_events, events - 1);
  if (!json_is_object(latest_event)) return false;

  json_t* has_memory = json_object_get(latest_event, "has_memory");
  json_t* has_diskio = json_object_get(latest_event, "has_diskio");

  if (json_is_false(has_diskio) && json_is_false(has_memory)) return false;

  if (json_is_true(has_memory)) {
    json_t* memory = json_object_get(latest_event, "memory");
    if (!memory) return false;
    json_t* memory_usage = json_object_get(memory, "usage");
    if (!memory_usage) return false;

    int memory_usage_value = json_integer_value(memory_usage) / BYTES_TO_MB;
    if (memory_usage_value != 0) {
      read_usage.set_ram_cap(memory_usage_value);
    }
  }

  uint64_t total_disk_usage_bytes = 0;
  uint64_t total_disk_time_ns = 0;
  bool sevice_time_read = false;
  if (json_is_true(has_diskio)) {
    json_t* diskio = json_object_get(latest_event, "diskio");

    json_t* io_service_bytes = json_object_get(diskio, "io_service_bytes");
    if (io_service_bytes) {
      uint64_t groups = json_array_size(io_service_bytes);
      for (uint32_t i = 0; i < groups; ++i) {
        json_t* cur_group = json_array_get(io_service_bytes, i);
        json_t* cur_disk_stats = json_object_get(cur_group, "stats");
        json_t* cur_disk_usage = json_object_get(cur_disk_stats, "Total");
        total_disk_usage_bytes += json_integer_value(cur_disk_usage);
      }
    }

    json_t* io_service_time = json_object_get(diskio, "io_service_time");
    if (io_service_time) {
      sevice_time_read = true;
      uint64_t groups = json_array_size(io_service_time);
      for (uint32_t i = 0; i < groups; ++i) {
        json_t* cur_group = json_array_get(io_service_time, i);
        json_t* cur_disk_stats = json_object_get(cur_group, "stats");
        json_t* cur_disk_time = json_object_get(cur_disk_stats, "Total");
        total_disk_time_ns += json_integer_value(cur_disk_time);
      }
    }
  }

  uint64_t total_disk_time_us = 0;
  total_disk_usage_bytes =
      disk_tracker_.UpdateDiskIOUsage(total_disk_usage_bytes);
  uint64_t time_since_last_check = disk_tracker_.UpdateDiskIOCheckTime(
      GetCurrentTimestamp());
  if (sevice_time_read) {
    total_disk_time_ns = disk_tracker_.UpdateDiskIOTime(total_disk_time_ns);
    total_disk_time_us = total_disk_time_ns / NANOSECONDS_IN_MICROSECOND;
  } else {
    total_disk_time_us = time_since_last_check;
  }

  uint64_t disk_bw_value = (total_disk_time_us > 0)
      ? ((total_disk_usage_bytes * SECONDS_TO_MICROSECONDS)
          / (total_disk_time_us * BYTES_TO_MB))
      : 0;

  read_usage.set_disk_bw(disk_bw_value);

  rv->CopyFrom(read_usage);

  return true;
}

bool ContainerMonitor::CreateResourceVector(ResourceVector* rv) {
  bool success = false;
  ResourceVector new_rv;
  success = CreateApiResourceVector(GetHttpResponse(), &new_rv);

  if (success && track_disk_cap_) {
    success = AddDiskCapacity(&new_rv);
  }

  if (success) {
    rv->CopyFrom(new_rv);
  }
  return success;
}

bool ContainerMonitor::AddDiskCapacity(ResourceVector* rv) {
  boost::unique_lock<boost::shared_mutex> tracker_lock(disk_tracker_mutex_);
  bool capacity_added_if_expected = false;
  if (!disk_tracker_.IsInitialized()) {
    if (init_disk_cap_sync_) {
      disk_tracker_.Update();
    } else {
      capacity_added_if_expected = true;
    }
  }
  if (disk_tracker_.IsInitialized()) {
    rv->set_disk_cap(disk_tracker_.GetFullDiskUsage() / BYTES_TO_MB);
    capacity_added_if_expected = true;
  }
  UpdateTaskDiskTrackerAsync();
  return capacity_added_if_expected;
}


void ContainerMonitor::UpdateTaskDiskTrackerSync() {
  boost::unique_lock<boost::shared_mutex> tracker_lock(disk_tracker_mutex_);
  disk_tracker_.Update();
}

void ContainerMonitor::UpdateTaskDiskTrackerAsync() {
  boost::thread async_update_thread(
      boost::bind(&ContainerMonitor::UpdateTaskDiskTrackerSync, this));
}

}  // namespace firmament

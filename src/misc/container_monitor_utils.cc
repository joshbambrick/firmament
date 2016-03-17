// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Container monitor utils.

#include <string>
#include <cstdlib>
#include <stdlib.h>
#include <jansson.h>
#include <curl/curl.h>
#include "misc/container_monitor_utils.h"
#include "base/common.h"
#include "base/units.h"

using namespace std;

namespace firmament {

void ContainerMonitorUtils::StartContainerMonitor(int port) {
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

size_t ContainerMonitorUtils::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

string ContainerMonitorUtils::GetHttpResponse(string url) {
  CURL *curl;
  CURLcode res;
  string readBuffer;

  curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
  }

  return readBuffer;
}


ResourceVector ContainerMonitorUtils::CreateResourceVector(
    string json_input, string task_container_name,
    ContainerDiskUsageTracker* disk_tracker) {
  ResourceVector resource_vector;
  string parse_fail_message = "Couldn't parse cAdvisor response";

  if (json_input == "") {
    throw parse_fail_message;
  }

  json_error_t error;
  json_t *root = json_loads(json_input.c_str(), 0, &error);

  if (!root) {
    throw parse_fail_message;
  }

  json_t* container_events = json_object_get(root, task_container_name.c_str());
  if (!json_is_array(container_events)) {
    throw parse_fail_message;
  }


  uint64_t events = json_array_size(container_events);
  if (events == 0) {
    throw parse_fail_message;
  }

  json_t* latest_event = json_array_get(container_events, events - 1);
  if (!json_is_object(latest_event)) {
    throw parse_fail_message;
  }

  json_t* has_memory = json_object_get(latest_event, "has_memory");
  json_t* has_diskio = json_object_get(latest_event, "has_diskio");

  if (json_is_false(has_diskio) && json_is_false(has_memory)) {
    throw parse_fail_message;
  }

  if (json_is_true(has_memory)) {
    json_t* memory = json_object_get(latest_event, "memory");
    if (!memory) {
      throw parse_fail_message;
    }
    json_t* memory_usage = json_object_get(memory, "usage");

    if (!memory_usage) {
      throw parse_fail_message;
    }

    int memory_usage_value = json_integer_value(memory_usage) / BYTES_TO_MB;
    if (memory_usage_value != 0) {
      resource_vector.set_ram_cap(memory_usage_value);
    }
  }

  // TODO(Josh): consider tracking difference in bytes and time between runs
  // the current solution considers mean disk bandwidth over entire execution
  // alternatively, could sample over the last n readings, or the last t time
  uint64_t total_disk_usage = 0;
  uint64_t total_disk_time_ns = 0;
  if (json_is_true(has_diskio)) {
    json_t* diskio = json_object_get(latest_event, "diskio");

    json_t* io_service_bytes = json_object_get(diskio, "io_service_bytes");
    if (io_service_bytes) {
      uint64_t groups = json_array_size(io_service_bytes);
      for (uint32_t i = 0; i < groups; ++i) {
        json_t* cur_group = json_array_get(io_service_bytes, i);
        json_t* cur_disk_stats = json_object_get(cur_group, "stats");
        json_t* cur_disk_usage = json_object_get(cur_disk_stats, "Total");
        total_disk_usage += json_integer_value(cur_disk_usage);
      }
    }

    json_t* io_service_time = json_object_get(diskio, "io_service_time");
    if (io_service_time) {
      uint64_t groups = json_array_size(io_service_time);
      for (uint32_t i = 0; i < groups; ++i) {
        json_t* cur_group = json_array_get(io_service_time, i);
        json_t* cur_disk_stats = json_object_get(cur_group, "stats");
        json_t* cur_disk_time = json_object_get(cur_disk_stats, "Total");
        total_disk_time_ns += json_integer_value(cur_disk_time);
      }
    }
  }

  if (disk_tracker) {
    total_disk_usage = disk_tracker->UpdateDiskIOUsage(total_disk_usage);
    total_disk_time_ns = disk_tracker->UpdateDiskIOTime(total_disk_time_ns);
  }

  uint64_t disk_bw_value = total_disk_time_ns > 0
      ? total_disk_usage / (total_disk_time_ns / SECONDS_TO_NANOSECONDS)
      : 0;
  resource_vector.set_disk_bw(disk_bw_value);

  return resource_vector;
}

ResourceVector ContainerMonitorUtils::CreateResourceVector(
    int port,
    string container_monitor_host,
    string task_container_name) {
  return CreateResourceVector(port, container_monitor_host, task_container_name,
                              NULL);
}

ResourceVector ContainerMonitorUtils::CreateResourceVector(
    int port,
    string container_monitor_host,
    string task_container_name,
    ContainerDiskUsageTracker* disk_tracker) {
  task_container_name = "/lxc/" + task_container_name;
  string url = container_monitor_host + ":" + to_string(port) +
      "/api/v2.0/stats" + task_container_name;
  ResourceVector resource_vector;
  try {
    resource_vector = CreateResourceVector(GetHttpResponse(url),
                                           task_container_name,
                                           disk_tracker);
  } catch (string message) {
    LOG(ERROR) << message;
  }

  return resource_vector;
}

}  // namespace firmament

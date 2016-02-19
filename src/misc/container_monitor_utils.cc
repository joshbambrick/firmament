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


ResourceVector ContainerMonitorUtils::CreateResourceVector(string json_input, string task_container_name) {
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

  if (json_is_false(has_diskio) || json_is_false(has_memory)) {
    throw parse_fail_message;
  }

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
  return resource_vector;
}

ResourceVector ContainerMonitorUtils::CreateResourceVector(int port,
                                                  string container_monitor_uri,
                                                  string task_container_name) {
  task_container_name = "/lxc/" + task_container_name;
  string url = container_monitor_uri + ":" + to_string(port) + "/api/v2.0/stats" + task_container_name;
  ResourceVector resource_vector;
  try {
    resource_vector = CreateResourceVector(GetHttpResponse(url), task_container_name);
  } catch (string message) { 
    LOG(ERROR) << message;
  }

  return resource_vector;
}

}  // namespace firmament

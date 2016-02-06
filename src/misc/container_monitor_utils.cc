// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Container monitor utils.

#include <string>
#include <cstdlib>
#include <stdlib.h>
#include "misc/container_monitor_utils.h"
#include "base/resource_vector.pb.h"
#include "cpprest/http_client.h"
#include "cpprest/json.h"

using namespace std;
using namespace web;
using namespace json;
using namespace utility;
using namespace http;
using namespace http::client;

namespace firmament {


void StartContainerMonitor(int port) {
  string command = string("sudo docker run \
    --volume=/:/rootfs:ro \
    --volume=/var/run:/var/run:rw \
    --volume=/sys:/sys:ro \
    --volume=/var/lib/docker/:/var/lib/docker:ro \
    --publish=") + to_string(port) + string(":8080 \
    --detach=true \
    google/cadvisor:latest");

  system(command.c_str());
}

ResourceVector ContainerMonitorCreateResourceVector(int port,
    string container_monitor_uri, string task_container_name) {
  uri_builder ub(container_monitor_uri.c_str());

  ub.set_port(port);
  ub.append_path(U("/api/v2.0/stats"));

  // cadvisor adds this prefix
  task_container_name = "/lxc/" + task_container_name;

  ub.append_path(U(task_container_name.c_str()));
  http::uri node_uri = ub.to_uri();
  return GetResourceUsageVector(node_uri, task_container_name);
}

ResourceVector GetResourceUsageVector(http::uri node_uri, string task_container_name) {
  ResourceVector resource_vector;
  json::value resource_usage = GetResourceUsageJson(node_uri, task_container_name);
  if (!resource_usage.has_field("error")) {
    resource_vector.set_ram_cap(resource_usage["memory"]["usage"].as_integer());
  }
  return resource_vector;
}

json::value GetResourceUsageJson(http::uri node_uri, string task_container_name) {
  pplx::task<json::value> t = GetResourceUsageTask(node_uri, task_container_name);
  t.wait();
  return t.get();
}

pplx::task<json::value> HandleResourceUsageException(
      pplx::task<json::value> task) {
  try {
    task.get();
  } catch (const std::exception& ex) {
    return pplx::task_from_result<json::value>(CreateErrorJson(ex.what()));
  }

  return task;
}

json::value CreateErrorJson(string msg) {
  json::value error_json = json::value::object();
  error_json[U("error")] = json::value::string(
      utility::conversions::to_string_t(msg));
  return error_json;
}

pplx::task<json::value> GetResourceUsageTask(http::uri node_uri, string task_container_name) {
  http_client monitor_client(node_uri);
  return monitor_client.request(methods::GET).then([](http_response resp) {
    return resp.extract_json();
  }).then([=](json::value resources_json) {
    bool isValid = true;

    json::value container_events = NULL;
    if (resources_json.has_field(U(task_container_name))) {
      container_events = resources_json[task_container_name];
    } else {
      isValid = false;
    }

    json::value latest_event = NULL;
    if (isValid && container_events.is_array()) {
      latest_event = container_events[0];
    } else {
      isValid = false;
    }

    if (isValid && (
        !latest_event.has_field(U("has_diskio"))
        || !latest_event.has_field(U("has_memory"))
        || !latest_event["has_diskio"].as_bool()
        || !latest_event["has_memory"].as_bool())) {
      isValid = false;
    }


    return isValid ? latest_event : CreateErrorJson("no data");
  }).then([=](pplx::task<json::value> t) {
    return HandleResourceUsageException(t);
  });
}

}  // namespace firmament

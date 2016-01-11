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

void StartContainerMonitor() {
  string command = string("sudo docker run \
    --volume=/:/rootfs:ro \
    --volume=/var/run:/var/run:rw \
    --volume=/sys:/sys:ro \
    --volume=/var/lib/docker/:/var/lib/docker:ro \
    --publish=8080:") + getenv("FLAGS_container_monitor_port_") + string(" \
    --detach=true \
    --name=cadvisor \
    google/cadvisor:latest");
  system(command.c_str());
}

ResourceVector ContainerMonitorCreateResourceVector(
    string container_monitor_uri, string task_container_name) {
  uri_builder ub(container_monitor_uri.c_str());
  ub.append_path(U("/api/v1/stats/"));
  ub.append_path(U(task_container_name.c_str()));
  http::uri node_uri = ub.to_uri();
  return GetResourceUsageVector(node_uri);
}

ResourceVector GetResourceUsageVector(http::uri node_uri) {
  json::value resource_usage = GetResourceUsageJson(node_uri);
  ResourceVector resource_vector;
  resource_vector.set_ram_cap(resource_usage["memory"]["usage"].as_integer());
  return resource_vector;
}

json::value GetResourceUsageJson(http::uri node_uri) {
  pplx::task<json::value> t = GetResourceUsageTask(node_uri);
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

pplx::task<json::value> GetResourceUsageTask(http::uri node_uri) {
  http_client monitor_client(node_uri);
  return monitor_client.request(methods::GET).then([](http_response resp) {
    //PrintResponse(full_uri.to_string(), apiClient.request(methods::GET, buf.str()).get());
    return resp.extract_json();
  }).then([=](json::value resources_json) {
    if (!resources_json.has_field(U("has_diskio"))
        || !resources_json.has_field(U("has_memory"))
        || !resources_json.get(U("has_diskio")).as_bool()
        || !resources_json.get(U("has_memory")).as_bool()) {
      return CreateErrorJson("no data");
    }

    return resources_json;
  }).then([=](pplx::task<json::value> t) {
    return HandleResourceUsageException(t);
  });
}

}  // namespace firmament

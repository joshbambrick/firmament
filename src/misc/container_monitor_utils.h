// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Container monitor utils.

#include "base/resource_vector.pb.h"
#include "cpprest/http_client.h"
#include "cpprest/json.h"
using std::string;
using namespace web;
using namespace http;
using namespace json;

namespace firmament {

void StartContainerMonitor();
ResourceVector ContainerMonitorCreateResourceVector(
    string container_monitor_uri, string task_container_name);
ResourceVector GetResourceUsageVector(http::uri node_uri);
json::value GetResourceUsageJson(http::uri node_uri);
pplx::task<json::value> HandleResourceUsageException(
      pplx::task<json::value> task);
json::value CreateErrorJson(string msg);
pplx::task<json::value> GetResourceUsageTask(http::uri node_uri);

}  // namespace firmament

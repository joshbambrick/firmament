// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Container monitor utils.

using std::string;
#include "base/resource_vector.pb.h"

namespace firmament {

void StartContainerMonitor();
ResourceVector* ContainerMonitorCreateResourceVector(
    string container_monitor_uri, string task_container_name);
ResourceVector GetResourceUsageVector(http::uri node_uri);
json::value GetResourceUsageJson(http::uri node_uri);
pplx::task<json::value> HandleResourceUsageException(
      pplx::task<json::value> task);
json::value CreateErrorJson(string msg);
pplx::task<json::value> GetResourceUsageTask(http::uri node_uri);

}  // namespace firmament

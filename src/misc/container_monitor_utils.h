// The Firmament project
// Copyright (c) 2016 Joshua Bambrick <jpbambrick@gmail.com>
//
// Container monitor utils.

#include "base/common.h"
#include "base/resource_vector.pb.h"
using std::string;

namespace firmament {

class ContainerMonitorUtils {
  public:
    static void StartContainerMonitor(int port);
    static ResourceVector CreateResourceVector(int port,
                                 string container_monitor_uri,
                                 string task_container_name);

  protected:
    static ResourceVector CreateResourceVector(string json_input,
                                               string task_container_name);
    static size_t WriteCallback(void *contents,
                                size_t size,
                                size_t nmemb,
                                void *userp);
    static string GetHttpResponse(string url);
};

}  // namespace firmament

// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Container monitor utils.

using std::string;
#include "base/resource_vector.pb.h"

namespace firmament {

void StartContainerMonitor();
ResourceVector ContainerMonitorCreateResourceVector(
	string container_monitor_uri, string task_container_name);

}  // namespace firmament

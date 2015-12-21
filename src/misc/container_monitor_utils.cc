// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Container monitor utils.

#include <string>
#include "misc/container_monitor_utils.h"
#include "base/resource_vector.pb.h"

namespace firmament {

void StartContainerMonitor() {
  string command = "sudo docker run \
    --volume=/:/rootfs:ro \
    --volume=/var/run:/var/run:rw \
    --volume=/sys:/sys:ro \
    --volume=/var/lib/docker/:/var/lib/docker:ro \
    --publish=8080:" + getenv("FLAGS_container_monitor_port_") + " \
    --detach=true \
    --name=cadvisor \
    google/cadvisor:latest";
  system(command);
}

ResourceVector* ContainerMonitorCreateResourceVector(
    string container_monitor_uri, string task_container_name) {
  ResourceVector* resource_vector;
  resource_vector->set_ram_cap();
  // create HTTP request and ram from JSON body
  return resource_vector;
}

}  // namespace firmament

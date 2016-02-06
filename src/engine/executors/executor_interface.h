// The Firmament project
// Copyright (c) 2011-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The executor interface assumed by the engine.

#ifndef FIRMAMENT_ENGINE_EXECUTORS_EXECUTOR_INTERFACE_H
#define FIRMAMENT_ENGINE_EXECUTORS_EXECUTOR_INTERFACE_H

#include "misc/printable_interface.h"

#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/task_final_report.pb.h"
#include "messages/task_heartbeat_message.pb.h"
#include "messages/task_state_message.pb.h"

namespace firmament {
namespace executor {

class ExecutorInterface : public PrintableInterface {
 public:
  virtual bool CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks) = 0;
  virtual void HandleTaskCompletion(TaskDescriptor* td,
                                    TaskFinalReport* report) = 0;
  virtual void HandleTaskEviction(TaskDescriptor* td) = 0;
  virtual void HandleTaskFailure(TaskDescriptor* td) = 0;
  virtual void RunTask(TaskDescriptor* td,
                       bool firmament_binary) = 0;
  virtual void KillTask(TaskDescriptor* td) = 0;
  virtual ostream& ToString(ostream* stream) const = 0;
  virtual void CreateTaskHeartbeats(vector<TaskHeartbeatMessage>* heartbeats) = 0;
  virtual void CreateTaskStateChanges(vector<TaskStateMessage>* state_messages) = 0;

 protected:
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EXECUTORS_EXECUTOR_INTERFACE_H

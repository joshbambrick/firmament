// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Ths implements a simple local executor. It currently simply starts processes,
// sets their CPU affinities and runs them under perf profiling.
//
// This class, however, also forms the endpoint of remote executors: what they
// do, in fact, is to send a message to a remote resource, which will then
// instruct its local executor to actually start a process.

#ifndef FIRMAMENT_ENGINE_EXECUTORS_LOCAL_EXECUTOR_H
#define FIRMAMENT_ENGINE_EXECUTORS_LOCAL_EXECUTOR_H

#include "engine/executors/executor_interface.h"

#include <vector>
#include <string>
#include <utility>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#else
#error Boost not available!
#endif

extern "C" {
  #include <lxc/lxccontainer.h>
}

#include "base/common.h"
#include "base/types.h"
#include "base/task_final_report.pb.h"
#include "misc/container_disk_usage_tracker.h"
#include "messages/task_heartbeat_message.pb.h"
#include "messages/task_state_message.pb.h"
#include "engine/executors/task_health_checker.h"
#include "engine/executors/topology_manager.h"

namespace firmament {
namespace executor {

using machine::topology::TopologyManager;

class LocalExecutor : public ExecutorInterface {
 public:
  LocalExecutor(ResourceID_t resource_id,
                const string& coordinator_uri);
  LocalExecutor(ResourceID_t resource_id,
                const string& coordinator_uri,
                shared_ptr<TopologyManager> topology_mgr);
  ~LocalExecutor();
  bool CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks);
  void HandleTaskCompletion(TaskDescriptor* td,
                            TaskFinalReport* report);
  void HandleTaskEviction(TaskDescriptor* td);
  void HandleTaskFailure(TaskDescriptor* td);
  void RunTask(TaskDescriptor* td,
               bool firmament_binary);
  void SendAbortMessage(TaskDescriptor* td);
  void SendFailedMessage(TaskDescriptor* td);
  void CreateTaskHeartbeats(vector<TaskHeartbeatMessage>* heartbeats);
  void CreateTaskStateChanges(vector<TaskStateMessage>* state_messages);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<LocalExecutor at resource "
                   << to_string(local_resource_id_)
                   << ">";
  }
 protected:
  // Unit tests
  FRIEND_TEST(LocalExecutorTest, SimpleSyncProcessExecutionTest);
  FRIEND_TEST(LocalExecutorTest, SyncProcessExecutionWithArgsTest);
  FRIEND_TEST(LocalExecutorTest, AsyncProcessExecutionWithArgsTest);
  FRIEND_TEST(LocalExecutorTest, ExecutionFailureTest);
  FRIEND_TEST(LocalExecutorTest, SimpleTaskExecutionTest);
  FRIEND_TEST(LocalExecutorTest, TaskExecutionWithArgsTest);
  ResourceID_t local_resource_id_;
  char* AddPerfMonitoringToCommandLine(const unordered_map<string, string>&,
                                       vector<char*>* argv);
  char* AddDebuggingToCommandLine(vector<char*>* argv);
  void CleanUpCompletedTask(const TaskDescriptor& td);
  void ClearCompletedTaskFinalizeMessages(TaskID_t task_id,
                                          bool called_from_cleared_up);
  void CreateDirectories();
  void GetPerfDataFromLine(TaskFinalReport* report,
                           const string& line);
  int32_t RunProcessAsync(TaskID_t task_id,
                          const string& cmdline,
                          vector<string> args,
                          unordered_map<string, string> env,
                          ResourceVector resource_reservations,
                          bool perf_monitoring,
                          bool debug,
                          bool default_args,
                          const string& tasklog);
  int32_t RunProcessSync(TaskID_t task_id,
                         const string& cmdline,
                         vector<string> args,
                         string data_dir,
                         ResourceVector resource_reservations,
                         bool perf_monitoring,
                         bool debug,
                         bool default_args,
                         const string& tasklog);
  bool _RunTask(TaskDescriptor* td,
                bool firmament_binary);
  int ExecuteBinaryInContainer(TaskID_t task_id,
                               string data_dir,
                               vector<char*> argv,
                               ResourceVector resource_reservations,
                               string container_name);
  void ShutdownContainerIfRunning(TaskID_t task_id);
  TaskHeartbeatMessage CreateTaskHeartbeat(TaskID_t task_id);
  void SetFinalizeMessage(TaskID_t task_id,
                          TaskDescriptor::TaskState new_state);
  string PerfDataFileName(const TaskDescriptor& td);
  void ReadFromPipe(int fd);
  char* TokenizeIntoArgv(const string& str, vector<char*>* argv);
  string CreateMountConfigEntry(string dir);
  bool WaitForPerfFile(const string& file_name);
  void WriteToPipe(int fd, void* data, size_t len);
  void UpdateTaskDiskTrackerSync(TaskID_t task_id);
  void UpdateTaskDiskTrackerAsync(TaskID_t task_id);
  string GetTaskContainerName(TaskID_t task_id);
  // This holds the currently configured URI of the coordinator for this
  // resource (which must be unique, for now).
  const string coordinator_uri_;
  // The health manager checks on the liveness of locally managed tasks.
  TaskHealthChecker health_checker_;
  // Local pointer to topology manager
  // TODO(malte): Figure out what to do if this local executor is associated
  // with a dumb worker, who does not have topology support!
  shared_ptr<TopologyManager> topology_manager_;
  // Heartbeat interval for tasks running on the associated resource, in
  // nanoseconds.
  uint64_t heartbeat_interval_;
  boost::mutex exec_mutex_;
  boost::shared_mutex task_running_map_mutex_;
  boost::shared_mutex cleared_up_map_mutex_;
  boost::shared_mutex handler_map_mutex_;
  boost::shared_mutex pid_map_mutex_;
  boost::shared_mutex task_finalize_message_map_mutex_;
  boost::shared_mutex task_container_names_map_mutex_;
  boost::shared_mutex task_disk_tracker_map_mutex_;
  boost::shared_mutex task_heartbeat_update_map_mutex_;
  boost::condition_variable exec_condvar_;
  // Map to each task's local handler thread
  unordered_map<TaskID_t, boost::thread*> task_handler_threads_;
  unordered_map<TaskID_t, pid_t> task_pids_;
  unordered_map<TaskID_t, TaskStateMessage> task_finalize_messages_;
  unordered_map<TaskID_t, bool> task_finalize_messages_sent_;
  unordered_map<TaskID_t, bool> task_running_;
  unordered_map<TaskID_t, bool> cleared_up_tasks_;
  unordered_map<TaskID_t, string> task_container_names_;
  unordered_map<TaskID_t, ContainerDiskUsageTracker> task_disk_trackers_;
  unordered_map<TaskID_t, uint64_t> task_heartbeat_sequence_numbers_;
  unordered_map<TaskID_t, bool> task_sent_perf_stats_;
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EXECUTORS_LOCAL_EXECUTOR_H

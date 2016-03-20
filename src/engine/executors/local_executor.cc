// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Local executor class.

#include "engine/executors/local_executor.h"

extern "C" {
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <lxc/lxccontainer.h>
#include <lxc/attach_options.h>
}
#include <fstream>
#include <boost/regex.hpp>
#include <boost/filesystem/operations.hpp>

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "engine/executors/task_health_checker.h"
#include "misc/utils.h"
#include "misc/container_monitor_utils.h"
#include "misc/uri_tools.h"
#include "misc/map-util.h"

// declare since defined in coordinator (linked first)
DEFINE_int32(container_monitor_port, 8010,
            "The port of the coordinator's container monitor");
DEFINE_int32(blockdev_major_number, 8,
            "The major number of the block device");
DEFINE_int32(blockdev_minor_number, 0,
            "The minor number of the block device");
// define since this can be overridden by a flag, not used in coordinator
DEFINE_string(container_monitor_host, "",
            "The host of the container monitor.");
DEFINE_bool(use_storage_resource_monitoring, true,
            "Whether to support monitoring of container storage use.");
DEFINE_bool(use_storage_resource_monitor_intialize_sync, true,
            "Whether to synchronously initialize storage resource monitor.");
DEFINE_bool(enforce_cgroup_limits, true,
            "Whether to enforce limits on resources using cgroups.");
DEFINE_string(rootfs_base_dir, "/var/lib/lxc/",
            "Directory where container rootfs directories will be.");
DEFINE_bool(pin_tasks_to_cores, true,
            "Pin tasks to their allocated CPU core when executing.");
DEFINE_bool(debug_tasks, false,
            "Run tasks through a debugger (gdb).");
DEFINE_uint64(debug_interactively, 0,
              "Run this task ID inside an interactive debugger.");
DEFINE_bool(perf_monitoring, false,
            "Enable performance monitoring for tasks executed.");
DEFINE_string(task_lib_dir, "build/engine/",
              "Path where task_lib.a and task_lib_inject.so are.");
DEFINE_string(task_log_dir, "/tmp/firmament-log",
              "Path where task logs will be stored.");
DEFINE_string(task_perf_dir, "/tmp/firmament-perf",
              "Path where tasks' perf logs should be written.");
DEFINE_string(task_data_dir, "/tmp/firmament-data",
              "Path where tasks' perf logs should be written.");
DEFINE_string(task_extra_dir, "",
              "Any additional director that you would like to mount.");
DEFINE_string(perf_event_list,
              "cpu-clock,task-clock,context-switches,cpu-migrations,"
              "page-faults,cycles,instructions,branches,branch-misses,"
              "cache-misses,cache-references,stalled-cycles-frontend,"
              "stalled-cycles-backend,node-loads,node-load-misses",
              "Comma-separated list of perf events to monitor.");

namespace firmament {
namespace executor {

using common::pb_to_vector;

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      health_checker_(&task_handler_threads_, &handler_map_mutex_),
      topology_manager_(shared_ptr<TopologyManager>()),  // NULL
      heartbeat_interval_(1000000000ULL) {  // 1 billios nanosec = 1 sec
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "No topology manager passed, so will not bind to resource.";
  CreateDirectories();
}

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri,
                             shared_ptr<TopologyManager> topology_mgr)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      health_checker_(&task_handler_threads_, &handler_map_mutex_),
      topology_manager_(topology_mgr),
      heartbeat_interval_(1000000000ULL) {  // 1 billios nanosec = 1 sec
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "Tasks will be bound to the resource by the topology manager"
          << "at " << topology_manager_;
  CreateDirectories();
}

LocalExecutor::~LocalExecutor() {
  for (auto& it : task_running_) {
    ShutdownContainerIfRunning(it.first);
  }
}

char* LocalExecutor::AddPerfMonitoringToCommandLine(
    const unordered_map<string, string>& env,
    vector<char*>* argv) {
  // Define the string prefix for performance monitoring
  //string perf_string = "perf stat -x, -o ";
  VLOG(2) << "Enabling performance monitoring...";
  string perf_string = "perf stat -o ";
  const string* perf_fname = FindOrNull(env, "PERF_FNAME");
  CHECK_NOTNULL(perf_fname);
  perf_string += *perf_fname;
  perf_string += " -e ";
  perf_string += FLAGS_perf_event_list;
  perf_string += " -- ";
  return TokenizeIntoArgv(perf_string, argv);
}

char* LocalExecutor::AddDebuggingToCommandLine(vector<char*>* argv) {
  // Define the string prefix for debugging
  string dbg_string;
  VLOG(2) << "Enabling debugging...";
  if (FLAGS_debug_tasks)
    dbg_string = "gdb -batch -ex run --args ";
  else if (FLAGS_debug_interactively != 0)
    dbg_string = "gdb '-ex run --args ";
  return TokenizeIntoArgv(dbg_string, argv);
}

bool LocalExecutor::CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks) {
  boost::unique_lock<boost::shared_mutex> details_lock(task_finalize_message_map_mutex_);
  return health_checker_.Run(failed_tasks, &task_finalize_messages_);
}

void LocalExecutor::CleanUpCompletedTask(const TaskDescriptor& td) {
  // Drop task handler thread
  boost::unique_lock<boost::shared_mutex> handler_lock(handler_map_mutex_);
  boost::unique_lock<boost::shared_mutex> pids_lock(pid_map_mutex_);
  boost::unique_lock<boost::shared_mutex> running_lock(task_running_map_mutex_);
  boost::unique_lock<boost::shared_mutex> heartbeat_lock(task_heartbeat_update_map_mutex_);
  task_handler_threads_.erase(td.uid());
  // Kill container
  ShutdownContainerIfRunning(td.uid());
  // Issue a kill to make double-sure that the task has finished
  // XXX(malte): this is a hack!
  pid_t* pid = FindOrNull(task_pids_, td.uid());
  CHECK_NOTNULL(pid);
  int ret = kill(*pid, SIGKILL);
  LOG(INFO) << "kill(2) for task " << td.uid() << " returned " << ret;
  task_pids_.erase(td.uid());
  task_running_.erase(td.uid());
  task_heartbeat_sequence_numbers_.erase(td.uid());
  task_sent_perf_stats_.erase(td.uid());
  boost::unique_lock<boost::shared_mutex> disk_tracker_lock(
      task_disk_tracker_map_mutex_);
  task_disk_trackers_.erase(td.uid());
  ClearCompletedTaskFinalizeMessages(td.uid(), true);
}

void LocalExecutor::ClearCompletedTaskFinalizeMessages(
      TaskID_t task_id,
      bool called_from_cleared_up) {
  boost::unique_lock<boost::shared_mutex> finalized_lock(task_finalize_message_map_mutex_);
  boost::unique_lock<boost::shared_mutex> cleared_lock(cleared_up_map_mutex_);
  bool* map_cleared_up = FindOrNull(cleared_up_tasks_, task_id);
  bool* message_sent = FindOrNull(task_finalize_messages_sent_, task_id);

  if (called_from_cleared_up || (map_cleared_up && *map_cleared_up)) {
    if (message_sent && *message_sent) {
      cleared_up_tasks_.erase(task_id);
      task_finalize_messages_sent_.erase(task_id);
      task_finalize_messages_.erase(task_id);
    } else {
      InsertIfNotPresent(&cleared_up_tasks_, task_id, true);
    }
  }
}


void LocalExecutor::CreateDirectories() {
  struct stat st;
  // Task logs (stdout and stderr)
  if (!FLAGS_task_log_dir.empty() &&
      stat(FLAGS_task_log_dir.c_str(), &st) == -1) {
    mkdir(FLAGS_task_log_dir.c_str(), 0700);
  }
  // Tasks' perf logs
  if (!FLAGS_task_perf_dir.empty() &&
      stat(FLAGS_task_perf_dir.c_str(), &st) == -1) {
    mkdir(FLAGS_task_perf_dir.c_str(), 0700);
  }
  // Tasks' data directory
  if (!FLAGS_task_data_dir.empty() &&
      stat(FLAGS_task_data_dir.c_str(), &st) == -1) {
    mkdir(FLAGS_task_data_dir.c_str(), 0700);
  }
}

void LocalExecutor::GetPerfDataFromLine(TaskFinalReport* report,
                                        const string& line) {
  boost::regex e("[[:space:]]*? ([0-9,.]+) ([a-zA-Z-]+) .*");
  boost::smatch m;
  if (boost::regex_match(line, m, e, boost::match_extra)
      && m.size() == 3) {
    string number = m[1].str();
    // Remove any commas
    number.erase(std::remove(number.begin(), number.end(), ','), number.end());
    VLOG(1) << "matched: " << m[2] << ", " << number;
    if (m[2] == "instructions") {
      report->set_instructions(strtoul(number.c_str(), NULL, 10));
    } else if (m[2] == "cycles") {
      report->set_cycles(strtoul(number.c_str(), NULL, 10));
    } else if (m[2] == "seconds") {
      report->set_runtime(strtold(number.c_str(), NULL));
    } else if (m[2] == "cache-references") {
      report->set_llc_refs(strtoul(number.c_str(), NULL, 10));
    } else if (m[2] == "cache-misses") {
      report->set_llc_misses(strtoul(number.c_str(), NULL, 10));
    }
  }
}

void LocalExecutor::HandleTaskCompletion(TaskDescriptor* td,
                                         TaskFinalReport* report) {
  uint64_t end_time = GetCurrentTimestamp();
  td->set_finish_time(end_time);
  uint64_t start_time = td->start_time();
  report->set_task_id(td->uid());
  report->set_start_time(start_time);
  report->set_finish_time(end_time);
  // Load perf data, if it exists
  if (FLAGS_perf_monitoring) {
    FILE* fptr;
    char line[1024];
    string file_name = PerfDataFileName(*td);
    if (WaitForPerfFile(file_name)) {
      // Once we get here, we have non-zero data in the perf data file
      if ((fptr = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(ERROR) << "Failed to open perf data file " << file_name;
      } else {
        VLOG(1) << "Processing perf output in file " << file_name
                << "...";
        while (!feof(fptr)) {
          char* lptr = fgets(line, 1024, fptr);
          if (lptr != NULL) {
            GetPerfDataFromLine(report, line);
          }
        }
        if (fclose(fptr) != 0) {
          PLOG(ERROR) << "Failed to close perf file " << file_name
                      << " after reading!";
        }
      }
    } else {
      LOG(ERROR) << "Perf file " << file_name << " does not exists or does not "
                 << "contain data!";
    }
  } else {
    // TODO(malte): this is a bit of a hack -- when we don't have the perf
    // information available, we use the executor's runtime measurements.
    // They should be identical, however, so maybe we should just always do
    // this. Multiplication by 1M converts from microseconds to seconds.
    report->set_runtime(end_time / SECONDS_TO_MICROSECONDS -
                        start_time / SECONDS_TO_MICROSECONDS);
  }
  // Now clean up any remaining state.
  CleanUpCompletedTask(*td);
}

void LocalExecutor::HandleTaskEviction(TaskDescriptor* td) {
  // TODO(ionel): Implement.
}

void LocalExecutor::SendAbortMessage(TaskDescriptor* td) {
  SetFinalizeMessage(td->uid(), TaskDescriptor::ABORTED);
}

void LocalExecutor::SendFailedMessage(TaskDescriptor* td) {
  SetFinalizeMessage(td->uid(), TaskDescriptor::FAILED);
}

void LocalExecutor::HandleTaskFailure(TaskDescriptor* td) {
  td->set_finish_time(GetCurrentTimestamp());
  // Nothing to be done other than cleaning up; there is no final
  // report for failed task at this time.
  CleanUpCompletedTask(*td);
}

void LocalExecutor::RunTask(TaskDescriptor* td,
                            bool firmament_binary) {
  CHECK(td);
  // Save the start time.
  uint64_t start_time = GetCurrentTimestamp();
  // Mark the start time of the task.
  td->set_start_time(start_time);
  // XXX(malte): Move this over to use RunProcessAsync, instead of custom thread
  // spawning.
  boost::unique_lock<boost::mutex> exec_lock(exec_mutex_);
  boost::unique_lock<boost::shared_mutex> handler_lock(handler_map_mutex_);
  boost::thread* task_thread = new boost::thread(
      boost::bind(&LocalExecutor::_RunTask, this, td, firmament_binary));
  CHECK(InsertIfNotPresent(&task_handler_threads_, td->uid(), task_thread));
  exec_condvar_.wait(exec_lock);
}

bool LocalExecutor::_RunTask(TaskDescriptor* td,
                             bool firmament_binary) {
  // Convert arguments as specified in TD into a string vector that we can munge
  // into an actual argv[].
  vector<string> args;
  // Arguments
  if (td->args_size() > 0) {
    args = pb_to_vector(td->args());
  }
  string data_dir = FLAGS_task_data_dir + "/" + td->job_id() + "-" +
                    to_string(td->uid());
  mkdir(data_dir.c_str(), 0700);
  // Path for task log files (stdout/stderr)
  string tasklog = FLAGS_task_log_dir + "/" + td->job_id() +
                   "-" + to_string(td->uid());
  // TODO(malte): This is somewhat hackish
  // arguments: binary (path + name), arguments, performance monitoring on/off,
  // debugging flags, is this a Firmament task binary? (on/off; will cause
  // default arugments to be passed)

  ResourceVector resource_reservations = td->resource_request();
  bool res = (RunProcessSync(
      td->uid(), td->binary(), args, data_dir, resource_reservations,
      FLAGS_perf_monitoring, (FLAGS_debug_tasks ||
                              ((FLAGS_debug_interactively != 0) &&
                                   (td->uid() == FLAGS_debug_interactively))),
      firmament_binary, tasklog) == 0);
  VLOG(1) << "Result of RunProcessSync was " << res;
  return res;
}

int32_t LocalExecutor::RunProcessAsync(TaskID_t task_id,
                                       const string& cmdline,
                                       vector<string> args,
                                       unordered_map<string, string> env,
                                       ResourceVector resource_reservations,
                                       bool perf_monitoring,
                                       bool debug,
                                       bool default_args,
                                       const string& tasklog) {
  // TODO(malte): We lose the thread reference here, so we can never join this
  // thread. Need to return or store if we ever need it for anythign again.
  // TODO(josh): Decide if this is needed. The extra arg causes boost issues.
  // boost::thread async_process_thread(
  //     boost::bind(&LocalExecutor::RunProcessSync, this, task_id, cmdline, args,
  //                 env, resource_reservations, perf_monitoring, debug,
  //                 default_args, tasklog));
  // We hard-code the return to zero here; maybe should return a thread
  // reference instead.
  return 0;
}

int32_t LocalExecutor::RunProcessSync(TaskID_t task_id,
                                      const string& cmdline,
                                      vector<string> args,
                                      string data_dir,
                                      ResourceVector resource_reservations,
                                      bool perf_monitoring,
                                      bool debug,
                                      bool default_args,
                                      const string& tasklog) {
  char* perf_prefix;
  pid_t pid;
  /*int pipe_to[2];    // pipe to feed input data to task
  int pipe_from[3];  // pipe to receive output data from task
  if (pipe(pipe_to) != 0) {
    PLOG(ERROR) << "Failed to create pipe to task.";
  }
  if (pipe(pipe_from) != 0) {
    PLOG(ERROR) << "Failed to create pipe from task.";
  }*/
  vector<char*> argv;
  // Get paths for task logs
  string tasklog_stdout = tasklog + "-stdout";
  string tasklog_stderr = tasklog + "-stderr";
  // N.B.: only one of debug and perf_monitoring can be active at a time;
  // debug takes priority here.
  if (debug) {
    // task debugging is active, so reserve extra space for the
    // gdb invocation prefix.
    argv.reserve(args.size() + (default_args ? 4 : 3));
    perf_prefix = AddDebuggingToCommandLine(&argv);
  } else {
    // only need to reserve space for the default and NULL args
    argv.reserve(args.size() + (default_args ? 2 : 1));
  }
  argv.push_back((char*)(cmdline.c_str()));  // NOLINT
  if (default_args)
    argv.push_back(
        (char*)"--tryfromenv=coordinator_uri,resource_id,task_id");  // NOLINT
  for (uint32_t i = 0; i < args.size(); ++i) {
    // N.B.: This casts away the const qualifier on the c_str() result.
    // Unsafe, but okay since args lives beyond the execvpe call.
    VLOG(1) << "Adding extra argument \"" << args[i] << "\"";
    argv.push_back((char*)(args[i].c_str()));  // NOLINT
  }
  // The last item in the argv for lxc is always NULL.
  argv.push_back(NULL);

  // Print the whole command line
  string full_cmd_line;
  for (vector<char*>::const_iterator arg_iter = argv.begin();
       arg_iter != argv.end();
       ++arg_iter) {
    if (*arg_iter != NULL) {
      full_cmd_line += *arg_iter;
      full_cmd_line += " ";
    }
  }
  LOG(INFO) << "COMMAND LINE for task " << task_id << ": "
            << full_cmd_line;
  VLOG(1) << "About to fork child process for task execution of "
          << task_id << "!";

  string container_name = GetTaskContainerName(task_id);
  {
    boost::unique_lock<boost::shared_mutex> containers_lock(task_container_names_map_mutex_);
    CHECK(InsertIfNotPresent(&task_container_names_, task_id, container_name));
    boost::unique_lock<boost::shared_mutex> running_lock(task_running_map_mutex_);
    CHECK(InsertIfNotPresent(&task_running_, task_id, true));
    boost::unique_lock<boost::shared_mutex> disk_tracker_lock(
        task_disk_tracker_map_mutex_);
    string fs_dir = FLAGS_rootfs_base_dir + container_name + "/";
    CHECK(InsertIfNotPresent(&task_disk_trackers_, task_id,
                             ContainerDiskUsageTracker(fs_dir)));
  }

  pid = fork();
  switch (pid) {
    case -1:
      // Error
      LOG(ERROR) << "Failed to fork child process.";
      break;
    case 0: {
      // Child
      // Set up stderr and stdout log redirections to files
      int stdout_fd = open(tasklog_stdout.c_str(),
                           O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
      // TODO(Josh): Re-enable this
      // dup2(stdout_fd, STDOUT_FILENO);
      int stderr_fd = open(tasklog_stderr.c_str(),
                           O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
      dup2(stderr_fd, STDERR_FILENO);
      if (close(stdout_fd) != 0)
        PLOG(FATAL) << "Failed to close stdout FD in child";
      if (close(stderr_fd) != 0)
        PLOG(FATAL) << "Failed to close stderr FD in child";

      // Close the open FDs in the child before exec-ing, so that the task does
      // not inherit all of the coordinator's sockets and FDs.
      // We start from 3 here in order to avoid closing stdin/stdout/stderr.
      int fd;
      int fds;
      if ((fds = getdtablesize()) == -1) fds = OPEN_MAX_GUESS;
      for (fd = 3; fd < fds; fd++) {
        if (close(fd) == -EBADF)
          break;
      }

      // kill child process if parent terminates
      // SOMEDAY(adam): make this portable beyond Linux?
#ifdef __linux__
      prctl(PR_SET_PDEATHSIG, SIGHUP);
#endif
     _exit(ExecuteBinaryInContainer(task_id, data_dir, argv,
                                    resource_reservations, container_name));
    }
    default:
      // Parent
      VLOG(1) << "Task process with PID " << pid << " created.";
      {
        boost::unique_lock<boost::shared_mutex> pid_lock(pid_map_mutex_);
        CHECK(InsertIfNotPresent(&task_pids_, task_id, pid));
      }
      // Pin the task to the appropriate resource
      if (topology_manager_ && FLAGS_pin_tasks_to_cores)
        topology_manager_->BindPIDToResource(pid, local_resource_id_);
      // Notify any other threads waiting to execute processes (?)
      exec_condvar_.notify_one();
      // Wait for task to terminate
      int status;
      while (waitpid(pid, &status, 0) != pid) {
        VLOG(3) << "Waiting for child process " << pid << " to exit...";
      }
      {
        boost::unique_lock<boost::shared_mutex> running_lock(task_running_map_mutex_);
        // Update value (if not already erased)
        if (FindOrNull(task_running_, task_id)) {
          CHECK(!InsertOrUpdate(&task_running_, task_id, false));
        }
      }

      if (WIFEXITED(status)) {
        VLOG(1) << "Task process with PID " << pid << " exited with status "
                << WEXITSTATUS(status);
        if (status == 0) {
          SetFinalizeMessage(task_id, TaskDescriptor::COMPLETED);
        }
      } else if (WIFSIGNALED(status)) {
        VLOG(1) << "Task process with PID " << pid << " exited due to uncaught "
                << "signal " << WTERMSIG(status);
      } else if (WIFSTOPPED(status)) {
        VLOG(1) << "Task process with PID " << pid << " is stopped due to "
                << "signal " << WSTOPSIG(status);
      } else {
        LOG(ERROR) << "Unexpected exit status: " << hex << status;
      }
      return status;
  }
  return -1;
}

int LocalExecutor::ExecuteBinaryInContainer(TaskID_t task_id,
                                            string data_dir,
                                            vector<char*> argv,
                                            ResourceVector resource_reservations,
                                            string container_name) {
  LOG(INFO) << "Creating container for task " << task_id;
  lxc_container *c = lxc_container_new(container_name.c_str(), NULL);
  if (!c->createl(c, "ubuntu",
                  FLAGS_use_storage_resource_monitoring ? "aufs" : NULL,
                  NULL, LXC_CREATE_QUIET, "-r", "trusty", NULL)) {
    LOG(ERROR) << "Could not create container for task " << task_id;
  } else {
    LOG(INFO) << "Successfully created container for task " << task_id;
  }

  if (FLAGS_enforce_cgroup_limits) {
    const char* ram_cap =
        to_string(resource_reservations.ram_cap() * BYTES_TO_MB).c_str();
    if (strcmp(ram_cap, "0") != 0) {
      c->set_config_item(c, "lxc.cgroup.memory.limit_in_bytes", ram_cap);
    }


    string disk_bw = to_string(resource_reservations.disk_bw() * BYTES_TO_MB);
    if (disk_bw != "0" && FLAGS_blockdev_major_number != -1
        && FLAGS_blockdev_minor_number != -1) {
      disk_bw = to_string(FLAGS_blockdev_major_number) + ":"
          + to_string(FLAGS_blockdev_minor_number) + " " + disk_bw;
      c->set_config_item(c, "lxc.cgroup.blkio.throttle.write_bps_device",
                         disk_bw.c_str());
      c->set_config_item(c, "lxc.cgroup.blkio.throttle.read_bps_device",
                         disk_bw.c_str());
    }
  }

  string full_task_log_dir =
      boost::filesystem::canonical(FLAGS_task_log_dir).string();
  string full_data_dir =
      boost::filesystem::canonical(data_dir).string();
  string full_extra_dir =
      boost::filesystem::canonical(FLAGS_task_extra_dir).string();

  c->set_config_item(c, "lxc.mount.entry",
                     CreateMountConfigEntry(full_task_log_dir).c_str());
  c->set_config_item(c, "lxc.mount.entry",
                     CreateMountConfigEntry(full_data_dir).c_str());
  c->set_config_item(c, "lxc.mount.entry",
                     CreateMountConfigEntry(full_extra_dir).c_str());

  c->start(c, 0, NULL);

  lxc_attach_options_t attach_options = LXC_ATTACH_OPTIONS_DEFAULT;

  // Change to task's working directory
  attach_options.initial_cwd = (char*) full_data_dir.c_str();


  LOG(INFO) << "Executing binary of task " << task_id;
  int res = -1;
  res = c->attach_run_wait(c, &attach_options, argv[0], &argv[0]);

  ShutdownContainerIfRunning(task_id);

  return res == -1 ? 100 : res;
}

string LocalExecutor::CreateMountConfigEntry(string dir) {
  return dir + " " + dir.substr(1) + " none defaults,bind,create=dir 0 0";
}

void LocalExecutor::CreateTaskHeartbeats(vector<TaskHeartbeatMessage>* heartbeats) {
  boost::shared_lock<boost::shared_mutex> handler_lock(handler_map_mutex_);
  boost::unique_lock<boost::shared_mutex> messages_lock(task_finalize_message_map_mutex_);
  boost::unique_lock<boost::shared_mutex> running_lock(task_running_map_mutex_);

  for (unordered_map<TaskID_t, boost::thread*>::const_iterator
       it = task_handler_threads_.begin();
       it != task_handler_threads_.end();
       ++it) {
      bool* state_message_sent = FindOrNull(task_finalize_messages_sent_, it->first);
      bool* task_running = FindOrNull(task_running_, it->first);

      if (task_running && *task_running && (!state_message_sent || !*state_message_sent)) {
        heartbeats->push_back(CreateTaskHeartbeat(it->first));
      }
  }
}

TaskHeartbeatMessage LocalExecutor::CreateTaskHeartbeat(TaskID_t task_id) {
  boost::unique_lock<boost::shared_mutex> heartbeat_lock(task_heartbeat_update_map_mutex_);
  boost::unique_lock<boost::shared_mutex> container_names_lock(task_container_names_map_mutex_);
  uint64_t* heartbeat_sequence_number_ptr =
      FindOrNull(task_heartbeat_sequence_numbers_, task_id);
  string* container_name = FindOrNull(task_container_names_, task_id);
  uint64_t heartbeat_sequence_number = 0;
  if (heartbeat_sequence_number_ptr) {
    heartbeat_sequence_number = *heartbeat_sequence_number_ptr + 1;
  }

  TaskHeartbeatMessage task_heartbeat;
  task_heartbeat.set_task_id(task_id);
  task_heartbeat.set_sequence_number(heartbeat_sequence_number);
  if (container_name) {
    string monitor_host = (FLAGS_container_monitor_host != "")
        ? FLAGS_container_monitor_host
        : URITools::GetHostnameFromURI(coordinator_uri_);

    ResourceVector usage;
    bool usage_read = false;

    {
      boost::unique_lock<boost::shared_mutex> disk_tracker_lock(
              task_disk_tracker_map_mutex_);
      ContainerDiskUsageTracker* task_disk_tracker =
          FindOrNull(task_disk_trackers_, task_id);
      CHECK_NOTNULL(task_disk_tracker);
      usage_read = ContainerMonitorUtils::CreateResourceVector(
          FLAGS_container_monitor_port, monitor_host, *container_name,
          task_disk_tracker, &usage);
    }

    bool* sent_perf_stats = FindOrNull(task_sent_perf_stats_, task_id);
    if ((usage_read && usage.has_ram_cap() && usage.has_disk_bw())
        || (sent_perf_stats && *sent_perf_stats)) {
      TaskPerfStatisticsSample stats;
      stats.set_task_id(task_id);
      stats.set_timestamp(GetCurrentTimestamp());
      if (usage.has_ram_cap() && usage.has_disk_bw()) {
        ResourceVector* stats_usage = stats.mutable_resources();
        stats_usage->CopyFrom(usage);

        {
          boost::unique_lock<boost::shared_mutex> disk_tracker_lock(
              task_disk_tracker_map_mutex_);
          ContainerDiskUsageTracker* task_disk_tracker =
              FindOrNull(task_disk_trackers_, task_id);
          CHECK_NOTNULL(task_disk_tracker);

          if (FLAGS_use_storage_resource_monitoring) {
            if (!task_disk_tracker->IsInitialized()
                && FLAGS_use_storage_resource_monitor_intialize_sync) {
              task_disk_tracker->Update();
            }
            if (task_disk_tracker->IsInitialized()) {
              stats_usage->set_disk_cap(task_disk_tracker->GetFullDiskUsage() / BYTES_TO_MB);
            }
            UpdateTaskDiskTrackerAsync(task_id);
          }
        }
      }
      task_heartbeat.mutable_stats()->CopyFrom(stats);
      InsertOrUpdate(&task_sent_perf_stats_, task_id, true);
    }
  }
  InsertOrUpdate(&task_heartbeat_sequence_numbers_, task_id, heartbeat_sequence_number);
  return task_heartbeat;
}

void LocalExecutor::CreateTaskStateChanges(vector<TaskStateMessage>* state_messages) {
  set<TaskID_t> sent_tasks;
  {
    boost::shared_lock<boost::shared_mutex> handler_lock(handler_map_mutex_);
    boost::unique_lock<boost::shared_mutex> messages_lock(task_finalize_message_map_mutex_);

    for (unordered_map<TaskID_t, TaskStateMessage>::const_iterator
         it = task_finalize_messages_.begin();
         it != task_finalize_messages_.end();
         ++it) {
        bool* state_message_sent = FindOrNull(task_finalize_messages_sent_, it->first);

        if (!state_message_sent || !*state_message_sent) {
          state_messages->push_back(it->second);
          InsertOrUpdate(&task_finalize_messages_sent_, it->first, true);
          sent_tasks.insert(it->first);
        }
    }
  }
  for (set<TaskID_t>::const_iterator
       it = sent_tasks.begin();
       it != sent_tasks.end();
       ++it) {
    ClearCompletedTaskFinalizeMessages(*it, false);
  }
}

void LocalExecutor::SetFinalizeMessage(TaskID_t task_id,
                                       TaskDescriptor::TaskState new_state) {
  boost::unique_lock<boost::shared_mutex>messages_lock(
      task_finalize_message_map_mutex_);
  TaskStateMessage* current_message = FindOrNull(
      task_finalize_messages_, task_id);
  if (!current_message) {
    TaskStateMessage finalize_message;
    finalize_message.set_id(task_id);
    finalize_message.set_new_state(new_state);
    CHECK(InsertIfNotPresent(&task_finalize_messages_, task_id,
                             finalize_message));
  }
}


void LocalExecutor::ShutdownContainerIfRunning(TaskID_t task_id) {
  boost::unique_lock<boost::shared_mutex> containers_lock(task_container_names_map_mutex_);
  string* container_name = FindOrNull(task_container_names_, task_id);
  if (container_name) {
    task_container_names_.erase(task_id);
    lxc_container *container = lxc_container_new(container_name->c_str(), NULL);
    if (container->is_running(container)) {
      if (!container->shutdown(container, 30)) {
        container->stop(container);
      }

      container->destroy(container);
    }

    lxc_container_put(container);
  }
}

string LocalExecutor::PerfDataFileName(const TaskDescriptor& td) {
  string fname = FLAGS_task_perf_dir + "/" + (to_string(local_resource_id_)) +
                 "-" + to_string(td.uid()) + ".perf";
  return fname;
}

char* LocalExecutor::TokenizeIntoArgv(const string& str, vector<char*>* argv) {
  // Ugly parsing code to transform str into an argv representation
  char* str_c_string = (char*)malloc(str.size()+1);  // NOLINT
  snprintf(str_c_string, str.size(), "%s", str.c_str());
  char* piece;
  char* tmp_ptr = NULL;
  piece = strtok_r(str_c_string, " ", &tmp_ptr);
  while (piece != NULL) {
    argv->push_back(piece);
    piece = strtok_r(NULL, " ", &tmp_ptr);
  }
  // Return pointer to the allocated string in order to be able to delete it
  // later.
  VLOG(1) << "After adding tokenized version of '" << str
          << "', size of argv is " << argv->size();
  return str_c_string;
}

bool LocalExecutor::WaitForPerfFile(const string& file_name) {
  // This hack is required to avoid a race between the data file being
  // written by the perf utility and it being opened for reading.
  // Perf creates a zero-byte file when the task starts, but only syncs
  // data to it when it finishes.
  struct stat st;
  bzero(&st, sizeof(struct stat));
  uint64_t timestamp = GetCurrentTimestamp();
  // Wait at most 10s for perf file to become available
  // TODO(malte): this is a bit of a hack to avoid us getting stuck here
  // forever; we will want to move to a saner, locking-based scheme here.
  if (stat(file_name.c_str(), &st) != 0)
    return false;
  while (st.st_size == 0 && GetCurrentTimestamp() <=
         timestamp + 1 * SECONDS_TO_MICROSECONDS) {
    if (stat(file_name.c_str(), &st) != 0) {
      PLOG(ERROR) << "Failed to stat perf data file " << file_name;
      return false;
    }
  }
  if (st.st_size > 0) {
    // The perf file exists and contains non-zero data
    return true;
  } else {
    return false;
  }
}

void LocalExecutor::WriteToPipe(int fd, void* data, size_t len) {
  FILE *stream;
  // Open the pipe
  if ((stream = fdopen(fd, "w")) == NULL) {
    LOG(ERROR) << "Failed to open pipe for writing. FD: " << fd;
  }
  // Write the data to the pipe
  fwrite(data, len, 1, stream);
  // Finally, close the pipe
  CHECK_EQ(fclose(stream), 0);
}

void LocalExecutor::ReadFromPipe(int fd) {
  FILE *stream;
  int ch;
  if ((stream =fdopen(fd, "r")) == NULL) {
    LOG(ERROR) << "Failed to open pipe for reading. FD " << fd;
  }
  while ( (ch = getc(stream)) != EOF ) {
    // XXX(malte): temp hack
    putc(ch, stdout);
  }
  fflush(stdout);
  CHECK_EQ(fclose(stream), 0);
}

void LocalExecutor::UpdateTaskDiskTrackerSync(TaskID_t task_id) {
  boost::unique_lock<boost::shared_mutex> disk_tracker_lock(
      task_disk_tracker_map_mutex_);
  ContainerDiskUsageTracker* task_disk_tracker =
      FindOrNull(task_disk_trackers_, task_id);
  CHECK_NOTNULL(task_disk_tracker);
  task_disk_tracker->Update();
}

void LocalExecutor::UpdateTaskDiskTrackerAsync(TaskID_t task_id) {
  boost::thread async_update_thread(
      boost::bind(&LocalExecutor::UpdateTaskDiskTrackerSync, this, task_id));
}

string LocalExecutor::GetTaskContainerName(TaskID_t task_id) {
  return "task" + to_string(task_id);
}

}  // namespace executor
}  // namespace firmament

// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// General abstract superclass for event-driven schedulers. This contains shared
// implementation, e.g. task binding and remote delegation mechanisms.

#include "scheduling/event_driven_scheduler.h"

#include <deque>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/units.h"
#include "misc/map-util.h"
#include "base/usage_list.pb.h"
#include "base/task_usage_record.pb.h"
#include "misc/utils.h"
#include "engine/executors/local_executor.h"
#include "engine/executors/remote_executor.h"
#include "engine/executors/simulated_executor.h"
#include "engine/request_usages/request.h"
#include "engine/request_usages/usage_record.h"
#include "engine/request_usages/usage_record_list.h"
#include "engine/request_usages/compared_usage_record_list.h"
#include "messages/task_heartbeat_message.pb.h"
#include "messages/task_state_message.pb.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/flow_scheduler.h"
#include "storage/object_store_interface.h"
#include "storage/reference_types.h"
#include "storage/reference_utils.h"

DEFINE_uint64(heartbeat_interval, 1000000,
              "Heartbeat interval in microseconds.");

DEFINE_bool(enable_resource_reservation_decay, true, "Should decay task "
            "resource reservations during execution.");

DEFINE_bool(track_same_ec_task_resource_usage, false, "Should track resource "
            "usage of tasks, for searches by equivalence class");

DEFINE_bool(track_similar_resource_request_usage, false, "Should track "
            "resource usage of tasks, for searches by similar request");

DEFINE_uint64(resource_usage_percentile, 90, "What percentile of resource "
             "usage is safe for estimating other task resource usages");

DEFINE_int32(similar_resource_neighbour_count, 8, "Number of neighbour to "
             "consider, if tracking their resource usages");

DEFINE_double(similar_resource_error_bound, 0.1, "Error to permit in finding"
              "neighbour distance, if tracking their resource usages");

DEFINE_double(similar_resource_tree_rebuild_threshhold, 2, "Size of new tree"
              "relative to current, after which to rebuild the tree");

DEFINE_int32(similar_resource_max_tracked_tasks, 16384, "Max number of tasks"
             "to track the usage of, for similar request queries");

DEFINE_bool(track_similar_task_usage_timeslices, false, "Should use similar "
            "task timeslices to decay resource reservations");

DEFINE_int64(tracked_usage_fixed_timeslices, -1, "How many timeslices to "
             "assign each task, if you want this to be fixed.");

DEFINE_int64(burstiness_estimation_window_size, -1, "Coeff for exponential "
              "averaging new usage measurements, applied to new measurement");

DEFINE_double(burstiness_estimation_dropoff, 0.5, "Dropoff affecting how much "
              "additional burstiness decreases reliability of measurement");

DEFINE_double(task_similarity_request_distance_weight_dropoff, 0.05, "Dropoff "
              "affecting how much additional resource request distance "
              "decreases the similarity of tasks");

DEFINE_double(task_similarity_equiv_class_weight_dropoff, 0.5, "Dropoff "
              "affecting how much additional matching equivalence classes "
              "increases the similarity of tasks");

DEFINE_double(usage_averaging_coeff, -1, "Coefficient used to exponentially "
              "average new usage measurements, applied to new measurement");

DEFINE_int64(task_fail_timeout, 60, "Time (in seconds) after which to declare "
             "a task as failed if it has not sent heartbeats");

DEFINE_double(reservation_safety_margin, 0.25,
             "Safety margin value for updating task reservations..");

DEFINE_double(reservation_increment, 0.9,
             "Increment value for updating task reservations.");

DEFINE_double(reservation_overshoot_boost, 1.5,
             "Overshoot boost value for updating task reservations.");

namespace firmament {
namespace scheduler {

using executor::LocalExecutor;
using executor::RemoteExecutor;
using executor::SimulatedExecutor;
using store::ObjectStoreInterface;

EventDrivenScheduler::EventDrivenScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    ResourceTopologyNodeDescriptor* resource_topology,
    shared_ptr<ObjectStoreInterface> object_store,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    shared_ptr<TopologyManager> topo_mgr,
    MessagingAdapterInterface<BaseMessage>* m_adapter,
    SchedulingEventNotifierInterface* event_notifier,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri)
  : SchedulerInterface(job_map, knowledge_base, resource_map, resource_topology,
                       object_store, task_map),
      coordinator_uri_(coordinator_uri),
      coordinator_res_id_(coordinator_res_id),
      event_notifier_(event_notifier),
      m_adapter_ptr_(m_adapter),
      topology_manager_(topo_mgr),
      similar_resource_request_usages_(
          FLAGS_similar_resource_neighbour_count,
          FLAGS_similar_resource_error_bound,
          FLAGS_similar_resource_tree_rebuild_threshhold,
          FLAGS_similar_resource_max_tracked_tasks) {
  VLOG(1) << "EventDrivenScheduler initiated.";
}

EventDrivenScheduler::~EventDrivenScheduler() {
  for (map<ResourceID_t, ExecutorInterface*>::const_iterator
       exec_iter = executors_.begin();
       exec_iter != executors_.end();
       ++exec_iter) {
    delete exec_iter->second;
  }
  executors_.clear();
  // We don't delete event_notifier_ and m_adapter_ptr because they're owned by
  // the coordinator or the simulator_bridge.
}

void EventDrivenScheduler::AddJob(JobDescriptor* jd_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  InsertOrUpdate(&jobs_to_schedule_, JobIDFromString(jd_ptr->uuid()), jd_ptr);
}

void EventDrivenScheduler::BindTaskToResource(TaskDescriptor* td_ptr,
                                              ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  // Mark resource as busy and record task binding
  rd_ptr->set_state(ResourceDescriptor::RESOURCE_BUSY);
  rd_ptr->set_current_running_task(task_id);
  CHECK(InsertIfNotPresent(&task_bindings_, task_id, res_id));
  resource_bindings_.insert(pair<ResourceID_t, TaskID_t>(res_id, task_id));
}

ResourceID_t* EventDrivenScheduler::BoundResourceForTask(TaskID_t task_id) {
  ResourceID_t* rid = FindOrNull(task_bindings_, task_id);
  return rid;
}

vector<TaskID_t> EventDrivenScheduler::BoundTasksForResource(
  ResourceID_t res_id) {
  vector<TaskID_t> tasks;
  pair<multimap<ResourceID_t, TaskID_t>::iterator,
       multimap<ResourceID_t, TaskID_t>::iterator> range_it =
    resource_bindings_.equal_range(res_id);
  for (; range_it.first != range_it.second; range_it.first++) {
    tasks.push_back(range_it.first->second);
  }
  return tasks;
}

void EventDrivenScheduler::CheckRunningTasksHealth() {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  for (auto& executor : executors_) {
    vector<TaskID_t> failed_tasks;
    if (!executor.second->CheckRunningTasksHealth(&failed_tasks)) {
      // Handle task failures
      for (auto& failed_task : failed_tasks) {
        TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, failed_task);
        CHECK_NOTNULL(td_ptr);
        if (td_ptr->state() != TaskDescriptor::COMPLETED &&
            td_ptr->last_heartbeat_time() <=
            (GetCurrentTimestamp() - FLAGS_task_fail_timeout *
             SECONDS_TO_MICROSECONDS)) {
          LOG(INFO) << "Task " << td_ptr->uid() << " has not reported "
                    << "heartbeats for " << FLAGS_task_fail_timeout
                    << "s and its handler thread has exited. "
                    << "Declaring it FAILED!";
          executor.second->SendFailedMessage(td_ptr);
        }
      }
    }
  }
}

void EventDrivenScheduler::ClearScheduledJobs() {
  for (auto it = jobs_to_schedule_.begin(); it != jobs_to_schedule_.end(); ) {
    if (RunnableTasksForJob(it->second).size() == 0) {
      it = jobs_to_schedule_.erase(it);
    } else {
      it++;
    }
  }
}

void EventDrivenScheduler::DebugPrintRunnableTasks() {
  VLOG(1) << "Runnable task queue now contains " << runnable_tasks_.size()
          << " elements:";
  for (auto& task : runnable_tasks_) {
    VLOG(1) << "  " << task;
  }
}

void EventDrivenScheduler::DeregisterResource(ResourceID_t res_id) {
  VLOG(1) << "Removing executor for resource " << res_id
          << " which is now deregistered from this scheduler.";
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
  CHECK_NOTNULL(exec);
  // Terminate any running tasks on the resource.
  // TODO(ionel): Terminate the tasks running on res_id or any of
  // its sub-resources. Make sure the tasks get re-scheduled.
  // exec->TerminateAllTasks();
  // Remove the executor for the resource.
  CHECK(executors_.erase(res_id));
  delete exec;
  resource_bindings_.erase(res_id);
}

void EventDrivenScheduler::ExecuteTask(TaskDescriptor* td_ptr,
                                       ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());

  if (FLAGS_enable_resource_reservation_decay) {
    TaskReservationDecayData base_decay_data;
    CHECK(InsertIfNotPresent(&task_reservation_decay_data_, td_ptr->uid(),
                             base_decay_data));
    TaskReservationDecayData* decay_data = FindOrNull(
        task_reservation_decay_data_,
        td_ptr->uid());
    CHECK_NOTNULL(decay_data);

    // Initialize resource reservations
    ResourceVector* task_reservations =
        td_ptr->mutable_resource_reservations();
    task_reservations->CopyFrom(
        td_ptr->resource_request());
    if (td_ptr->similar_resource_request_usage_lists_size()
        && (FLAGS_track_same_ec_task_resource_usage
            || FLAGS_track_similar_resource_request_usage)) {
      vector<uint64_t> timeslice_durations_ms;
      for (uint32_t i = 0;
           i < static_cast<uint32_t>(
               td_ptr->similar_resource_request_usage_lists_size());
           ++i) {
        UsageList usage_list = td_ptr->similar_resource_request_usage_lists(i);
        timeslice_durations_ms.push_back(usage_list.timeslice_duration_ms());
      }
      CHECK_NOTNULL(decay_data);
      decay_data->median_timeslice_duration_ms =
          GetPercentile(timeslice_durations_ms, 50);

      ResourceVector usage_estimate;
      bool usage_estimated = EstimateTaskResourceUsageFromSimilarTasks(
          td_ptr, 0, ResourceIDFromString(rd_ptr->uuid()),
          &usage_estimate);
      if (usage_estimated) {
        CalculateReservationsFromUsage(usage_estimate,
                                       usage_estimate,
                                       td_ptr->resource_request(),
                                       FLAGS_reservation_increment,
                                       task_reservations);
      }
    }

    ResourceVector empty_resource_reservations;
    UpdateMachineReservations(res_id, &empty_resource_reservations,
                              task_reservations);
  }
  // Remove the task from the runnable set
  CHECK_EQ(runnable_tasks_.erase(task_id), 1)
    << "Failed to remove task " << task_id << " from runnable set!";
  if (VLOG_IS_ON(2))
    DebugPrintRunnableTasks();
  // Find an executor for this resource.
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
  CHECK_NOTNULL(exec);
  // Actually kick off the task
  // N.B. This is an asynchronous call, as the executor will spawn a thread.
  exec->RunTask(td_ptr, !td_ptr->inject_task_lib());
  // Mark task as running and report
  td_ptr->set_state(TaskDescriptor::RUNNING);
  td_ptr->set_scheduled_to_resource(rd_ptr->uuid());
  VLOG(1) << "Task " << task_id << " running.";
}

void EventDrivenScheduler::HandleJobCompletion(JobID_t job_id) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  JobDescriptor* jd = FindOrNull(*job_map_, job_id);
  CHECK_NOTNULL(jd);
  jd->set_state(JobDescriptor::COMPLETED);
  if (event_notifier_) {
    event_notifier_->OnJobCompletion(job_id);
  }
}

void EventDrivenScheduler::HandleReferenceStateChange(
    const ReferenceInterface& old_ref,
    const ReferenceInterface& new_ref,
    TaskDescriptor* td_ptr) {
  CHECK_EQ(old_ref.id(), new_ref.id());
  // Perform the appropriate actions for a reference changing status
  if (old_ref.Consumable() && new_ref.Consumable()) {
    // no change, return
    return;
  } else if (!old_ref.Consumable() && new_ref.Consumable()) {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    // something became available, unblock the waiting tasks
    set<TaskDescriptor*>* tasks = FindOrNull(reference_subscriptions_,
                                             old_ref.id());
    if (!tasks) {
      // Nobody cares about this ref, so we don't do anything
      return;
    }
    for (auto& task : *tasks) {
      CHECK_NOTNULL(task);
      bool any_outstanding = false;
      if (task->state() == TaskDescriptor::COMPLETED ||
          task->state() == TaskDescriptor::RUNNING)
        continue;
      for (auto& dependency : task->dependencies()) {
        set<ReferenceInterface*>* deps = object_store_->GetReferences(
            DataObjectIDFromProtobuf(dependency.id()));
        for (auto& dep : *deps) {
          if (!dep->Consumable())
            any_outstanding = true;
        }
      }
      if (!any_outstanding) {
        task->set_state(TaskDescriptor::RUNNABLE);
        runnable_tasks_.insert(task->uid());
      }
    }
  } else if (old_ref.Consumable() && !new_ref.Consumable()) {
    // failure or reference loss, re-run producing task(s)
    // TODO(malte): implement
  } else {
    // neither is consumable, so no scheduling implications
    return;
  }
}

void EventDrivenScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                                TaskFinalReport* report) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  ClearTaskResourceReservations(td_ptr->uid());
  // Find resource for task
  ResourceID_t* res_id_ptr = BoundResourceForTask(td_ptr->uid());
  CHECK_NOTNULL(res_id_ptr);
  // This copy is necessary because UnbindTaskFromResource ends up deleting the
  // ResourceID_t pointed to by res_id_ptr
  ResourceID_t res_id_tmp = *res_id_ptr;
  ResourceID_t machine_res_id_tmp = MachineResIDForResource(res_id_tmp);
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
  CHECK_NOTNULL(rs_ptr);
  VLOG(1) << "Handling completion of task " << td_ptr->uid()
          << ", freeing resource " << res_id_tmp;
  CHECK(UnbindTaskFromResource(td_ptr, res_id_tmp));
  // Record final report
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id_tmp);
  CHECK_NOTNULL(exec);
  exec->HandleTaskCompletion(td_ptr, report);
  if (!report->has_usage_list()) {
    const deque<TaskPerfStatisticsSample>* task_stats =
        knowledge_base()->GetStatsForTask(td_ptr->uid());
    if (task_stats && task_stats->size() > 0) {
      UsageList* usage_list = report->mutable_usage_list();

      if (FLAGS_track_similar_task_usage_timeslices) {
        if (FLAGS_tracked_usage_fixed_timeslices == -1) {
          usage_list->set_timeslice_duration_ms(
              FLAGS_heartbeat_interval / MILLISECONDS_TO_MICROSECONDS);
        } else {
          CHECK_GT(FLAGS_tracked_usage_fixed_timeslices, 0);
          usage_list->set_timeslice_duration_ms(
              (FLAGS_heartbeat_interval / MILLISECONDS_TO_MICROSECONDS)
              * task_stats->size()
              / FLAGS_tracked_usage_fixed_timeslices);
        }
      } else {
        usage_list->set_timeslice_duration_ms(0);
      }
      usage_list->set_machine_id(to_string(machine_res_id_tmp));

      // Create vector of task usages
      vector<TaskUsageRecord> usage_records;
      vector<TaskUsageRecord> valid_usage_records;
      for (auto& stats : *task_stats) {
        TaskUsageRecord new_record;
        // The first item in task stats always has resources
        if (stats.has_resources()) {
          new_record.set_is_valid(true);
          new_record.set_ram_cap(stats.resources().ram_cap());
          new_record.set_disk_bw(stats.resources().disk_bw());
          new_record.set_disk_cap(stats.resources().disk_cap());
        } else {
          new_record.set_is_valid(false);
          new_record.set_ram_cap(0);
          new_record.set_disk_bw(0);
          new_record.set_disk_cap(0);
        }
        usage_records.push_back(new_record);
        if (stats.has_resources()) {
          valid_usage_records.push_back(usage_records.back());
        }
      }

      // TODO(Josh): fix issue of indices referring to all records, but only
      // looking up valid (perhaps just shift the start, or spread them
      // proportionally to each vector's size) -- current solution is just to
      // set max to valid_usage_records size (assumes invalid are unlikely)

      // Indices of list of usages to add to the record
      vector<uint32_t> min_usage_indices;
      vector<uint32_t> max_usage_indices;
      if (FLAGS_track_similar_task_usage_timeslices) {
        if (FLAGS_tracked_usage_fixed_timeslices == -1) {
          for (uint32_t i = 0; i < valid_usage_records.size(); ++i) {
            min_usage_indices.push_back(i);
            max_usage_indices.push_back(i);
          }
        } else {
          CHECK_GT(FLAGS_tracked_usage_fixed_timeslices, 0);
          double record_timeslice_ratio =
              static_cast<double>(usage_records.size())
              / static_cast<double>(FLAGS_tracked_usage_fixed_timeslices);
          double absolute_max_index = valid_usage_records.size() - 1;
          for (uint32_t i = 0; i < FLAGS_tracked_usage_fixed_timeslices; ++i) {
            // The min and max indices corresponding to this timeslice
            min_usage_indices.push_back(
                min(round(i * record_timeslice_ratio),
                    absolute_max_index));
            max_usage_indices.push_back(
                min(round((i + 1) * record_timeslice_ratio),
                    absolute_max_index));
          }
        }
      } else if (valid_usage_records.size() > 0) {
        min_usage_indices.push_back(0);
        max_usage_indices.push_back(valid_usage_records.size() - 1);
      }

      // Look create a median record of each timeslice
      CHECK_EQ(min_usage_indices.size(), max_usage_indices.size());
      for (uint32_t i = 0; i < min_usage_indices.size(); ++i) {
        CHECK(valid_usage_records.size() > min_usage_indices[i]);
        CHECK(valid_usage_records.size() > max_usage_indices[i]);
        TaskUsageRecord median_record;
        GetPercentileTaskUsageRecord(valid_usage_records,
                                     min_usage_indices[i],
                                     max_usage_indices[i],
                                     FLAGS_resource_usage_percentile,
                                     &median_record);
        median_record.set_is_valid(true);
        usage_list->add_usage_records()->CopyFrom(median_record);
      }
    }
  }

  task_reservation_decay_data_.erase(td_ptr->uid());

  // Store the final report in the TD for future reference
  td_ptr->mutable_final_report()->CopyFrom(*report);
  if (event_notifier_) {
    event_notifier_->OnTaskCompletion(td_ptr, rs_ptr->mutable_descriptor());
  }
}

void EventDrivenScheduler::HandleTaskAbortion(TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Find resource for task
  ResourceID_t* res_id_ptr = BoundResourceForTask(td_ptr->uid());
  CHECK_NOTNULL(res_id_ptr);
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, *res_id_ptr);
  CHECK_NOTNULL(rs_ptr);
  VLOG(1) << "Handling abortion of task " << td_ptr->uid()
          << ", freeing resource " << *res_id_ptr;
  CHECK(UnbindTaskFromResource(td_ptr, *res_id_ptr));
  task_reservation_decay_data_.erase(td_ptr->uid());
  if (event_notifier_) {
    event_notifier_->OnTaskCompletion(td_ptr, rs_ptr->mutable_descriptor());
  }
}

void EventDrivenScheduler::HandleTaskDelegationFailure(
    TaskDescriptor* td_ptr) {
  {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Find the resource where the task was supposed to be delegated
  ResourceID_t* res_id_ptr = BoundResourceForTask(td_ptr->uid());
  CHECK_NOTNULL(res_id_ptr);
  CHECK(UnbindTaskFromResource(td_ptr, *res_id_ptr));
  task_reservation_decay_data_.erase(td_ptr->uid());
  }
  ClearTaskDescriptorSchedulingData(td_ptr);
  RescheduleTask(td_ptr);
}

void EventDrivenScheduler::HandleTaskEviction(TaskDescriptor* td_ptr,
                                              ResourceDescriptor* rd_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  ClearTaskResourceReservations(td_ptr->uid());
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  VLOG(1) << "Handling completion of task " << td_ptr->uid()
          << ", freeing resource " << res_id;
  CHECK(UnbindTaskFromResource(td_ptr, res_id));
  task_reservation_decay_data_.erase(td_ptr->uid());
  // Record final report
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
  td_ptr->set_state(TaskDescriptor::RUNNABLE);
  runnable_tasks_.insert(td_ptr->uid());
  CHECK_NOTNULL(exec);
  exec->HandleTaskEviction(td_ptr);
  if (event_notifier_) {
    event_notifier_->OnTaskEviction(td_ptr, rd_ptr);
  }
  ClearTaskDescriptorSchedulingData(td_ptr);
}

void EventDrivenScheduler::HandleTaskFailure(TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  ClearTaskResourceReservations(td_ptr->uid());
  // Find resource for task
  ResourceID_t* res_id_ptr = FindOrNull(task_bindings_, td_ptr->uid());
  CHECK_NOTNULL(res_id_ptr);
  // This copy is necessary because UnbindTaskFromResource ends up deleting the
  // ResourceID_t pointed to by res_id_ptr
  ResourceID_t res_id_tmp = *res_id_ptr;
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
  CHECK_NOTNULL(rs_ptr);
  VLOG(1) << "Handling failure of task " << td_ptr->uid()
          << ", freeing resource " << res_id_tmp;
  // TODO(malte): We should probably check if the resource has failed at this
  // point...
  // Executor cleanup: drop the task from the health checker's list, etc.
  ExecutorInterface* exec_ptr = FindPtrOrNull(executors_, res_id_tmp);
  CHECK_NOTNULL(exec_ptr);
  exec_ptr->HandleTaskFailure(td_ptr);
  // Remove the task's resource binding (as it is no longer currently bound)
  CHECK(UnbindTaskFromResource(td_ptr, res_id_tmp));
  task_reservation_decay_data_.erase(td_ptr->uid());
  // Set the task to "failed" state and deal with the consequences
  // (The state may already have been changed elsewhere, but since the failure
  // case can arise unexpectedly, we set it again here).
  td_ptr->set_state(TaskDescriptor::FAILED);
  // We only need to run the scheduler if the failed task was not delegated from
  // elsewhere, i.e. if it is managed by the local scheduler. If so, we kick the
  // scheduler if we haven't exceeded the retry limit.
  if (td_ptr->has_delegated_from()) {
    // XXX(malte): Need to forward message about task failure to delegator here!
  }
  if (event_notifier_) {
    event_notifier_->OnTaskFailure(td_ptr, rs_ptr->mutable_descriptor());
  }
}

void EventDrivenScheduler::HandleTaskFinalReport(const TaskFinalReport& report,
                                                 TaskDescriptor* td_ptr) {
  CHECK_NOTNULL(td_ptr);
  VLOG(1) << "Handling task final report for " << report.task_id();
  // Add the report to the TD if the task is not local (otherwise, the
  // scheduler has already done so)
  if (td_ptr->has_delegated_to()) {
    td_ptr->mutable_final_report()->CopyFrom(report);
  }
}

void EventDrivenScheduler::HandleTaskMigration(TaskDescriptor* td_ptr,
                                               ResourceDescriptor* rd_ptr) {
  CHECK_NOTNULL(td_ptr);
  CHECK_NOTNULL(rd_ptr);
  VLOG(1) << "Migrating task " << td_ptr->uid() << " to resource "
          << rd_ptr->uuid();
  rd_ptr->set_state(ResourceDescriptor::RESOURCE_BUSY);
  td_ptr->set_state(TaskDescriptor::RUNNING);
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t* old_res_id_ptr = FindOrNull(task_bindings_, task_id);
  CHECK_NOTNULL(old_res_id_ptr);
  // XXX(ionel): Assumes only one task per resource.
  ResourceStatus* old_rs = FindPtrOrNull(*resource_map_, *old_res_id_ptr);
  CHECK_NOTNULL(old_rs);
  ResourceDescriptor* old_rd = old_rs->mutable_descriptor();
  old_rd->set_state(ResourceDescriptor::RESOURCE_IDLE);
  rd_ptr->set_current_running_task(task_id);
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  InsertOrUpdate(&task_bindings_, task_id, res_id);
  if (event_notifier_) {
    event_notifier_->OnTaskMigration(td_ptr, rd_ptr);
  }
}

void EventDrivenScheduler::HandleTaskPlacement(
    TaskDescriptor* td_ptr,
    ResourceDescriptor* rd_ptr) {
  CHECK_NOTNULL(td_ptr);
  CHECK_NOTNULL(rd_ptr);
  TaskID_t task_id = td_ptr->uid();
  VLOG(1) << "Placing task " << task_id << " on resource " << rd_ptr->uuid();
  BindTaskToResource(td_ptr, rd_ptr);
  // Tag the job to which this task belongs as running
  JobDescriptor* jd =
    FindOrNull(*job_map_, JobIDFromString(td_ptr->job_id()));
  if (jd && jd->state() != JobDescriptor::RUNNING) {
    jd->set_state(JobDescriptor::RUNNING);
  }
  ExecuteTask(td_ptr, rd_ptr);
  if (event_notifier_) {
    event_notifier_->OnTaskPlacement(td_ptr, rd_ptr);
  }
}

void EventDrivenScheduler::KillRunningTask(
    TaskID_t task_id,
    TaskKillMessage::TaskKillReason reason) {
  // Check if this task is managed by this coordinator
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  if (!td_ptr) {
    LOG(ERROR) << "Tried to kill unknown task " << task_id;
    return;
  }
  // Check if we have a bound resource for the task and if it is marked as
  // running
  ResourceID_t* rid = BoundResourceForTask(task_id);
  if (td_ptr->state() != TaskDescriptor::RUNNING || !rid) {
    LOG(ERROR) << "Task " << task_id << " is not running locally, "
               << "so cannot kill it!";
    return;
  }
  // Find the executor for this task
  ExecutorInterface* exec = FindPtrOrNull(executors_, *rid);
  CHECK_NOTNULL(exec);

  // Kill the task on the executor
  exec->SendAbortMessage(td_ptr);
  // Clear up task data (rescheduling may prevent this)
  exec->HandleTaskFailure(td_ptr);

  td_ptr->set_state(TaskDescriptor::ABORTING);

  ClearTaskResourceReservations(task_id);
}

void EventDrivenScheduler::RescheduleTask(
    TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Go back to try scheduling this task again
  td_ptr->set_state(TaskDescriptor::RUNNABLE);
  runnable_tasks_.insert(td_ptr->uid());
  td_ptr->clear_start_time();
  td_ptr->clear_finish_time();
  td_ptr->clear_scheduled_to_resource();
  td_ptr->clear_resource_reservations();
  JobDescriptor* jd = FindOrNull(*job_map_, JobIDFromString(td_ptr->job_id()));
  CHECK_NOTNULL(jd);
  // Try again to schedule...
  ScheduleJob(jd, NULL);
}

void EventDrivenScheduler::ClearTaskDescriptorSchedulingData(
    TaskDescriptor* td_ptr) {
  td_ptr->clear_delegated_to();
  td_ptr->clear_last_heartbeat_location();
  td_ptr->clear_last_heartbeat_time();
}

// Implementation of lazy graph reduction algorithm, as per p58, fig. 3.5 in
// Derek Murray's thesis on CIEL.
uint64_t EventDrivenScheduler::LazyGraphReduction(
    const set<DataObjectID_t*>& output_ids,
    TaskDescriptor* root_task,
    const JobID_t& job_id) {
  VLOG(2) << "Performing lazy graph reduction";
  // Local data structures
  deque<TaskDescriptor*> newly_active_tasks;
  bool do_schedule = false;
  // Add expected producer for object_id to queue, if the object reference is
  // not already concrete.
  VLOG(2) << "for a job with " << output_ids.size() << " outputs";
  for (auto& output_id : output_ids) {
    set<ReferenceInterface*> refs = ReferencesForID(*output_id);
    if (!refs.empty()) {
      for (auto& ref : refs) {
        // TODO(malte): this logic is very simple-minded; sometimes, it may be
        // beneficial to produce locally instead of fetching remotely!
        if (ref->Consumable() && !ref->desc().non_deterministic()) {
          // skip this output, as it is already present
          continue;
        }
      }
    }
    // otherwise, we add the producer for said output reference to the queue, if
    // it is not already scheduled.
    // N.B.: by this point, we know that no concrete reference exists in the set
    // of references available.
    set<TaskDescriptor*> tasks =
        ProducingTasksForDataObjectID(*output_id, job_id);
    CHECK_GT(tasks.size(), 0) << "Could not find task producing output ID "
                              << *output_id;
    for (auto& task : tasks) {
      if (task->state() == TaskDescriptor::CREATED ||
          task->state() == TaskDescriptor::FAILED) {
        VLOG(2) << "Setting task " << task->uid() << " active as it produces "
                << "output " << *output_id << ", which we're interested in.";
        task->set_state(TaskDescriptor::BLOCKING);
        newly_active_tasks.push_back(task);
      }
    }
  }
  // Add root task to queue
  TaskDescriptor* rtd_ptr = FindPtrOrNull(*task_map_, root_task->uid());
  CHECK_NOTNULL(rtd_ptr);
  // Only add the root task if it is not already scheduled, running, done
  // or failed.
  if (rtd_ptr->state() == TaskDescriptor::CREATED)
    newly_active_tasks.push_back(rtd_ptr);
  // Keep iterating over tasks as long as there are more to visit
  while (!newly_active_tasks.empty()) {
    TaskDescriptor* current_task = newly_active_tasks.front();
    VLOG(2) << "Next active task considered is " << current_task->uid();
    newly_active_tasks.pop_front();
    // Find any unfulfilled dependencies
    bool will_block = false;
    for (auto& dependency : current_task->dependencies()) {
      ReferenceInterface* ref = ReferenceFromDescriptor(dependency);
      // Subscribe the current task to the reference, to enable it to be
      // unblocked if it becomes available.
      // Note that we subscribe even tasks whose dependencies are concrete, as
      // they may later disappear and failures will be handled via the
      // subscription relationship.
      set<TaskDescriptor*>* subscribers = FindOrNull(
          reference_subscriptions_, ref->id());
      if (!subscribers) {
        InsertIfNotPresent(&reference_subscriptions_,
                           ref->id(), set<TaskDescriptor*>());
        subscribers = FindOrNull(reference_subscriptions_, ref->id());
      }
      subscribers->insert(current_task);
      // Now proceed to check if it is available
      if (ref->Consumable()) {
        // This input reference is consumable. So far, so good.
        VLOG(2) << "Task " << current_task->uid() << "'s dependency " << *ref
                << " is consumable.";
      } else {
        // This input reference is not consumable; set the task to block and
        // look at its predecessors (which may produce the necessary input, and
        // may be runnable).
        VLOG(2) << "Task " << current_task->uid()
                << " is blocking on reference " << *ref;
        will_block = true;
        // Look at predecessor task (producing this reference)
        set<TaskDescriptor*> producing_tasks =
            ProducingTasksForDataObjectID(ref->id(), job_id);
        if (producing_tasks.size() == 0) {
          LOG(ERROR) << "Failed to find producing task for ref " << ref
                     << "; will block until it is produced.";
          continue;
        }
        for (auto& task : producing_tasks) {
          if (task->state() == TaskDescriptor::CREATED ||
              task->state() == TaskDescriptor::COMPLETED) {
            task->set_state(TaskDescriptor::BLOCKING);
            newly_active_tasks.push_back(task);
          }
        }
      }
    }
    // Process any eager children not related via dependencies
    for (auto& child_task : *current_task->mutable_spawned()) {
      if (child_task.outputs_size() == 0)
        newly_active_tasks.push_back(&child_task);
    }
    if (!will_block || (current_task->dependencies_size() == 0
                        && current_task->outputs_size() == 0)) {
      // This task is runnable
      VLOG(2) << "Adding task " << current_task->uid() << " to RUNNABLE set.";
      current_task->set_state(TaskDescriptor::RUNNABLE);
      runnable_tasks_.insert(current_task->uid());
    }
  }
  VLOG(1) << "do_schedule is " << do_schedule << ", runnable_task set "
          << "contains " << runnable_tasks_.size() << " tasks.";
  return runnable_tasks_.size();
}

bool EventDrivenScheduler::PlaceDelegatedTask(TaskDescriptor* td,
                                              ResourceID_t target_resource) {
  // Check if the resource is available
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, target_resource);
  // Do we know about this resource?
  if (!rs_ptr) {
    // Requested resource unknown or does not exist any more
    LOG(WARNING) << "Attempted to place delegated task " << td->uid()
                 << " on resource " << target_resource << ", which is "
                 << "unknown!";
    return false;
  }
  ResourceDescriptor* rd = rs_ptr->mutable_descriptor();
  CHECK_NOTNULL(rd);
  // Is the resource still idle?
  if (rd->state() != ResourceDescriptor::RESOURCE_IDLE) {
    // Resource is no longer idle
    LOG(WARNING) << "Attempted to place delegated task " << td->uid()
                 << " on resource " << target_resource << ", which is "
                 << "not idle!";
    return false;
  }
  // Otherwise, bind the task
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  runnable_tasks_.insert(td->uid());
  InsertIfNotPresent(task_map_.get(), td->uid(), td);
  HandleTaskPlacement(td, rd);
  td->set_state(TaskDescriptor::RUNNING);
  return true;
}

set<TaskDescriptor*> EventDrivenScheduler::ProducingTasksForDataObjectID(
    const DataObjectID_t& id, const JobID_t& cur_job) {
  // Find all producing tasks for an object ID, as indicated by the references
  // stored locally.
  set<TaskDescriptor*> producing_tasks;
  VLOG(2) << "Looking up producing task for object " << id;
  set<ReferenceInterface*>* refs = object_store_->GetReferences(id);
  if (!refs)
    return producing_tasks;
  for (auto& ref : *refs) {
    if (ref->desc().has_producing_task()) {
      TaskDescriptor* td_ptr =
        FindPtrOrNull(*task_map_, ref->desc().producing_task());
      if (td_ptr) {
        if (JobIDFromString(td_ptr->job_id()) == cur_job) {
          VLOG(2) << "... is " << td_ptr->uid() << " (in THIS job).";
          producing_tasks.insert(td_ptr);
        } else {
          VLOG(2) << "... is " << td_ptr->uid() << " (in job "
                  << td_ptr->job_id() << ").";
          // Someone in another job is producing this object. We have a choice
          // of making him runnable, or ignoring him.
          // We do the former if the reference is public, and the latter if it
          // is private.
          if (ref->desc().scope() == ReferenceDescriptor::PUBLIC)
            producing_tasks.insert(td_ptr);
        }
      } else {
        VLOG(2) << "... NOT FOUND";
      }
    }
  }
  return producing_tasks;
}

const set<ReferenceInterface*> EventDrivenScheduler::ReferencesForID(
    const DataObjectID_t& id) {
  // Find all locally known references for a specific object
  VLOG(2) << "Looking up object " << id;
  set<ReferenceInterface*>* ref_set = object_store_->GetReferences(id);
  if (!ref_set) {
    VLOG(2) << "... NOT FOUND";
    set<ReferenceInterface*> es;  // empty set
    return es;
  } else {
    VLOG(2) << " ... FOUND, " << ref_set->size() << " references.";
    // Return the unfiltered set of all known references to this name
    return *ref_set;
  }
}

void EventDrivenScheduler::RegisterLocalResource(ResourceID_t res_id) {
  // Create an executor for each resource.
  VLOG(1) << "Adding executor for local resource " << res_id;
  LocalExecutor* exec = new LocalExecutor(res_id, coordinator_uri_,
                                          topology_manager_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

// Simple 2-argument wrapper
void EventDrivenScheduler::RegisterResource(ResourceID_t res_id,
                                            bool local,
                                            bool simulated) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  if (local) {
    RegisterLocalResource(res_id);
  } else if (simulated) {
    RegisterSimulatedResource(res_id);
  } else {
    RegisterRemoteResource(res_id);
  }
}

void EventDrivenScheduler::RegisterRemoteResource(ResourceID_t res_id) {
  // Create an executor for each resource.
  VLOG(1) << "Adding executor for remote resource " << res_id;
  RemoteExecutor* exec = new RemoteExecutor(res_id, coordinator_res_id_,
                                            coordinator_uri_,
                                            resource_map_.get(),
                                            m_adapter_ptr_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

void EventDrivenScheduler::RegisterSimulatedResource(ResourceID_t res_id) {
  VLOG(1) << "Adding executor for simulated resource " << res_id;
  SimulatedExecutor* exec = new SimulatedExecutor(res_id, coordinator_uri_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

const set<TaskID_t>& EventDrivenScheduler::RunnableTasksForJob(
    JobDescriptor* job_desc) {
  // TODO(malte): check if this is broken
  set<DataObjectID_t*> outputs =
      DataObjectIDsFromProtobuf(job_desc->output_ids());
  TaskDescriptor* rtp = job_desc->mutable_root_task();
  LazyGraphReduction(outputs, rtp, JobIDFromString(job_desc->uuid()));
  return runnable_tasks_;
}

bool EventDrivenScheduler::UnbindTaskFromResource(TaskDescriptor* td_ptr,
                                                  ResourceID_t res_id) {
  TaskID_t task_id = td_ptr->uid();
  // Set the bound resource idle again.
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs_ptr);
  // XXX(ionel): Assumes only one task is running per resource.
  rs_ptr->mutable_descriptor()->set_state(ResourceDescriptor::RESOURCE_IDLE);
  rs_ptr->mutable_descriptor()->clear_current_running_task();
  ResourceID_t* res_id_ptr = FindOrNull(task_bindings_, task_id);
  if (res_id_ptr) {
    pair<multimap<ResourceID_t, TaskID_t>::iterator,
         multimap<ResourceID_t, TaskID_t>::iterator> range_it =
      resource_bindings_.equal_range(*res_id_ptr);
    for (; range_it.first != range_it.second; range_it.first++) {
      if (range_it.first->second == task_id) {
        // We've found the element.
        resource_bindings_.erase(range_it.first);
        break;
      }
    }
    return task_bindings_.erase(task_id) == 1;
  } else {
    return false;
  }
}

bool EventDrivenScheduler::EstimateTaskResourceUsageFromSimilarTasks(
    TaskDescriptor* td_ptr,
    uint32_t timeslice_index,
    ResourceID_t scheduled_resource,
    ResourceVector* usage_estimate) {
  vector<ResourceVector> similar_task_vectors;
  vector<double> similar_task_weights;
  bool valid_records_exist = false;
  double base_weight = 1;
  double same_machine_weight = 1;

  ResourceID_t machine_res_id = MachineResIDForResource(scheduled_resource);

  uint32_t number_of_usage_lists = static_cast<uint32_t>(
      td_ptr->similar_resource_request_usage_lists_size());
  for (uint32_t i = 0; i < number_of_usage_lists; ++i) {
    UsageList usage_list = td_ptr->similar_resource_request_usage_lists(i);
    uint32_t number_of_usage_records = static_cast<uint32_t>(
        usage_list.usage_records_size());
    if (number_of_usage_records > timeslice_index) {
      TaskUsageRecord usage_record = usage_list.usage_records(timeslice_index);
      ResourceVector task_vector;
      task_vector.set_ram_cap(usage_record.ram_cap());
      task_vector.set_disk_bw(usage_record.disk_bw());
      task_vector.set_disk_cap(usage_record.disk_cap());
      similar_task_vectors.push_back(task_vector);
      similar_task_weights.push_back(0);
      if (usage_record.is_valid()) {
        valid_records_exist = true;

        similar_task_weights[i] += base_weight;
        if (usage_list.has_distance()) {
          // Use exponential decay to achieve max weight of 1 at 0
          similar_task_weights[i] +=
              exp(-1
                  * FLAGS_task_similarity_request_distance_weight_dropoff
                  * usage_list.distance());
        }
        if (machine_res_id == ResourceIDFromString(usage_list.machine_id())) {
          similar_task_weights[i] += same_machine_weight;
        }

        // Use logistic function to achieve max weight of 1
        similar_task_weights[i] += ScaleToLogistic(
            FLAGS_task_similarity_equiv_class_weight_dropoff,
            usage_list.matched_equivalence_classes());
      }
    }
  }

  if (valid_records_exist) {
    DetermineWeightedAverage(similar_task_vectors, similar_task_weights,
                             usage_estimate);
  }
  return valid_records_exist;
}

double EventDrivenScheduler::ScaleToLogistic(double dropoff, double value) {
  return (2 / (1 + exp(-1 * dropoff * value))) - 1;
}

void EventDrivenScheduler::DetermineWeightedAverage(
    const vector<ResourceVector>& resource_vectors,
    const vector<double>& weights,
    ResourceVector* weighted_average) {
  CHECK_EQ(resource_vectors.size(), weights.size());
  double total_weights = 0;

  for (auto& weight : weights) {
    total_weights += weight;
  }

  double sum_ram_cap = 0;
  double sum_disk_bw = 0;
  double sum_disk_cap = 0;

  for (uint32_t i = 0; i < resource_vectors.size(); ++i) {
    double record_weight = weights[i] / total_weights;
    sum_ram_cap += resource_vectors[i].ram_cap() * record_weight;
    sum_disk_bw += resource_vectors[i].disk_bw() * record_weight;
    sum_disk_cap += resource_vectors[i].disk_cap() * record_weight;
  }

  weighted_average->set_ram_cap(sum_ram_cap);
  weighted_average->set_disk_bw(sum_disk_bw);
  weighted_average->set_disk_cap(sum_disk_cap);
}

void EventDrivenScheduler::CalculateReservationsFromUsage(
      const ResourceVector& usage,
      const ResourceVector& safe_usage,
      const ResourceVector& limit,
      double reservation_increment,
      ResourceVector* reservations) {
  if (usage.has_ram_cap()) {
    uint64_t usage_ram = usage.ram_cap();
    uint64_t safe_usage_ram = safe_usage.ram_cap();
    uint64_t current_reservation_ram = reservations->ram_cap();
    uint64_t safety_ram =
        floor((1 + FLAGS_reservation_safety_margin) * safe_usage_ram);
    uint64_t updated_reservation_ram =
        (usage_ram > current_reservation_ram)
            ? usage_ram * FLAGS_reservation_overshoot_boost
            : current_reservation_ram * reservation_increment;
    updated_reservation_ram = min(max(updated_reservation_ram,
                                      safety_ram),
                                  limit.ram_cap());
    reservations->set_ram_cap(updated_reservation_ram);
  }

  if (usage.has_disk_bw()) {
    uint64_t usage_disk_bw = usage.disk_bw();
    uint64_t safe_usage_disk_bw = safe_usage.disk_bw();
    uint64_t current_reservation_disk_bw = reservations->disk_bw();
    uint64_t safety_disk_bw =
        floor((1 + FLAGS_reservation_safety_margin) * safe_usage_disk_bw);
    uint64_t updated_reservation_disk_bw =
        (usage_disk_bw > current_reservation_disk_bw)
            ? usage_disk_bw * FLAGS_reservation_overshoot_boost
            : current_reservation_disk_bw * reservation_increment;
    updated_reservation_disk_bw = min(max(updated_reservation_disk_bw,
                                          safety_disk_bw),
                                      limit.disk_bw());
    reservations->set_disk_bw(updated_reservation_disk_bw);
  }

  if (usage.has_disk_cap()) {
    uint64_t usage_disk_cap = usage.disk_cap();
    uint64_t safe_usage_disk_cap = safe_usage.disk_cap();
    uint64_t current_reservation_disk_cap = reservations->disk_cap();
    uint64_t safety_disk_cap =
        floor((1 + FLAGS_reservation_safety_margin) * safe_usage_disk_cap);
    uint64_t updated_reservation_disk_cap =
        (usage_disk_cap > current_reservation_disk_cap)
            ? usage_disk_cap * FLAGS_reservation_overshoot_boost
            : current_reservation_disk_cap * reservation_increment;
    updated_reservation_disk_cap = min(max(updated_reservation_disk_cap,
                                          safety_disk_cap),
                                      limit.disk_cap());
    reservations->set_disk_cap(updated_reservation_disk_cap);
  }
}

void EventDrivenScheduler::UpdateTaskResourceReservations() {
  if (!FLAGS_enable_resource_reservation_decay) return;

  for (thread_safe::map<TaskID_t, TaskDescriptor*>::iterator it
           = task_map_->begin();
       it != task_map_->end(); it++) {
    TaskDescriptor* td_ptr = it->second;
    TaskID_t task_id = it->first;

    if (td_ptr->state() == TaskDescriptor::RUNNING
        && !td_ptr->has_delegated_to()
        && td_ptr->has_scheduled_to_resource()) {
      ResourceID_t task_scheduled_res_id =
          ResourceIDFromString(td_ptr->scheduled_to_resource());
      const TaskPerfStatisticsSample* latest_stats =
          knowledge_base()->GetLatestStatsForTask(task_id);
      const deque<TaskPerfStatisticsSample>* full_stats =
          knowledge_base()->GetStatsForTask(task_id);
      uint32_t number_of_stats = full_stats ? full_stats->size() : 0;
      TaskReservationDecayData* decay_data = FindOrNull(
        task_reservation_decay_data_,
        td_ptr->uid());
      CHECK_NOTNULL(decay_data);
      ResourceVector* reservations = td_ptr->mutable_resource_reservations();

      if (reservations && latest_stats && latest_stats->has_resources()) {
        const ResourceVector limit = td_ptr->resource_request();
        const ResourceVector measured_usage = latest_stats->resources();
        if (measured_usage.ram_cap() > limit.ram_cap()
            || measured_usage.disk_bw() > limit.disk_bw()
            || measured_usage.disk_cap() > limit.disk_cap()) {
          ExecutorInterface* exec = FindPtrOrNull(executors_,
                                                  task_scheduled_res_id);
          CHECK_NOTNULL(exec);
          exec->SendAbortMessage(td_ptr);
        } else {
          if (decay_data->usage_measured) {
            DetermineCurrentTaskUsage(measured_usage,
                                      decay_data->last_usage_calculated,
                                      &decay_data->last_usage_calculated);
          } else {
            decay_data->last_usage_calculated.CopyFrom(measured_usage);
            decay_data->usage_measured = true;
          }

          ResourceVector next_timeslice_usage(decay_data->last_usage_calculated);

          // Use the timeslice to estimate next_timeslice_usage better
          if (FLAGS_track_similar_task_usage_timeslices) {
            CHECK(FLAGS_track_same_ec_task_resource_usage
                  || FLAGS_track_similar_resource_request_usage);
            uint32_t timeslice;
            if (FLAGS_tracked_usage_fixed_timeslices == -1) {
              // This is the next timeslice, since timeslices are 0-indexed
              timeslice = number_of_stats;
            } else {
              CHECK_GT(FLAGS_tracked_usage_fixed_timeslices, 0);

              // TODO(Josh): should perhaps add health monitor check frequency
              timeslice = FLAGS_tracked_usage_fixed_timeslices;

              if (decay_data->median_timeslice_duration_ms != 0) {
                int64_t next_timeslice_index_estimate =
                    number_of_stats
                    * (FLAGS_heartbeat_interval / MILLISECONDS_TO_MICROSECONDS)
                    / decay_data->median_timeslice_duration_ms;
                timeslice = min(next_timeslice_index_estimate,
                                      FLAGS_tracked_usage_fixed_timeslices);
              }
            }

            ResourceVector usage_estimate;
            bool usage_estimated = EstimateTaskResourceUsageFromSimilarTasks(
                td_ptr, timeslice, task_scheduled_res_id, &usage_estimate);

            if (usage_estimated) {
              // TODO(Josh): Set accuracy rating by comparing previous estimate
              // with current usage, if a flag is set (an extension). Probably want
              // to use the proper measured_usage
              // TODO(Josh): Could calculate rating in each resource type dimension
              // alternatively, can just use the min accuracy rating of all of them
              double accuracy_rating = 0.5;

              DetermineWeightedAverage({decay_data->last_usage_calculated,
                                        usage_estimate},
                                       {accuracy_rating, 1 - accuracy_rating},
                                       &next_timeslice_usage);
            }
          }

          double reservation_increment = FLAGS_reservation_increment;
          if (FLAGS_burstiness_estimation_window_size > 0) {
            // TODO(Josh): consider just multiplying by the coeff
            reservation_increment = 
                (reservation_increment
                 + DetermineTaskBurstinessCoeff(full_stats)) / 2;
          }

          ResourceVector old_reservations(td_ptr->resource_reservations());
          CalculateReservationsFromUsage(next_timeslice_usage,
                                         measured_usage,
                                         limit,
                                         reservation_increment,
                                         reservations);

          UpdateMachineReservations(task_scheduled_res_id,
                                    &old_reservations,
                                    reservations);
        }

      }
    }
  }
}

double EventDrivenScheduler::DetermineTaskBurstinessCoeff(
    const deque<TaskPerfStatisticsSample>* stats) {
  CHECK_GT(FLAGS_burstiness_estimation_window_size, 0);
  uint64_t window_size = FLAGS_burstiness_estimation_window_size;

  vector<ResourceVector> usages;
  for (deque<TaskPerfStatisticsSample>::const_reverse_iterator it =
           stats->crbegin();
       it != stats->crend()
       && usages.size() < window_size;
       ++it) {
    if (it->has_resources()) {
      usages.push_back(it->resources());
    }
  }

  if (usages.size() != window_size) return 1;

  // Calculate sums of usages
  uint64_t sum_ram_cap = 0;
  uint64_t sum_disk_bw = 0;
  uint64_t sum_disk_cap = 0;
  for (auto& usage : usages) {
    sum_ram_cap += usage.ram_cap();
    sum_disk_bw += usage.disk_bw();
    sum_disk_cap += usage.disk_cap();
  }

  // Calculate mean usages
  double mean_ram_cap = sum_ram_cap / usages.size();
  double mean_disk_bw = sum_disk_bw / usages.size();
  double mean_disk_cap = sum_disk_cap / usages.size();

  // Calculate variances of usages
  double var_ram_cap = 0;
  double var_disk_bw = 0;
  double var_disk_cap = 0;
  for (auto& usage : usages) {
      var_ram_cap += pow((usage.ram_cap() - mean_ram_cap), 2);
      var_disk_bw += pow((usage.disk_bw() - mean_disk_bw), 2);
      var_disk_cap += pow((usage.disk_cap() - mean_disk_cap), 2);
  }
  var_ram_cap /= window_size;
  var_disk_bw /= window_size;
  var_disk_cap /= window_size;

  // Calculate Fano factor of usages
  double fano_ram_cap = var_ram_cap / mean_ram_cap;
  double fano_disk_bw = var_disk_bw / mean_disk_bw;
  double fano_disk_cap = var_disk_cap / mean_disk_cap;

  // Estimate burtiness coefficient for each resource
  double burst_ram_cap =
      ScaleToLogistic(FLAGS_burstiness_estimation_dropoff, fano_ram_cap);
  double burst_disk_bw =
      ScaleToLogistic(FLAGS_burstiness_estimation_dropoff, fano_disk_bw);
  double burst_disk_cap =
      ScaleToLogistic(FLAGS_burstiness_estimation_dropoff, fano_disk_cap);

  // Return the max burstiness coefficient
  return max(burst_ram_cap, max(burst_disk_bw, burst_disk_cap));
}

void EventDrivenScheduler::DetermineCurrentTaskUsage(
    const ResourceVector& measured_usage,
    const ResourceVector& prev_usage,
    ResourceVector* current_usage) {
  if (FLAGS_usage_averaging_coeff == -1
      || !FLAGS_track_similar_resource_request_usage) {
    current_usage->CopyFrom(measured_usage);
  } else {
    CHECK_GT(FLAGS_usage_averaging_coeff, 0);
    DetermineWeightedAverage(
        {measured_usage, prev_usage},
        {FLAGS_usage_averaging_coeff, 1 - FLAGS_usage_averaging_coeff},
        current_usage);
  }
}

void EventDrivenScheduler::ClearTaskResourceReservations(TaskID_t task_id) {
  if (!FLAGS_enable_resource_reservation_decay) {
    TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
    CHECK_NOTNULL(td);
    ResourceID_t* res_id_ptr = BoundResourceForTask(task_id);
    CHECK_NOTNULL(res_id_ptr);
    ResourceVector empty_resource_reservations;
    UpdateMachineReservations(*res_id_ptr, &(td->resource_reservations()),
                              &empty_resource_reservations);
    td->clear_resource_reservations();
  }
}

void EventDrivenScheduler::UpdateMachineReservations(
    ResourceID_t res_id,
    const ResourceVector* old_reservations,
    const ResourceVector* new_reservations) {
  uint64_t old_reservation_ram = old_reservations->ram_cap();
  uint64_t new_reservation_ram = new_reservations->ram_cap();
  ResourceID_t machine_res_id = MachineResIDForResource(res_id);
  const ResourceVector* old_machine_reservations =
      knowledge_base()->GetMachineReservations(machine_res_id);
  CHECK_NOTNULL(old_machine_reservations);
  ResourceVector new_machine_reservations;
  uint64_t new_machine_ram_reservation = new_reservation_ram +
      old_machine_reservations->ram_cap() - old_reservation_ram;
  new_machine_reservations.set_ram_cap(new_machine_ram_reservation);
  knowledge_base()->UpdateMachineReservations(machine_res_id,
                                              new_machine_reservations);
}

void EventDrivenScheduler::GetPercentileTaskUsageRecord(
    const vector<TaskUsageRecord>& usage_records,
    uint32_t min_index, uint32_t max_index, uint64_t percentile,
    TaskUsageRecord* percentile_record) {
  auto ram_cap_comp = [](const TaskUsageRecord& first,
                         const TaskUsageRecord& second) {
    return first.ram_cap() < second.ram_cap();
  };
  TaskUsageRecord percentile_ram_cap_record =
      GetPercentile(usage_records, percentile, ram_cap_comp,
                    min_index, max_index);
  percentile_record->set_ram_cap(percentile_ram_cap_record.ram_cap());

  auto disk_bw_comp = [](const TaskUsageRecord& first,
                         const TaskUsageRecord& second) {
    return first.disk_bw() < second.disk_bw();
  };
  TaskUsageRecord percentile_disk_bw_record =
      GetPercentile(usage_records, percentile, disk_bw_comp,
                    min_index, max_index);
  percentile_record->set_disk_bw(percentile_disk_bw_record.disk_bw());

  auto disk_cap_comp = [](const TaskUsageRecord& first,
                         const TaskUsageRecord& second) {
    return first.disk_cap() < second.disk_cap();
  };
  TaskUsageRecord percentile_disk_cap_record =
      GetPercentile(usage_records, percentile, disk_cap_comp,
                    min_index, max_index);
  percentile_record->set_disk_cap(percentile_disk_cap_record.disk_cap());
}

ResourceID_t EventDrivenScheduler::MachineResIDForResource(
    ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  while (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_MACHINE) {
    if (!rtnd->has_parent_id()) {
      VLOG(2) << "Non-machine resource " << rtnd->resource_desc().uuid()
              << " has no parent!";
      return ResourceID_t();
    }
    rs = FindPtrOrNull(*resource_map_, ResourceIDFromString(rtnd->parent_id()));
    rtnd = rs->mutable_topology_node();
  }
  return ResourceIDFromString(rtnd->resource_desc().uuid());
}

vector<TaskHeartbeatMessage> EventDrivenScheduler::CreateTaskHeartbeats() {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  vector<TaskHeartbeatMessage> task_heartbeats;
  for (auto& executor : executors_) {
    executor.second->CreateTaskHeartbeats(&task_heartbeats);
  }

  return task_heartbeats;
}

vector<TaskStateMessage> EventDrivenScheduler::CreateTaskStateChanges() {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  vector<TaskStateMessage> task_state_changes;
  for (auto& executor : executors_) {
    executor.second->CreateTaskStateChanges(&task_state_changes);
  }

  return task_state_changes;
}

}  // namespace scheduler
}  // namespace firmament

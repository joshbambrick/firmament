// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// General abstract superclass for event-driven schedulers.

#ifndef FIRMAMENT_SCHEDULING_EVENT_DRIVEN_SCHEDULER_H
#define FIRMAMENT_SCHEDULING_EVENT_DRIVEN_SCHEDULER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "base/task_final_report.pb.h"
#include "base/resource_vector_double.pb.h"
#include "engine/executors/executor_interface.h"
#include "engine/request_usages/request_usages.h"
#include "misc/messaging_interface.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/scheduling_event_notifier_interface.h"
#include "scheduling/task_reservation_decay_data.h"
#include "storage/reference_interface.h"

namespace firmament {
namespace scheduler {

using executor::ExecutorInterface;

class EventDrivenScheduler : public SchedulerInterface {
 public:
  EventDrivenScheduler(shared_ptr<JobMap_t> job_map,
                       shared_ptr<ResourceMap_t> resource_map,
                       ResourceTopologyNodeDescriptor* resource_topology,
                       shared_ptr<store::ObjectStoreInterface> object_store,
                       shared_ptr<TaskMap_t> task_map,
                       shared_ptr<KnowledgeBase> knowledge_base,
                       shared_ptr<TopologyManager> topo_mgr,
                       MessagingAdapterInterface<BaseMessage>* m_adapter,
                       SchedulingEventNotifierInterface* event_notifier,
                       ResourceID_t coordinator_res_id,
                       const string& coordinator_uri);
  ~EventDrivenScheduler();
  virtual void AddJob(JobDescriptor* jd_ptr);
  ResourceID_t* BoundResourceForTask(TaskID_t task_id);
  vector<TaskID_t> BoundTasksForResource(ResourceID_t res_id);
  ResourceID_t MachineResIDForResource(ResourceID_t res_id);
  void CheckRunningTasksHealth();
  virtual void DeregisterResource(ResourceID_t res_id);
  virtual void HandleJobCompletion(JobID_t job_id);
  virtual void HandleReferenceStateChange(const ReferenceInterface& old_ref,
                                          const ReferenceInterface& new_ref,
                                          TaskDescriptor* td_ptr);
  virtual void HandleTaskCompletion(TaskDescriptor* td_ptr,
                                    TaskFinalReport* report);
  virtual void HandleTaskAbortion(TaskDescriptor* td_ptr);
  virtual void HandleTaskDelegationFailure(TaskDescriptor* td_ptr);
  virtual void HandleTaskEviction(TaskDescriptor* td_ptr,
                                  ResourceDescriptor* rd_ptr);
  virtual void HandleTaskFailure(TaskDescriptor* td_ptr);
  virtual void HandleTaskFinalReport(const TaskFinalReport& report,
                                     TaskDescriptor* td_ptr);
  virtual void KillRunningTask(TaskID_t task_id,
                               TaskKillMessage::TaskKillReason reason);
  virtual void ClearTaskDescriptorSchedulingData(TaskDescriptor* td_ptr);
  virtual void RescheduleTask(TaskDescriptor* td_ptr);
  bool PlaceDelegatedTask(TaskDescriptor* td, ResourceID_t target_resource);
  virtual void RegisterResource(ResourceID_t res_id,
                                bool local,
                                bool simulated);
  // N.B. ScheduleJob must be implemented in scheduler-specific logic
  virtual uint64_t ScheduleAllJobs(SchedulerStats* scheduler_stats) = 0;
  virtual uint64_t ScheduleJob(JobDescriptor* jd_ptr,
                               SchedulerStats* scheduler_stats) = 0;
  virtual uint64_t ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                                SchedulerStats* scheduler_stats) = 0;
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<EventDrivenScheduler>";
  }
  virtual void UpdateTaskResourceReservations();
  vector<TaskHeartbeatMessage> CreateTaskHeartbeats();
  vector<TaskStateMessage> CreateTaskStateChanges();

 protected:
  FRIEND_TEST(SimpleSchedulerTest, FindRunnableTasksForJob);
  FRIEND_TEST(SimpleSchedulerTest, FindRunnableTasksForComplexJob);
  FRIEND_TEST(SimpleSchedulerTest, FindRunnableTasksForComplexJob2);
  void BindTaskToResource(TaskDescriptor* td_ptr, ResourceDescriptor* rd_ptr);
  void ClearScheduledJobs();
  void DebugPrintRunnableTasks();
  void ExecuteTask(TaskDescriptor* td_ptr, ResourceDescriptor* rd_ptr);
  virtual void HandleTaskMigration(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr);
  virtual void HandleTaskPlacement(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr);
  uint64_t LazyGraphReduction(const set<DataObjectID_t*>& output_ids,
                              TaskDescriptor* root_task,
                              const JobID_t& job_id);
  set<TaskDescriptor*> ProducingTasksForDataObjectID(const DataObjectID_t& id,
                                                     const JobID_t& cur_job);
  const set<ReferenceInterface*> ReferencesForID(const DataObjectID_t& id);
  void RegisterLocalResource(ResourceID_t res_id);
  void RegisterRemoteResource(ResourceID_t res_id);
  void RegisterSimulatedResource(ResourceID_t res_id);
  const set<TaskID_t>& RunnableTasksForJob(JobDescriptor* job_desc);
  bool UnbindTaskFromResource(TaskDescriptor* td_ptr, ResourceID_t res_id);
  bool ResourceExceedsLimit(const ResourceVector& resource,
                            const ResourceVector& limit);
  void DetermineTaskBurstinessCoeffs(
      TaskID_t task_id,
      const deque<TaskPerfStatisticsSample>* stats,
      ResourceVectorDouble* coeffs);
  void DetermineCurrentTaskUsage(
      const ResourceVector& measured_usage,
      const ResourceVectorDouble& prev_usage,
      ResourceVectorDouble* current_usage);
  void ClearTaskResourceReservations(TaskID_t task_id);
  bool EstimateTaskResourceUsageFromSimilarTasks(
      TaskDescriptor* td_ptr,
      uint32_t timeslice_index,
      ResourceID_t scheduled_resource,
      ResourceVector* usage_estimate);
  double ScaleToLogistic(double dropoff, double value);
  void DetermineWeightedAverage(const vector<ResourceVector>& resource_vectors,
                                const vector<double>& weights,
                                ResourceVector* weighted_average);
  void DetermineWeightedAverage(const vector<ResourceVector>& resource_vectors,
                                const vector<ResourceVectorDouble>& weights,
                                ResourceVector* weighted_average);
  void DetermineWeightedAverage(const vector<ResourceVector>& resource_vectors,
                                const vector<ResourceVectorDouble>& weights,
                                ResourceVectorDouble* weighted_average);
  void DetermineWeightedAverage(
      const vector<ResourceVectorDouble>& resource_vectors,
      const vector<ResourceVectorDouble>& weights,
      ResourceVectorDouble* weighted_average);
  void CalculateReservationsFromUsage(const ResourceVector& usage,
                                      const ResourceVector& safe_usage,
                                      const ResourceVector& limit,
                                      double reservation_increment,
                                      double safe_margin,
                                      ResourceVector* reservations);
  void UpdateUsageAccuracyRating(const ResourceVector& measured_usage,
                                 const ResourceVector& usage_estimate,
                                 bool previously_estimated,
                                 double exponential_average_coeff,
                                 ResourceVectorDouble* accuracy_ratings);
  void CalculateExponentialAverageWeights(
      double weight, vector<ResourceVectorDouble>* weight_pair);
  void CalculateExponentialAverageWeights(
      const ResourceVectorDouble& weights,
      vector<ResourceVectorDouble>* weight_pair);
  void CalculateReservationsFromUsage(
      const ResourceVector& usage,
      const ResourceVector& safe_usage,
      const ResourceVector& limit,
      const ResourceVectorDouble& reservation_increments,
      const ResourceVectorDouble& safe_margins,
      ResourceVector* reservations);
  void UpdateMachineReservations(ResourceID_t res_id,
                                 const ResourceVector* old_reservations,
                                 const ResourceVector* new_reservations);
  void GetPercentileTaskUsageRecord(
      const vector<TaskUsageRecord>& usage_records,
      uint32_t min_index, uint32_t max_index, uint64_t percentile,
      TaskUsageRecord* median_record);
  string ReservationResourceVectorToString(const ResourceVectorDouble& rv);
  string ReservationResourceVectorToString(const ResourceVector& rv);

  // Cached sets of runnable and blocked tasks; these are updated on each
  // execution of LazyGraphReduction. Note that this set includes tasks from all
  // jobs.
  set<TaskID_t> runnable_tasks_;
  // Initialized to hold the URI of the (currently unique) coordinator this
  // scheduler is associated with. This is passed down to the executor and to
  // tasks so that they can find the coordinator at runtime.
  const string coordinator_uri_;
  // We also record the resource ID of the owning coordinator.
  ResourceID_t coordinator_res_id_;
  // Object that is injected into the scheduler by other modules in order
  // to be notified when certain events happen.
  SchedulingEventNotifierInterface* event_notifier_;
  // A map holding pointers to all executors known to this scheduler. This
  // includes both executors for local and for remote resources.
  map<ResourceID_t, ExecutorInterface*> executors_;
  // A vector holding descriptors of the jobs to be scheduled in the next
  // scheduling round.
  unordered_map<JobID_t, JobDescriptor*,
    boost::hash<boost::uuids::uuid> > jobs_to_schedule_;
  // Pointer to messaging adapter to use for communication with remote
  // resources.
  MessagingAdapterInterface<BaseMessage>* m_adapter_ptr_;
  // A lock indicating if the scheduler is currently
  // in the process of making scheduling decisions.
  boost::recursive_mutex scheduling_lock_;
  // Map of reference subscriptions
  map<DataObjectID_t, set<TaskDescriptor*> > reference_subscriptions_;
  // The current resource to task bindings managed by this scheduler.
  multimap<ResourceID_t, TaskID_t> resource_bindings_;
  // The current task bindings managed by this scheduler.
  unordered_map<TaskID_t, ResourceID_t> task_bindings_;
  // Reservation decay data for each task managed by this scheduler.
  unordered_map<TaskID_t, TaskReservationDecayData>
      task_reservation_decay_data_;
  // Pointer to the coordinator's topology manager
  shared_ptr<TopologyManager> topology_manager_;
  // A tool to record the resource usages of tasks with similar requests
  RequestUsages similar_resource_request_usages_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_EVENT_DRIVEN_SCHEDULER_H

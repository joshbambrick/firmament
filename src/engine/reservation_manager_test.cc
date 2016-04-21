// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// ReservationManager unit tests.

#include <string>
#include <vector>
#include <deque>

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>
#include <boost/uuid/uuid.hpp>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/flow/flow_scheduler.h"
#include "scheduling/event_driven_scheduler.h"
#include "scheduling/task_reservation_decay_data.h"

#include "base/task_perf_statistics_sample.pb.h"
#include "misc/utils.h"

namespace firmament {
namespace scheduler {

// The fixture for testing class ReservationManager.
class ReservationManagerTest : public ::testing::Test {
protected:
  FlowScheduler* scheduler_;

  // You can remove any or all of the following functions if its body
  // is empty.

  ReservationManagerTest() {
    // You can do set-up work for each test here.
  }

  virtual ~ReservationManagerTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).


    boost::uuids::uuid uuid;

    shared_ptr<JobMap_t> job_map(new JobMap_t);
    shared_ptr<ResourceMap_t> resource_map(new ResourceMap_t);
    ResourceTopologyNodeDescriptor* resource_topology(
        new ResourceTopologyNodeDescriptor);
    shared_ptr<ObjectStoreInterface> object_store(NULL);
    shared_ptr<TaskMap_t> task_map(new TaskMap_t);
    shared_ptr<KnowledgeBase> knowledge_base(new KnowledgeBase());
    shared_ptr<TopologyManager> topo_mgr(new TopologyManager());
    MessagingAdapterInterface<BaseMessage>* m_adapter = NULL;
    SchedulingEventNotifierInterface* event_notifier = NULL;
    ResourceID_t coordinator_res_id;
    string coordinator_uri = "";

    ResourceDescriptor resource_desc;
    resource_desc.set_uuid(to_string(uuid));
    resource_desc.set_friendly_name("===");
    resource_desc.set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
    resource_topology->mutable_resource_desc()->CopyFrom(resource_desc);

    scheduler_ = new FlowScheduler(
        job_map, resource_map, resource_topology, object_store, task_map,
        knowledge_base, topo_mgr, m_adapter, event_notifier,
        coordinator_res_id, coordinator_uri);
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for
  // ReservationManager.
};

// Test that two equal numbers are equal.
TEST_F(ReservationManagerTest, SimpleEqualityTest) {
  CHECK_EQ(4, 4);
}

// Test that reservation decay occurs.
TEST_F(ReservationManagerTest, ReservationDecayTest) {
  ResourceVector usage;
  usage.set_ram_cap(50);
  usage.set_disk_bw(50);
  usage.set_disk_cap(50);
  ResourceVector safe_usage(usage);

  ResourceVector limit;
  limit.set_ram_cap(2500);
  limit.set_disk_bw(2500);
  limit.set_disk_cap(2500);

  double reservation_increment = 0.9;
  double overshoot_boost = 2;
  double safe_margin = 0.1;

  ResourceVector reservations;
  reservations.set_ram_cap(250);
  reservations.set_disk_bw(250);
  reservations.set_disk_cap(250);

  scheduler_->CalculateReservationsFromUsage(usage,
      safe_usage,
      limit,
      reservation_increment,
      safe_margin,
      overshoot_boost,
      &reservations);

  CHECK_EQ(reservations.ram_cap(), 225);
  CHECK_EQ(reservations.disk_bw(), 225);
  CHECK_EQ(reservations.disk_cap(), 225);
}

// Test that reservation decay safety is enforced.
TEST_F(ReservationManagerTest, ReservationSafetyTest) {
  ResourceVector usage;
  usage.set_ram_cap(250);
  usage.set_disk_bw(250);
  usage.set_disk_cap(250);
  ResourceVector safe_usage(usage);

  ResourceVector limit;
  limit.set_ram_cap(2500);
  limit.set_disk_bw(2500);
  limit.set_disk_cap(2500);

  double reservation_increment = 0.9;
  double overshoot_boost = 2;
  double safe_margin = 0.1;

  ResourceVector reservations;
  reservations.set_ram_cap(250);
  reservations.set_disk_bw(250);
  reservations.set_disk_cap(250);

  scheduler_->CalculateReservationsFromUsage(usage,
      safe_usage,
      limit,
      reservation_increment,
      safe_margin,
      overshoot_boost,
      &reservations);

  CHECK_EQ(reservations.ram_cap(), 275);
  CHECK_EQ(reservations.disk_bw(), 275);
  CHECK_EQ(reservations.disk_cap(), 275);
}

// Test that reservation decay limit is enforced.
TEST_F(ReservationManagerTest, ReservationLimitTest) {
  ResourceVector usage;
  usage.set_ram_cap(250);
  usage.set_disk_bw(250);
  usage.set_disk_cap(250);
  ResourceVector safe_usage(usage);

  ResourceVector limit;
  limit.set_ram_cap(260);
  limit.set_disk_bw(260);
  limit.set_disk_cap(260);

  double reservation_increment = 0.9;
  double overshoot_boost = 2;
  double safe_margin = 0.1;

  ResourceVector reservations;
  reservations.set_ram_cap(250);
  reservations.set_disk_bw(250);
  reservations.set_disk_cap(250);


  scheduler_->CalculateReservationsFromUsage(usage,
      safe_usage,
      limit,
      reservation_increment,
      safe_margin,
      overshoot_boost,
      &reservations);

  CHECK_EQ(reservations.ram_cap(), 260);
  CHECK_EQ(reservations.disk_bw(), 260);
  CHECK_EQ(reservations.disk_cap(), 260);
}

// Test that reservation boost occurs.
TEST_F(ReservationManagerTest, ReservationBoostTest) {
  ResourceVector usage;
  usage.set_ram_cap(250);
  usage.set_disk_bw(250);
  usage.set_disk_cap(250);
  ResourceVector safe_usage(usage);

  ResourceVector limit;
  limit.set_ram_cap(2500);
  limit.set_disk_bw(2500);
  limit.set_disk_cap(2500);

  double reservation_increment = 0.9;
  double overshoot_boost = 2;
  double safe_margin = 0.1;

  ResourceVector reservations;
  reservations.set_ram_cap(249);
  reservations.set_disk_bw(249);
  reservations.set_disk_cap(249);

  scheduler_->CalculateReservationsFromUsage(usage,
      safe_usage,
      limit,
      reservation_increment,
      safe_margin,
      overshoot_boost,
      &reservations);

  CHECK_EQ(reservations.ram_cap(), 500);
  CHECK_EQ(reservations.disk_bw(), 500);
  CHECK_EQ(reservations.disk_cap(), 500);
}

// Test that task termination occurs correctly.
TEST_F(ReservationManagerTest, TaskTerminationTest) {
  ResourceVector usage;
  usage.set_ram_cap(250);
  usage.set_disk_bw(250);
  usage.set_disk_cap(250);

  ResourceVector limit;
  limit.set_ram_cap(245);
  limit.set_disk_bw(250);
  limit.set_disk_cap(250);

  CHECK(scheduler_->ResourceExceedsLimit(usage, limit, false));
}

// Test that reservation burstiness calculation is correct.
TEST_F(ReservationManagerTest, ReservationBurstinessTest) {
  TaskReservationDecayData decay_data;
  deque<TaskPerfStatisticsSample> stats;

  TaskPerfStatisticsSample sample1;
  sample1.set_task_id(1);
  sample1.set_timestamp(1);
  ResourceVector resources1;
  resources1.set_ram_cap(250);
  resources1.set_disk_bw(250);
  resources1.set_disk_cap(250);
  sample1.mutable_resources()->CopyFrom(resources1);
  stats.push_back(sample1);

  TaskPerfStatisticsSample sample2;
  sample2.set_task_id(2);
  sample2.set_timestamp(2);
  ResourceVector resources2;
  resources2.set_ram_cap(200);
  resources2.set_disk_bw(250);
  resources2.set_disk_cap(250);
  sample2.mutable_resources()->CopyFrom(resources2);
  stats.push_back(sample2);

  TaskPerfStatisticsSample sample3;
  sample3.set_task_id(3);
  sample3.set_timestamp(3);
  ResourceVector resources3;
  resources3.set_ram_cap(300);
  resources3.set_disk_bw(250);
  resources3.set_disk_cap(250);
  sample3.mutable_resources()->CopyFrom(resources3);
  stats.push_back(sample3);

  ResourceVectorDouble coeffs;

  int64_t window_size = 3;
  int64_t approx_min_window_size = -1;

  scheduler_->DetermineTaskBurstinessCoeffs(
      &decay_data, &stats,
      window_size, approx_min_window_size,
      &coeffs);

  CHECK_GT(coeffs.ram_cap(), 6.66);
  CHECK_LT(coeffs.ram_cap(), 6.67);
}

// Test that reservation approximate burstiness calculation is correct.
TEST_F(ReservationManagerTest, ReservationApproximateBurstinessTest) {
  TaskReservationDecayData decay_data;
  ResourceVectorDouble coeffs;

  int64_t window_size = 3;
  int64_t approx_min_window_size = 3;

  deque<TaskPerfStatisticsSample> stats;

  TaskPerfStatisticsSample sample1;
  sample1.set_task_id(1);
  sample1.set_timestamp(1);
  ResourceVector resources1;
  resources1.set_ram_cap(250);
  resources1.set_disk_bw(250);
  resources1.set_disk_cap(250);
  sample1.mutable_resources()->CopyFrom(resources1);
  stats.push_back(sample1);


  scheduler_->DetermineTaskBurstinessCoeffs(
      &decay_data, &stats,
      window_size, approx_min_window_size,
      &coeffs);

  TaskPerfStatisticsSample sample2;
  sample2.set_task_id(2);
  sample2.set_timestamp(2);
  ResourceVector resources2;
  resources2.set_ram_cap(200);
  resources2.set_disk_bw(250);
  resources2.set_disk_cap(250);
  sample2.mutable_resources()->CopyFrom(resources2);
  stats.push_back(sample2);

  scheduler_->DetermineTaskBurstinessCoeffs(
      &decay_data, &stats,
      window_size, approx_min_window_size,
      &coeffs);

  TaskPerfStatisticsSample sample3;
  sample3.set_task_id(3);
  sample3.set_timestamp(3);
  ResourceVector resources3;
  resources3.set_ram_cap(300);
  resources3.set_disk_bw(250);
  resources3.set_disk_cap(250);
  sample3.mutable_resources()->CopyFrom(resources3);
  stats.push_back(sample3);


  scheduler_->DetermineTaskBurstinessCoeffs(
      &decay_data, &stats,
      window_size, approx_min_window_size,
      &coeffs);

  CHECK_GT(coeffs.ram_cap(), 116.11);
  CHECK_LT(coeffs.ram_cap(), 116.12);
}


// Test that accuracy rating calculation is correct.
TEST_F(ReservationManagerTest, AccuracyRatingCalculationTest) {
  ResourceVector usage;
  usage.set_ram_cap(250);
  usage.set_disk_bw(250);
  usage.set_disk_cap(250);

  ResourceVector prediction;
  prediction.set_ram_cap(145);
  prediction.set_disk_bw(250);
  prediction.set_disk_cap(250);

  bool previously_calculated = true;
  double coeff = 0.7;

  ResourceVectorDouble rating;
  rating.set_ram_cap(0.6);

  scheduler_->UpdateUsageAccuracyRating(
      usage, prediction, previously_calculated, coeff, &rating);
  CHECK_GT(rating.ram_cap(), 0.455);
  CHECK_LT(rating.ram_cap(), 0.456);

  previously_calculated = false;

  scheduler_->UpdateUsageAccuracyRating(
      usage, prediction, previously_calculated, coeff, &rating);
  CHECK_GT(rating.ram_cap(), 0.394);
  CHECK_LT(rating.ram_cap(), 0.395);
}

// Test that usage smoothing is correct.
TEST_F(ReservationManagerTest, ExponentialUsageSmoothingTest) {
  ResourceVector usage;
  usage.set_ram_cap(250);
  usage.set_disk_bw(250);
  usage.set_disk_cap(250);

  ResourceVectorDouble prev_usage;
  prev_usage.set_ram_cap(145);
  prev_usage.set_disk_bw(250);
  prev_usage.set_disk_cap(250);

  double coeff = 0.7;

  ResourceVectorDouble current_usage;

  scheduler_->DetermineCurrentTaskUsage(
    usage, prev_usage, coeff, -1, &current_usage);
  CHECK_GT(current_usage.ram_cap(), 218);
  CHECK_LT(current_usage.ram_cap(), 219);
}

// Test that the task similarity prediction works correctly.
TEST_F(ReservationManagerTest, SimilarTaskPredictionTest) {
  TaskDescriptor new_task;

  UsageList* new_list1 = new_task.add_similar_resource_request_usage_lists();
  new_list1->set_distance(30);
  new_list1->set_machine_id(to_string(scheduler_->coordinator_res_id_));
  new_list1->set_matched_equivalence_classes(1);
  TaskUsageRecord* record1_1 = new_list1->add_usage_records();
  record1_1->set_is_valid(true);
  record1_1->set_ram_cap(100);
  record1_1->set_disk_bw(10);
  record1_1->set_disk_cap(10);
  TaskUsageRecord* record1_2 = new_list1->add_usage_records();
  record1_2->set_is_valid(true);
  record1_2->set_ram_cap(200);
  record1_2->set_disk_bw(20);
  record1_2->set_disk_cap(20);

  UsageList* new_list2 = new_task.add_similar_resource_request_usage_lists();
  new_list2->set_distance(10);
  new_list2->set_machine_id(to_string(scheduler_->coordinator_res_id_));
  new_list2->set_matched_equivalence_classes(2);
  TaskUsageRecord* record2_1 = new_list2->add_usage_records();
  record2_1->set_is_valid(true);
  record2_1->set_ram_cap(200);
  record2_1->set_disk_bw(20);
  record2_1->set_disk_cap(20);
  TaskUsageRecord* record2_2 = new_list2->add_usage_records();
  record2_2->set_is_valid(true);
  record2_2->set_ram_cap(300);
  record2_2->set_disk_bw(30);
  record2_2->set_disk_cap(30);

  UsageList* new_list3 = new_task.add_similar_resource_request_usage_lists();
  new_list3->set_distance(50);
  new_list3->set_machine_id(to_string(scheduler_->coordinator_res_id_));
  new_list3->set_matched_equivalence_classes(3);
  TaskUsageRecord* record3_1 = new_list3->add_usage_records();
  record3_1->set_is_valid(true);
  record3_1->set_ram_cap(300);
  record3_1->set_disk_bw(30);
  record3_1->set_disk_cap(30);
  TaskUsageRecord* record3_2 = new_list3->add_usage_records();
  record3_2->set_is_valid(true);
  record3_2->set_ram_cap(100);
  record3_2->set_disk_bw(10);
  record3_2->set_disk_cap(10);


  ResourceVector usage_estimate;

  uint32_t timeslice_index = 0;
  bool res;
  res = scheduler_->EstimateTaskResourceUsageFromSimilarTasks(
    &new_task,
    timeslice_index,
    scheduler_->coordinator_res_id_,
    &usage_estimate);
  CHECK(res);
  CHECK_EQ(usage_estimate.ram_cap(), 203);

  timeslice_index = 1;
  res = scheduler_->EstimateTaskResourceUsageFromSimilarTasks(
    &new_task,
    timeslice_index,
    scheduler_->coordinator_res_id_,
    &usage_estimate);
  CHECK(res);
  CHECK_EQ(usage_estimate.ram_cap(), 204);

  timeslice_index = 2;
  res = scheduler_->EstimateTaskResourceUsageFromSimilarTasks(
    &new_task,
    timeslice_index,
    scheduler_->coordinator_res_id_,
    &usage_estimate);
  CHECK(!res);
}

// Test that the task similarity tree works correctly.
TEST_F(ReservationManagerTest, SimilarTaskTreeTest) {
  uint64_t k = 1;
  scheduler_->similar_resource_request_usages_.UpdateK(k);

  uint64_t ten_secs = 10000;

  UsageRecord new_record1_1(true, 100, 25, 10);
  UsageRecord new_record1_2(true, 95, 29, 50);
  UsageRecord new_record1_3(true, 80, 34, 100);
  Request usage_request1(126, 47, 500);

  vector<EquivClass_t> equiv_classes1 = {1, 5, 10, 25};
  ResourceID_t machine_id1 = GenerateResourceID();
  UsageRecordList task_usage_records1(
      {new_record1_1, new_record1_2, new_record1_3},
      equiv_classes1,
      ten_secs,
      machine_id1,
      usage_request1);
  scheduler_->similar_resource_request_usages_.AddToTree(task_usage_records1);

  UsageRecord new_record2_1(true, 60, 45, 70);
  UsageRecord new_record2_2(true, 90, 30, 50);
  UsageRecord new_record2_3(true, 91, 29, 45);
  Request usage_request2(110, 40, 400);

  vector<EquivClass_t> equiv_classes2 = {1, 5, 10, 25};
  ResourceID_t machine_id2 = GenerateResourceID();
  UsageRecordList task_usage_records2(
      {new_record2_1, new_record2_2, new_record2_3},
      equiv_classes2,
      ten_secs,
      machine_id2,
      usage_request2);
  scheduler_->similar_resource_request_usages_.AddToTree(task_usage_records2);

  Request request1(111, 41, 405);
  vector<ComparedUsageRecordList> record_lists1;
  scheduler_->similar_resource_request_usages_.LookUpTree(request1, &record_lists1);

  CHECK_EQ(record_lists1.size(), k);
  CHECK_EQ(record_lists1[0].request.ram_cap, 110);
  CHECK_EQ(record_lists1[0].request.disk_bw, 40);
  CHECK_EQ(record_lists1[0].request.disk_cap, 400);

  Request request2(125, 50, 485);
  vector<ComparedUsageRecordList> record_lists2;
  scheduler_->similar_resource_request_usages_.LookUpTree(request2, &record_lists2);
  CHECK_EQ(record_lists2.size(), k);
  CHECK_EQ(record_lists2[0].request.ram_cap, 126);
  CHECK_EQ(record_lists2[0].request.disk_bw, 47);
  CHECK_EQ(record_lists2[0].request.disk_cap, 500);

}

}  // namespace scheduler
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

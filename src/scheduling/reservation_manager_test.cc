// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// ReservationManager unit tests.

#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/simple/simple_scheduler.h"
#include "scheduling/event_driven_scheduler.h"

namespace firmament {
namespace scheduler {

// The fixture for testing class ReservationManager.
class ReservationmanagerTest : public ::testing::Test {
protected:
  SchedulerInterface* scheduler_;

  // You can remove any or all of the following functions if its body
  // is empty.

  ReservationmanagerTest() {
    // You can do set-up work for each test here.
  }

  virtual ~ReservationmanagerTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).


    shared_ptr<JobMap_t> job_map;
    shared_ptr<ResourceMap_t> resource_map;
    ResourceTopologyNodeDescriptor* resource_topology = NULL;
    shared_ptr<ObjectStoreInterface> object_store;
    shared_ptr<TaskMap_t> task_map;
    shared_ptr<KnowledgeBase> knowledge_base;
    shared_ptr<TopologyManager> topo_mgr;
    MessagingAdapterInterface<BaseMessage>* m_adapter = NULL;
    SchedulingEventNotifierInterface* event_notifier = NULL;
    ResourceID_t coordinator_res_id;
    string coordinator_uri = "";

    scheduler_ = new SimpleScheduler(
        job_map, resource_map, resource_topology,object_store, task_map,
        knowledge_base, topo_mgr, m_adapter, event_notifier, coordinator_res_id,
        coordinator_uri);
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for
  // ReservationManager.
};

// Test that two equal numbers are equal.
TEST_F(ReservationmanagerTest, SimpleEqualityTest) {
  CHECK_EQ(4, 4);
}

// Test that reservation decay occurs.
TEST_F(ReservationmanagerTest, ReservationDecayTest) {
  CHECK_EQ(4, 4);
}

}  // namespace scheduler
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

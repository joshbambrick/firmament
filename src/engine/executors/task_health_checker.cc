// The Firmament project
// Copyright (c) 2011-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "engine/executors/task_health_checker.h"

#include <vector>

#if BOOST_VERSION <= 104800
#include <boost/chrono.hpp>
#endif

#include "misc/map-util.h"
#include "misc/utils.h"

namespace firmament {

TaskHealthChecker::TaskHealthChecker(
    const unordered_map<TaskID_t, boost::thread*>* handler_thread_map,
    boost::shared_mutex* handler_map_lock,
    boost::shared_mutex* finalize_map_lock)
  : handler_thread_map_(handler_thread_map),
    handler_map_lock_(handler_map_lock),
    finalize_map_lock_(finalize_map_lock) {
}

bool TaskHealthChecker::Run(vector<TaskID_t>* failed_tasks,
    const unordered_map<TaskID_t, TaskStateMessage>* task_finalize_messages) {
  bool all_good = true;
  boost::shared_lock<boost::shared_mutex> map_lock(*handler_map_lock_);
  boost::shared_lock<boost::shared_mutex> finalize_map_lock(*finalize_map_lock_);
  for (unordered_map<TaskID_t, boost::thread*>::const_iterator
       it = handler_thread_map_->begin();
       it != handler_thread_map_->end();
       ++it) {
    VLOG(2) << "Checking liveness of task " << it->first;

    if (!CheckTaskLiveness(it->first, it->second)
                    && !CheckTaskCompleted(it->first, task_finalize_messages)) {
      all_good = false;
      LOG(ERROR) << "Task " << it->first << " has failed!";
      failed_tasks->push_back(it->first);
    }
  }
  return all_good;
}

bool TaskHealthChecker::CheckTaskLiveness(TaskID_t task_id,
                                          boost::thread* handler_thread) {
  if (!handler_thread)
    return false;
#if BOOST_VERSION <= 104800
  if (handler_thread->timed_join(boost::posix_time::milliseconds(100)))
    return false;
#else
  if (handler_thread->try_join_for(boost::chrono::milliseconds(100)))
    return false;
#endif
  if (!handler_thread->joinable())
    return false;

  return true;
}

bool TaskHealthChecker::CheckTaskCompleted(TaskID_t task_id,
  const unordered_map<TaskID_t, TaskStateMessage>* task_finalize_messages) {
  const TaskStateMessage* message = FindOrNull(*task_finalize_messages, task_id);
  return message;
}

}  // namespace firmament

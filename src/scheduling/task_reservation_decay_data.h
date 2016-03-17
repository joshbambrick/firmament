#ifndef FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H
#define FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H

#include "base/common.h"
#include "base/resource_vector.pb.h"

namespace firmament {

class TaskReservationDecayData {
  public:
    uint32_t median_timeslice_duration_ms = 0;
    ResourceVector last_usage_calculated;
    bool usage_measured = false;
    ResourceVector last_usage_estimate;
    bool usage_estimated = false;
    double usage_estimate_accuracy_rating = 0;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H

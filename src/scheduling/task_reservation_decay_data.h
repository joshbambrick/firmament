#ifndef FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H
#define FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H

#include "base/common.h"
#include "base/resource_vector.pb.h"

namespace firmament {

class TaskReservationDecayData {
  public:
    uint32_t median_timeslice_duration_ms;
    ResourceVector last_usage_measurement;
    bool usage_measured;
    ResourceVector last_usage_estimate;
    double usage_estimate_accuracy_rating;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H

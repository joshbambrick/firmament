#ifndef FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H
#define FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H

#include <list>
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

    ResourceVector sum_xs_diff;
    ResourceVector sum_usage;
    uint64_t usages_included = 0;
    list<ResourceVector> xs_diffs;
    list<ResourceVector> usages;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_TASK_RESERVATION_DECAY_DATA_H

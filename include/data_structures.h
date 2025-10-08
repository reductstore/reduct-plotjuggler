#pragma once
#include <cstdint>

namespace common {
  struct AccelerationData {
    int64_t ts_ns;
    double linear_acceleration_x;
    double linear_acceleration_y;
    double linear_acceleration_z;
  };
}
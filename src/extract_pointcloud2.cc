#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <reduct/client.h>
#include <string>
#include <vector>
#include "../include/common.h"
#include "../include/utilities.h"

using reduct::Error;
using reduct::IBucket;
using reduct::IClient;

struct PointData {
  float x;
  float y;
  float z;
  float intensity;
};

struct ScanData {
  std::vector<PointData> points;
  std::map<std::string, std::string> labels;
  reduct::IBucket::Time timestamp;
};

std::vector<PointData> to_xyz(const std::string &blob) {
  std::vector<PointData> points;
  const size_t point_size = sizeof(PointData);
  const size_t num_points = blob.size() / point_size;

  points.reserve(num_points);

  const unsigned char *bytes =
      reinterpret_cast<const unsigned char *>(blob.data());
  for (size_t i = 0; i < num_points; ++i) {
    const unsigned char *p = bytes + i * point_size;
    PointData point;
    std::memcpy(&point.x, p + 0, 4);
    std::memcpy(&point.y, p + 4, 4);
    std::memcpy(&point.z, p + 8, 4);
    std::memcpy(&point.intensity, p + 12, 4);
    points.push_back(point);
  }

  return points;
}

int main() {
  constexpr const char *ENTRY =
      "raw__os_node_segmented_point_cloud_no_destagger";
  constexpr int MAX_SCANS = 4;

  auto start_time = common::parse_time(common::START_STR);
  auto stop_time = common::parse_time(common::STOP_STR);

  const std::string when = std::string(R"({"$each_t":"5s","$limit":)") +
                           std::to_string(MAX_SCANS) + "}";

  auto client = IClient::Build(common::URL, {.api_token = common::TOKEN});
  auto [bucket, b_err] = client->GetBucket(common::BUCKET);
  assert(b_err == Error::kOk);

  std::vector<ScanData> scan_data;
  std::cout << "Fetching " << MAX_SCANS
            << " point cloud scans from ReductStore...\n";

  auto q_err =
      bucket->Query(ENTRY, start_time, stop_time, {.when = when},
                    [&](const IBucket::ReadableRecord &rec) {
                      auto [blob, r_err] = rec.ReadAll();
                      assert(r_err == Error::kOk);

                      ScanData scan;
                      scan.points = to_xyz(blob);
                      scan.labels = rec.labels;
                      scan.timestamp = rec.timestamp;

                      scan_data.push_back(std::move(scan));

                      std::cout << "Loaded scan " << scan_data.size() << ": "
                                << scan.points.size() << " points, timestamp: "
                                << scan.timestamp.time_since_epoch().count()
                                << "\n";

                      return true;
                    });

  assert(q_err == Error::kOk);

  if (scan_data.empty()) {
    std::cout << "No scans retrieved!\n";
    return 1;
  }

  auto t0 = scan_data[0].timestamp;
  std::vector<double> rel_t;
  for (const auto &scan : scan_data) {
    auto duration = scan.timestamp - t0;
    rel_t.push_back(std::chrono::duration<double>(duration).count());
  }

  std::cout << "\n=== Point Cloud Analysis (C++ Proof of Concept) ===\n";
  std::cout << "Total scans: " << scan_data.size() << "\n";
  std::cout << "Time range: " << std::fixed << std::setprecision(1)
            << rel_t.front() << "s to " << rel_t.back() << "s\n\n";

  const int sample_step = 100;

  for (size_t i = 0; i < scan_data.size(); ++i) {
    const auto &scan = scan_data[i];
    const auto &points = scan.points;

    std::cout << "Scan " << (i + 1) << " at t = " << std::fixed
              << std::setprecision(1) << rel_t[i] << "s:\n";
    std::cout << "  Total points: " << points.size() << "\n";

    std::vector<float> x_vals, y_vals, z_vals, intensity_vals;
    for (size_t j = 0; j < points.size(); j += sample_step) {
      const auto &pt = points[j];
      if (!std::isnan(pt.x) && !std::isnan(pt.y) && !std::isnan(pt.z)) {
        x_vals.push_back(pt.x);
        y_vals.push_back(pt.y);
        z_vals.push_back(pt.z);
        intensity_vals.push_back(pt.intensity);
      }
    }

    std::cout << "  Sampled points (every " << sample_step
              << "th): " << points.size() / sample_step << " total, "
              << x_vals.size() << " valid (non-NaN)\n";

    if (!x_vals.empty()) {
      auto [x_min, x_max] = std::minmax_element(x_vals.begin(), x_vals.end());
      auto [y_min, y_max] = std::minmax_element(y_vals.begin(), y_vals.end());
      auto [z_min, z_max] = std::minmax_element(z_vals.begin(), z_vals.end());
      auto [i_min, i_max] =
          std::minmax_element(intensity_vals.begin(), intensity_vals.end());

      std::cout << "  X range: [" << std::fixed << std::setprecision(2)
                << *x_min << ", " << *x_max << "]\n";
      std::cout << "  Y range: [" << *y_min << ", " << *y_max << "]\n";
      std::cout << "  Z range: [" << *z_min << ", " << *z_max << "]\n";
      std::cout << "  Intensity range: [" << *i_min << ", " << *i_max << "]\n";

      std::cout << "  Sample points:\n";
      for (size_t j = 0; j < std::min(size_t(5), x_vals.size()); ++j) {
        std::cout << "    Point " << j * sample_step << ": "
                  << "x=" << x_vals[j] << ", y=" << y_vals[j]
                  << ", z=" << z_vals[j] << ", I=" << intensity_vals[j] << "\n";
      }
    }

    std::cout << "\n";
  }

  return 0;
}

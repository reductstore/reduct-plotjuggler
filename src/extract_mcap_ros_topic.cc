#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <nlohmann/json.hpp>
#include <numeric>
#include <reduct/client.h>
#include <string>
#include <vector>
#include "../include/common.h"
#include "../include/data_structures.h"
#include "../include/utilities.h"

using reduct::Error;
using reduct::IBucket;
using reduct::IClient;
using json = nlohmann::json;

int main() {
  constexpr const char *MCAP_ENTRY = "mcap";
  constexpr const char *IMU_TOPIC = "/vectornav/IMU_restamped";

  auto start_time = common::parse_time(common::START_STR);
  auto stop_time = common::parse_time(common::STOP_STR);

  const std::string ext =
      std::string(R"({"ros":{"extract":{"topic":")") + IMU_TOPIC + R"("}}})";

  std::cout << "=== C++ MCAP ROS Topic Extraction (Proof of Concept) ===\n";
  std::cout << "Connecting to ReductStore...\n";
  std::cout << "MCAP Entry: " << MCAP_ENTRY << "\n";
  std::cout << "IMU Topic: " << IMU_TOPIC << "\n\n";

  auto client = IClient::Build(common::URL, {.api_token = common::TOKEN});
  auto [bucket, b_err] = client->GetBucket(common::BUCKET);
  assert(b_err == Error::kOk);

  std::vector<common::AccelerationData> df_ros;

  std::cout << "Processing ROS messages from MCAP...\n";

  auto q_err = bucket->Query(
      MCAP_ENTRY, start_time, stop_time, {.ext = ext},
      [&](const IBucket::ReadableRecord &rec) {
        auto [blob, r_err] = rec.ReadAll();
        assert(r_err == Error::kOk);

        auto data = json::parse(blob);

        int64_t sec = data["header"]["stamp"]["sec"].get<int64_t>();
        int64_t nsec = data["header"]["stamp"]["nanosec"].get<int64_t>();
        int64_t ts_ns = sec * 1'000'000'000LL + nsec;

        double ax = data["linear_acceleration"]["x"].get<double>();
        double ay = data["linear_acceleration"]["y"].get<double>();
        double az = data["linear_acceleration"]["z"].get<double>();

        common::AccelerationData row = {ts_ns, ax, ay, az};
        df_ros.push_back(row);

        if (df_ros.size() % 100 == 0) {
          std::cout << "  Processed " << df_ros.size() << " messages...\n";
        }

        return true;
      });

  assert(q_err == Error::kOk);

  std::cout << "\n=== Processing Complete ===\n";
  std::cout << "Total ROS messages extracted: " << df_ros.size() << "\n";

  if (!df_ros.empty()) {
    std::sort(
        df_ros.begin(), df_ros.end(),
        [](const common::AccelerationData &a, const common::AccelerationData &b) { return a.ts_ns < b.ts_ns; });
    common::print_dataframe_head(df_ros);
    common::print_dataframe_stats(df_ros);
  } else {
    std::cout << "\nNo ROS messages found for topic: " << IMU_TOPIC << "\n";
  }

  return 0;
}

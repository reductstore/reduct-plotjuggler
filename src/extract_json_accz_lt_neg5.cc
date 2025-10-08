#include <algorithm>
#include <cassert>
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
  constexpr const char *JSON_ENTRY = "json__vectornav_IMU";

  auto start_time = common::parse_time(common::START_STR);
  auto stop_time = common::parse_time(common::STOP_STR);

  const std::string ext = R"({
      "select": {
        "json": {},
        "columns": [
          {"name": "ts_ns"},
          {"name": "linear_acceleration_x"},
          {"name": "linear_acceleration_y"},
          {"name": "linear_acceleration_z", "as_label": "acc_z"}
        ]
      },
      "when": {"@acc_z": {"$lt": -5}}
    })";

  std::cout
      << "=== C++ JSON Data Extraction with Filtering (Proof of Concept) ===\n";
  std::cout << "Connecting to ReductStore...\n";
  std::cout << "JSON Entry: " << JSON_ENTRY << "\n";
  std::cout << "Filter: acc_z < -5\n\n";

  auto client = IClient::Build(common::URL, {.api_token = common::TOKEN});
  auto [bucket, b_err] = client->GetBucket(common::BUCKET);
  assert(b_err == Error::kOk);

  std::vector<common::AccelerationData> df_json;

  std::cout << "Processing filtered JSON data...\n";

  auto q_err =
      bucket->Query(JSON_ENTRY, start_time, stop_time, {.ext = ext},
                    [&](const IBucket::ReadableRecord &rec) {
                      auto [blob, r_err] = rec.ReadAll();
                      assert(r_err == Error::kOk);

                      auto rows = json::parse(blob);

                      for (const auto &row : rows) {
                        common::AccelerationData data_row;
                        data_row.ts_ns = row["ts_ns"].get<int64_t>();
                        data_row.linear_acceleration_x =
                            row["linear_acceleration_x"].get<double>();
                        data_row.linear_acceleration_y =
                            row["linear_acceleration_y"].get<double>();
                        data_row.linear_acceleration_z =
                            row["linear_acceleration_z"].get<double>();

                        df_json.push_back(data_row);
                      }

                      if (df_json.size() % 100 == 0 && df_json.size() > 0) {
                        std::cout << "  Processed " << df_json.size()
                                  << " filtered records...\n";
                      }

                      return true;
                    });

  assert(q_err == Error::kOk);

  std::cout << "\n=== Processing Complete ===\n";
  std::cout << "Total filtered JSON records extracted: " << df_json.size()
            << "\n";
  std::cout << "All records have linear_acceleration_z < -5.0\n";

  if (!df_json.empty()) {
    std::sort(
        df_json.begin(), df_json.end(),
        [](const common::AccelerationData &a, const common::AccelerationData &b) { return a.ts_ns < b.ts_ns; });

    common::print_dataframe_head(df_json);
    common::print_dataframe_stats(df_json);

    std::cout << "\n=== Filter Verification ===\n";
    bool all_filtered_correctly =
        std::all_of(df_json.begin(), df_json.end(), [](const common::AccelerationData &row) {
          return row.linear_acceleration_z < -5.0;
        });

    std::cout << "Filter verification: "
              << (all_filtered_correctly ? "✓ PASSED" : "✗ FAILED") << "\n";
    std::cout << "All records have acc_z < -5.0: "
              << (all_filtered_correctly ? "Yes" : "No") << "\n";

    auto [min_z, max_z] = std::minmax_element(
        df_json.begin(), df_json.end(),
        [](const common::AccelerationData &a, const common::AccelerationData &b) {
          return a.linear_acceleration_z < b.linear_acceleration_z;
        });

    std::cout << "Filtered acc_z range: [" << std::fixed << std::setprecision(6)
              << min_z->linear_acceleration_z << ", "
              << max_z->linear_acceleration_z << "]\n";
  } else {
    std::cout << "\nNo JSON records found matching the filter criteria (acc_z "
                 "< -5).\n";
  }

  return 0;
}

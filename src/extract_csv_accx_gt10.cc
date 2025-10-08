#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <reduct/client.h>
#include <sstream>
#include <string>
#include <vector>
#include "../include/common.h"
#include "../include/data_structures.h"
#include "../include/utilities.h"

using reduct::Error;
using reduct::IBucket;
using reduct::IClient;

common::AccelerationData parse_csv_line(const std::string &line) {
  std::istringstream iss(line);
  std::string field;
  common::AccelerationData row;

  std::getline(iss, field, ',');
  row.ts_ns = std::stoll(field);

  std::getline(iss, field, ',');
  row.linear_acceleration_x = std::stod(field);

  std::getline(iss, field, ',');
  row.linear_acceleration_y = std::stod(field);

  std::getline(iss, field, ',');
  row.linear_acceleration_z = std::stod(field);

  return row;
}

int main() {
  constexpr const char *CSV_ENTRY = "csv__vectornav_IMU";

  auto start_time = common::parse_time(common::START_STR);
  auto stop_time = common::parse_time(common::STOP_STR);

  const std::string ext = R"({
      "select": {
        "csv": {"has_headers": true},
        "columns": [
          {"name": "ts_ns"},
          {"name": "linear_acceleration_x", "as_label": "acc_x"},
          {"name": "linear_acceleration_y"},
          {"name": "linear_acceleration_z"}
        ]
      },
      "when": {"$gt":[{"$abs":["@acc_x"]},10]}
    })";

  std::cout
      << "=== C++ CSV Data Extraction with Filtering (Proof of Concept) ===\n";
  std::cout << "Connecting to ReductStore...\n";
  std::cout << "CSV Entry: " << CSV_ENTRY << "\n";
  std::cout << "Filter: |acc_x| > 10\n\n";

  auto client = IClient::Build(common::URL, {.api_token = common::TOKEN});
  auto [bucket, b_err] = client->GetBucket(common::BUCKET);
  assert(b_err == Error::kOk);

  std::vector<common::AccelerationData> df_csv;

  std::cout << "Processing filtered CSV data...\n";

  auto q_err =
      bucket->Query(CSV_ENTRY, start_time, stop_time, {.ext = ext},
                    [&](const IBucket::ReadableRecord &rec) {
                      auto [blob, r_err] = rec.ReadAll();
                      assert(r_err == Error::kOk);

                      std::istringstream csv_stream(blob);
                      std::string line;
                      bool is_header = true;

                      while (std::getline(csv_stream, line)) {
                        if (line.empty())
                          continue;

                        if (is_header) {
                          is_header = false;
                          continue;
                        }

                        try {
                          common::AccelerationData row = parse_csv_line(line);
                          df_csv.push_back(row);
                        } catch (const std::exception &e) {
                          std::cerr << "Error parsing line: " << line << " - "
                                    << e.what() << "\n";
                        }
                      }

                      if (df_csv.size() % 100 == 0 && df_csv.size() > 0) {
                        std::cout << "  Processed " << df_csv.size()
                                  << " filtered records...\n";
                      }

                      return true;
                    });

  assert(q_err == Error::kOk);

  std::cout << "\n=== Processing Complete ===\n";
  std::cout << "Total filtered CSV records extracted: " << df_csv.size()
            << "\n";
  std::cout << "All records have |linear_acceleration_x| > 10.0\n";

  if (!df_csv.empty()) {
    std::sort(
        df_csv.begin(), df_csv.end(),
        [](const common::AccelerationData &a, const common::AccelerationData &b) { return a.ts_ns < b.ts_ns; });

    common::print_dataframe_head(df_csv);
    common::print_dataframe_stats(df_csv);

    std::cout << "\n=== Filter Verification ===\n";
    bool all_filtered_correctly =
        std::all_of(df_csv.begin(), df_csv.end(), [](const common::AccelerationData &row) {
          return std::abs(row.linear_acceleration_x) > 10.0;
        });

    std::cout << "Filter verification: "
              << (all_filtered_correctly ? "✓ PASSED" : "✗ FAILED") << "\n";
    std::cout << "All records have |acc_x| > 10.0: "
              << (all_filtered_correctly ? "Yes" : "No") << "\n";

    auto [min_x, max_x] = std::minmax_element(
        df_csv.begin(), df_csv.end(), [](const common::AccelerationData &a, const common::AccelerationData &b) {
          return a.linear_acceleration_x < b.linear_acceleration_x;
        });

    std::cout << "Filtered acc_x range: [" << std::fixed << std::setprecision(6)
              << min_x->linear_acceleration_x << ", "
              << max_x->linear_acceleration_x << "]\n";

    std::vector<double> abs_x_vals;
    for (const auto &row : df_csv) {
      abs_x_vals.push_back(std::abs(row.linear_acceleration_x));
    }
    auto [min_abs, max_abs] =
        std::minmax_element(abs_x_vals.begin(), abs_x_vals.end());
    std::cout << "Filtered |acc_x| range: [" << *min_abs << ", " << *max_abs
              << "]\n";
  } else {
    std::cout << "\nNo CSV records found matching the filter criteria (|acc_x| "
                 "> 10).\n";
  }

  return 0;
}

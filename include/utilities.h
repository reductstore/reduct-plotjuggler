#pragma once
#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <vector>
#include <cctype>
#include <reduct/client.h>
#include "data_structures.h"


namespace common {
  using Time = reduct::IBucket::Time;
  
  std::optional<Time> parse_time(const std::string& s) {
    if (s.empty()) return std::nullopt;

    std::tm tm{}; std::istringstream ss(s);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) throw std::runtime_error("Failed to parse datetime");

    time_t tt = timegm(&tm);
    Time tp = std::chrono::time_point_cast<Time::duration>(
        std::chrono::system_clock::from_time_t(tt));

    if (ss.peek() == '.') {
      ss.get();
      std::string frac;
      while (std::isdigit(ss.peek())) frac.push_back(char(ss.get()));
      if (frac.size() > 6) frac.resize(6);
      while (frac.size() < 6) frac.push_back('0');
      tp += std::chrono::microseconds(std::stoi(frac));
    }
    if (ss.peek() == 'Z') ss.get();

    return tp;
  }

  void print_dataframe_head(const std::vector<AccelerationData> &data, size_t n = 5) {
    std::cout << "\n=== DataFrame Head (first " << std::min(n, data.size())
              << " rows) ===\n";
    std::cout << std::setw(20) << "ts_ns" << std::setw(20) << "linear_accel_x"
              << std::setw(20) << "linear_accel_y" << std::setw(20)
              << "linear_accel_z" << "\n";
    std::cout << std::string(80, '-') << "\n";

    for (size_t i = 0; i < std::min(n, data.size()); ++i) {
      const auto &row = data[i];
      std::cout << std::setw(20) << row.ts_ns << std::setw(20) << std::fixed
                << std::setprecision(6) << row.linear_acceleration_x
                << std::setw(20) << std::fixed << std::setprecision(6)
                << row.linear_acceleration_y << std::setw(20) << std::fixed
                << std::setprecision(6) << row.linear_acceleration_z << "\n";
    }
  }

  void print_dataframe_stats(const std::vector<AccelerationData> &data) {
    if (data.empty()) {
      std::cout << "\nNo data available for statistics.\n";
      return;
    }

    std::cout << "\n=== DataFrame Statistics ===\n";
    std::cout << "Total records: " << data.size() << "\n";

    std::vector<double> x_vals, y_vals, z_vals;
    for (const auto &row : data) {
      x_vals.push_back(row.linear_acceleration_x);
      y_vals.push_back(row.linear_acceleration_y);
      z_vals.push_back(row.linear_acceleration_z);
    }

    auto calc_stats = [](const std::vector<double> &vals,
                         const std::string &name) {
      auto [min_it, max_it] = std::minmax_element(vals.begin(), vals.end());
      double sum = std::accumulate(vals.begin(), vals.end(), 0.0);
      double mean = sum / vals.size();

      std::cout << std::setw(20) << name << ": min=" << std::setw(10)
                << std::fixed << std::setprecision(6) << *min_it
                << ", max=" << std::setw(10) << std::fixed << std::setprecision(6)
                << *max_it << ", mean=" << std::setw(10) << std::fixed
                << std::setprecision(6) << mean << "\n";
    };

    calc_stats(x_vals, "linear_accel_x");
    calc_stats(y_vals, "linear_accel_y");
    calc_stats(z_vals, "linear_accel_z");

    if (!data.empty()) {
      int64_t min_ts = data.front().ts_ns;
      int64_t max_ts = data.back().ts_ns;
      double duration_sec = (max_ts - min_ts) / 1e9;

      std::cout << "\nTime range:\n";
      std::cout << "  Start timestamp: " << min_ts << " ns\n";
      std::cout << "  End timestamp:   " << max_ts << " ns\n";
      std::cout << "  Duration:        " << std::fixed << std::setprecision(3)
                << duration_sec << " seconds\n";
      if (duration_sec > 0) {
        std::cout << "  Sample rate:     " << std::fixed << std::setprecision(1)
                  << (data.size() / duration_sec) << " Hz (approx)\n";
      }
    }
  }
}
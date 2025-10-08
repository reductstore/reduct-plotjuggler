#include <cassert>
#include <chrono>
#include <ctime>
#include <iostream>
#include <reduct/client.h>
#include <string>
#include "../include/common.h"

using reduct::Error;
using reduct::IClient;

static std::string PrintTime(std::chrono::system_clock::time_point tp) {
  std::time_t t = std::chrono::system_clock::to_time_t(tp);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
  return buf;
}

int main() {
  auto client = IClient::Build(common::URL, {.api_token = common::TOKEN});

  auto [bucket, b_err] = client->GetBucket(common::BUCKET);
  assert(b_err == Error::kOk && "GetBucket failed");

  auto [entries, e_err] = bucket->GetEntryList();
  assert(e_err == Error::kOk && "GetEntryList failed");

  std::cout << "Entries in bucket '" << common::BUCKET << "':\n";
  for (const auto &e : entries) {
    std::cout << "  - " << e.name << " | size=" << e.size
              << " | records=" << e.record_count
              << " | oldest=" << PrintTime(e.oldest_record)
              << " | latest=" << PrintTime(e.latest_record) << "\n";
  }
  return 0;
}

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <reduct/client.h>
#include <string>
#include "../include/common.h"
#include "../include/utilities.h"

using reduct::Error;
using reduct::IBucket;
using reduct::IClient;

int main() {
  constexpr const char *ENTRY = "raw__rsense_color_image_raw_compressed";
  constexpr int MAX_FRAMES = 5;

  auto start_time = common::parse_time(common::START_STR);
  auto stop_time = common::parse_time(common::STOP_STR);

  const std::string when = std::string(R"({"$each_t":"5s","$limit":)") +
                           std::to_string(MAX_FRAMES) + "}";

  auto client = IClient::Build(common::URL, {.api_token = common::TOKEN});
  auto [bucket, b_err] = client->GetBucket(common::BUCKET);
  assert(b_err == Error::kOk);

  std::filesystem::create_directories("img");

  int idx = 0;
  auto q_err = bucket->Query(
      ENTRY, start_time, stop_time, {.when = when},
      [&](const IBucket::ReadableRecord &rec) {
        auto [blob, r_err] = rec.ReadAll();
        assert(r_err == Error::kOk);

        std::string ext = (rec.content_type.find("png") != std::string::npos)
                              ? ".png"
                              : ".jpg";
        std::string fname = "img/frame_" + std::to_string(idx++) + ext;

        std::ofstream of(fname, std::ios::binary);
        of.write(blob.data(), static_cast<std::streamsize>(blob.size()));
        of.close();

        std::cout << "Saved " << fname
                  << " | ts=" << rec.timestamp.time_since_epoch().count()
                  << " | size=" << rec.size
                  << " | content_type=" << rec.content_type << "\n";
        return true;
      });

  assert(q_err == Error::kOk);
  return 0;
}

/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/common/io/IoStatistics.h"

#include "velox/common/file/File.h"

#include <gtest/gtest.h>

namespace facebook::velox::dwio::common {

// Testing stream producing deterministic data. The byte at offset is
// the low byte of 'seed_' + offset.
class TestReadFile : public velox::ReadFile {
 public:
  TestReadFile(
      uint64_t seed,
      uint64_t length,
      std::shared_ptr<io::IoStatistics> ioStats)
      : seed_(seed), length_(length), ioStats_(std::move(ioStats)) {}

  uint64_t size() const override {
    return length_;
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override {
    const uint64_t content = offset + seed_;
    const uint64_t available = std::min(length_ - offset, length);
    int fill;
    for (fill = 0; fill < available; ++fill) {
      reinterpret_cast<char*>(buffer)[fill] = content + fill;
    }
    return std::string_view(static_cast<const char*>(buffer), fill);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    auto res = ReadFile::preadv(offset, buffers);
    ++numIos_;
    return res;
  }

  // Asserts that 'bytes' is as would be read from 'offset'.
  void checkData(const void* bytes, uint64_t offset, int32_t size) {
    for (auto i = 0; i < size; ++i) {
      char expected = seed_ + offset + i;
      ASSERT_EQ(expected, reinterpret_cast<const char*>(bytes)[i])
          << " at " << offset + i;
    }
  }

  uint64_t memoryUsage() const override {
    VELOX_NYI();
  }

  bool shouldCoalesce() const override {
    VELOX_NYI();
  }

  int64_t numIos() const {
    return numIos_;
  }

  std::string getName() const override {
    return "<TestReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    VELOX_NYI();
  }

 private:
  const uint64_t seed_;
  const uint64_t length_;
  std::shared_ptr<io::IoStatistics> ioStats_;
  mutable std::atomic<int64_t> numIos_{0};
};

} // namespace facebook::velox::dwio::common

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

#include "velox/common/base/Exceptions.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/TreeOfLosers.h"

#include <folly/Random.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>

namespace facebook::velox::exec::test {

// Testing value struct for merge.
class MergeValue {
 public:
  MergeValue() = default;

  explicit MergeValue(uint32_t value) : value_(value) {}

  uint32_t value() const {
    return value_;
  }

  int64_t payload() const {
    return payload_;
  }

 private:
  uint32_t value_ = 0;
  uint64_t payload_ = 11;
};

// Testing source for sorted streams for merging.
class TestingStream final : public MergeStream {
 public:
  explicit TestingStream(std::vector<uint32_t>&& numbers)
      : numbers_(std::move(numbers)) {}

  bool hasData() const final {
    if (!numbers_.empty()) {
      current();
      return true;
    }
    return false;
  }

  MergeValue* current() const {
    if (numbers_.empty()) {
      return nullptr;
    }
    if (!currentValid_) {
      currentValid_ = true;
      current_ = MergeValue(numbers_.back());
    }
    return &current_;
  }

  // Removes the first value. hasData() must have returned true before
  // this. hasData() must again be called after this to check for end.
  void pop() {
    numbers_.pop_back();
    currentValid_ = false;
  }

  bool operator<(const MergeStream& other) const final {
    return current_.value() <
        static_cast<const TestingStream&>(other).current_.value();
  }

 private:
  // True if 'current_' is initialized.
  mutable bool currentValid_{false};
  mutable MergeValue current_;

  // The reversed sequence of values 'this represents.
  std::vector<uint32_t> numbers_;
};

// Test data for merging.
struct TestData {
  // Globally sorted sequence of test keys.
  std::vector<uint32_t> data;

  // 'data' divided over multiple locally sorted runs encapsulated in Sources.
  std::vector<std::unique_ptr<TestingStream>> sources;
};

class MergeTestBase {
 public:
  void seed(int32_t seed) {
    rng_.seed(seed);
  }

  // Makes 'numRuns' sorted streams totalling 'numValues' entries.
  TestData makeTestData(int32_t numValues, int32_t numRuns) {
    TestData data;
    data.data.reserve(numValues);
    for (auto i = 0; i < numValues; ++i) {
      data.data.push_back(folly::Random::rand32(rng_));
    }
    std::vector<std::vector<uint32_t>> runs;
    int32_t offset = 0;
    for (auto i = 0; i < numRuns; ++i) {
      int size = i == numRuns - 1 ? data.data.size() - offset
                                  : data.data.size() / numRuns;
      runs.emplace_back();
      runs.back().insert(
          runs.back().begin(),
          data.data.begin() + offset,
          data.data.begin() + offset + size);
      std::sort(
          runs.back().begin(),
          runs.back().end(),
          [](uint32_t left, uint32_t right) { return left > right; });
      offset += size;
    }
    std::sort(data.data.begin(), data.data.end());

    for (auto& run : runs) {
      data.sources.push_back(std::make_unique<TestingStream>(std::move(run)));
    }
    return data;
  }

  // Reads the data in 'testData.runs' using the merging class MergeType. Checks
  // that the results match the globally sorted data in 'testData' if check is
  // true.
  template <typename MergeType>
  static void test(const TestData& testData, bool check) {
    std::vector<std::unique_ptr<TestingStream>> sources;
    for (auto& source : testData.sources) {
      sources.push_back(std::make_unique<TestingStream>(*source));
    }
    MergeType merge(std::move(sources));
    if (check) {
      for (auto expected : testData.data) {
        auto source = merge.next();
        if (!source) {
          FAIL() << "Premature end in merged stream";
        }
        auto result = source->current()->value();
        ASSERT_EQ(result, expected);
        source->pop();
      }
      ASSERT_FALSE(merge.next());
    } else {
      TestingStream* result;
      while ((result = merge.next())) {
        result->pop();
      }
    }
  }

 protected:
  folly::Random::DefaultGenerator rng_;
};
} // namespace facebook::velox::exec::test

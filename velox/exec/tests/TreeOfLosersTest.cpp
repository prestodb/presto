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
#include "velox/exec/TreeOfLosers.h"
#include "velox/common/base/Exceptions.h"

#include <folly/Random.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <optional>

using namespace facebook::velox;

class TreeOfLosersTest : public testing::Test {
 protected:
  void SetUp() override {
    rng_.seed(1);
  }

  folly::Random::DefaultGenerator rng_;
};

struct Value {
  uint32_t value;

  bool operator<(const Value& other) {
    return value < other.value;
  }
  bool operator==(const Value& other) {
    return value == other.value;
  }
};

class Source {
 public:
  Source(std::vector<uint32_t>&& numbers) : numbers_(std::move(numbers)) {}

  bool atEnd() const {
    return numbers_.empty();
  }

  Value next() {
    VELOX_CHECK(!numbers_.empty());
    auto value = numbers_.back();
    numbers_.pop_back();
    return Value{value};
  }

 private:
  std::vector<uint32_t> numbers_;
};

int compare(Value left, Value right) {
  return left < right ? -1 : left == right ? 0 : 1;
}

TEST_F(TreeOfLosersTest, merge) {
  constexpr int32_t kNumValues = 1000000;
  constexpr int32_t kNumRuns = 17;
  std::vector<uint32_t> data;
  for (auto i = 0; i < kNumValues; ++i) {
    data.push_back(folly::Random::rand32(rng_));
  }
  std::vector<std::vector<uint32_t>> runs;
  int32_t offset = 0;
  for (auto i = 0; i < kNumRuns; ++i) {
    int size =
        i == kNumRuns - 1 ? data.size() - offset : data.size() / kNumRuns;
    runs.emplace_back();
    runs.back().insert(
        runs.back().begin(),
        data.begin() + offset,
        data.begin() + offset + size);
    std::sort(
        runs.back().begin(),
        runs.back().end(),
        [](uint32_t left, uint32_t right) { return left > right; });
    offset += size;
  }
  std::sort(data.begin(), data.end());

  std::vector<std::unique_ptr<Source>> sources;
  for (auto& run : runs) {
    sources.push_back(std::make_unique<Source>(std::move(run)));
  }
  TreeOfLosers<Value, Source> tree(std::move(sources));
  for (auto expected : data) {
    auto result = tree.next(compare);
    ASSERT_EQ(result.value().value, expected);
  }
  ASSERT_FALSE(tree.next(compare).has_value());
}

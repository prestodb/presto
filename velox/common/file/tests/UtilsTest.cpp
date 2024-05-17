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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/file/Utils.h"
#include "velox/common/file/tests/TestUtils.h"

using namespace ::testing;
using namespace ::facebook::velox;
using namespace ::facebook::velox::file::utils;
using namespace ::facebook::velox::tests::utils;
using ::facebook::velox::common::Region;
using Regions = std::vector<Region>;

namespace {

class MockShouldCoalesce {
 public:
  virtual ~MockShouldCoalesce() = default;

  MOCK_METHOD(
      bool,
      shouldCoalesceRegions,
      (const Region& a, const Region& b),
      (const));
};

class ShouldCoalesceWrapper {
 public:
  explicit ShouldCoalesceWrapper(MockShouldCoalesce& shouldCoalesce)
      : shouldCoalesce_(shouldCoalesce) {}

  bool operator()(const Region& a, const Region& b) const {
    return shouldCoalesce_.shouldCoalesceRegions(a, b);
  }

 private:
  MockShouldCoalesce& shouldCoalesce_;
};

template <typename Iter, typename ShouldCoalesce>
std::vector<std::pair<size_t, size_t>>
coalescedIndices(Iter begin, Iter end, ShouldCoalesce& shouldCoalesce) {
  std::vector<std::pair<size_t, size_t>> result;
  ShouldCoalesceWrapper shouldCoalesceWrapper{shouldCoalesce};
  for (auto [coalescedBegin, coalescedEnd] :
       CoalesceRegions(begin, end, shouldCoalesceWrapper)) {
    result.push_back(
        {std::distance(begin, coalescedBegin),
         std::distance(begin, coalescedEnd)});
  }
  return result;
}

bool willCoalesceIfDistanceLE(
    uint64_t distance,
    const Region& regionA,
    const Region& regionB,
    uint64_t expectedCoalescedBytes) {
  uint64_t coalescedBytes = 0;
  const bool willCoalesce =
      CoalesceIfDistanceLE(distance, &coalescedBytes)(regionA, regionB);
  EXPECT_EQ(coalescedBytes, expectedCoalescedBytes);
  return willCoalesce;
}

auto getReader(
    bool chained,
    std::string content =
        "aaaaabbbbbcccccdddddeeeeefffffggggghhhhhiiiiijjjjjkkkkk") {
  return [buf = std::move(content), chained](uint64_t offset, uint64_t size) {
    if (offset + size > buf.size()) {
      VELOX_FAIL("read is too big.");
    }
    if (size == 0) {
      VELOX_FAIL("empty read not allowed");
    }
    if (chained) {
      auto head = folly::IOBuf::copyBuffer(
          buf.data() + offset,
          /* size */ 1,
          /* headroom */ 0,
          /* minTailRoom*/ 0);

      for (size_t i = 1; i < size; ++i) {
        head->appendToChain(folly::IOBuf::copyBuffer(
            buf.data() + offset + i,
            /* size */ 1,
            /* headroom */ 0,
            /* minTailRoom*/ 0));
      }
      return head;
    } else {
      return folly::IOBuf::copyBuffer(
          buf.data() + offset, size, /* headroom */ 0, /* minTailRoom*/ 0);
    }
  };
}

} // namespace

TEST(CoalesceSegmentsTest, EmptyCase) {
  const Regions r = {};

  MockShouldCoalesce shouldCoalesce;
  EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(_, _)).Times(0);

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {};
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeAll) {
  const Regions r = {{0, 4}, {4, 4}, {8, 1}, {9, 2}, {11, 3}};

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < r.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(true));
  }

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {{0UL, 5UL}};
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeNone) {
  const Regions r = {{0, 4}, {4, 4}, {8, 1}, {9, 2}, {11, 3}};

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < r.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(false));
  }

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 1UL}, {1UL, 2UL}, {2UL, 3UL}, {3UL, 4UL}, {4UL, 5UL}};
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeOdd) {
  const Regions r = {{0, 4}, {4, 4}, {8, 1}, {9, 2}, {11, 3}};

  auto isOdd = [](size_t i) { return i % 2 == 1; };

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < r.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(isOdd(i)));
  }

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 2UL}, {2UL, 4UL}, {4UL, 5UL}};
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeEven) {
  const Regions r = {{{0, 4}, {4, 4}, {8, 1}, {9, 2}, {11, 3}}};
  auto isEven = [](size_t i) { return i % 2 == 0; };

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < r.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(isEven(i)));
  }

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 1UL}, {1UL, 3UL}, {3UL, 5UL}};
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceIfDistanceLETest, MultipleCases) {
  EXPECT_TRUE(willCoalesceIfDistanceLE(0, {0, 1}, {1, 1}, 0));
  EXPECT_FALSE(willCoalesceIfDistanceLE(0, {0, 1}, {2, 1}, 0));

  EXPECT_TRUE(willCoalesceIfDistanceLE(1, {0, 1}, {2, 1}, 1));

  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 1}, {1, 1}, 0));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {10, 1}, {11, 1}, 0));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 10}, {19, 5}, 9));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 10}, {20, 5}, 10));
  EXPECT_FALSE(willCoalesceIfDistanceLE(10, {0, 10}, {21, 5}, 0));

  EXPECT_TRUE(willCoalesceIfDistanceLE(0, {0, 0}, {0, 1}, 0));
}

TEST(CoalesceIfDistanceLETest, MultipleSegments) {
  uint64_t coalescedBytes = 0;
  auto willCoalesce = CoalesceIfDistanceLE(10, &coalescedBytes);
  EXPECT_TRUE(willCoalesce({0, 1}, {1, 1})); // 0
  EXPECT_TRUE(willCoalesce({10, 1}, {11, 1})); // 0
  EXPECT_TRUE(willCoalesce({0, 10}, {19, 5})); // 9
  EXPECT_TRUE(willCoalesce({0, 10}, {20, 5})); // 10
  EXPECT_FALSE(willCoalesce({0, 10}, {21, 5})); // 0
  EXPECT_EQ(coalescedBytes, 19);
}

TEST(CoalesceIfDistanceLETest, SupportsNullArgument) {
  EXPECT_NO_THROW(CoalesceIfDistanceLE(10, nullptr)({0, 10}, {20, 5})); // 10
}

TEST(CoalesceIfDistanceLETest, SegmentsMustBeSorted) {
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {1, 1}, {0, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {1, 1}, {0, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {1000, 1}, {2, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {1000, 1}, {2, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
}

TEST(CoalesceIfDistanceLETest, SegmentsCantOverlap) {
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 1}, {0, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 1}, {0, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 2}, {1, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 2}, {1, 1}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 2}, {1, 2}, 0),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 2}, {1, 2}, 0),
      ::facebook::velox::VeloxRuntimeError);
}

class ReadToIOBufsTest : public testing::TestWithParam<bool> {};

TEST_P(ReadToIOBufsTest, CanRead) {
  Regions r = {{0, 1}, {5, 1}, {10, 6}, {16, 5}};
  std::vector<folly::IOBuf> iobufs;
  iobufs.reserve(r.size());
  auto readToIOBufs = ReadToIOBufs(
      r.begin(), r.end(), std::back_inserter(iobufs), getReader(GetParam()));
  readToIOBufs();

  ASSERT_EQ(iobufs.size(), r.size());

  EXPECT_EQ(
      iobufsToStrings(iobufs),
      (std::vector<std::string>{"a", "b", "cccccd", "dddde"}));
}

INSTANTIATE_TEST_SUITE_P(
    ReadToIOBufsSuite,
    ReadToIOBufsTest,
    ValuesIn(
        std::vector<bool /* Should generated chained IOBuf */>({false, true})));

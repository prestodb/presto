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

using namespace ::testing;
using namespace ::facebook::velox;
using namespace ::facebook::velox::file::utils;

namespace {

class MockShouldCoalesce {
 public:
  virtual ~MockShouldCoalesce() = default;

  MOCK_METHOD(
      bool,
      shouldCoalesce,
      (const ReadFile::Segment* a, const ReadFile::Segment* b),
      (const));
};

class ShouldCoalesceWrapper {
 public:
  explicit ShouldCoalesceWrapper(MockShouldCoalesce& shouldCoalesce)
      : shouldCoalesce_(shouldCoalesce) {}

  bool operator()(const ReadFile::Segment* a, const ReadFile::Segment* b)
      const {
    return shouldCoalesce_.shouldCoalesce(a, b);
  }

 private:
  MockShouldCoalesce& shouldCoalesce_;
};

struct Result {
  std::vector<std::string> buffers;
  std::vector<ReadFile::Segment> segments;
  std::vector<ReadFile::Segment*> segmentPtrs;
};

Result getSegments(
    std::vector<std::string> buffers,
    const std::unordered_set<size_t>& skip = {}) {
  Result result;
  result.buffers = std::move(buffers);
  uint64_t lastOffset = 0;
  size_t i = 0;
  for (auto& buffer : result.buffers) {
    if (skip.count(i++) == 0) {
      result.segments.emplace_back(ReadFile::Segment{
          lastOffset, folly::Range<char*>(&buffer[0], buffer.size()), {}});
    }
    lastOffset += buffer.size();
  }
  for (auto& segment : result.segments) {
    result.segmentPtrs.emplace_back(&segment);
  }
  return result;
}

template <typename Iter, typename ShouldCoalesce>
std::vector<std::pair<size_t, size_t>>
coalescedIndices(Iter begin, Iter end, ShouldCoalesce& shouldCoalesce) {
  std::vector<std::pair<size_t, size_t>> result;
  ShouldCoalesceWrapper shouldCoalesceWrapper{shouldCoalesce};
  for (auto [coalescedBegin, coalescedEnd] :
       CoalesceSegments(begin, end, shouldCoalesceWrapper)) {
    result.push_back(
        {std::distance(begin, coalescedBegin),
         std::distance(begin, coalescedEnd)});
  }
  return result;
}

bool willCoalesceIfDistanceLE(
    uint64_t distance,
    const std::pair<uint64_t, uint64_t>& segA,
    const std::pair<uint64_t, uint64_t>& segB) {
  std::string bufA(segA.second /* size */, '-');
  std::string bufB(segB.second /* size */, '-');
  ReadFile::Segment a{
      segA.first, folly::Range<char*>(bufA.data(), bufA.size()), {}};
  ReadFile::Segment b{
      segB.first, folly::Range<char*>(bufB.data(), bufB.size()), {}};
  return CoalesceIfDistanceLE(distance)(&a, &b);
}

} // namespace

TEST(CoalesceSegmentsTest, EmptyCase) {
  const auto testData = getSegments({});
  ASSERT_EQ(testData.segments.size(), testData.segmentPtrs.size());
  const auto& p = testData.segmentPtrs;

  MockShouldCoalesce shouldCoalesce;
  EXPECT_CALL(shouldCoalesce, shouldCoalesce(_, _)).Times(0);

  std::vector<std::pair<size_t, size_t>> expected = {};
  std::vector<std::pair<size_t, size_t>> result =
      coalescedIndices(p.cbegin(), p.cend(), shouldCoalesce);
  EXPECT_EQ(result, expected);
}

TEST(CoalesceSegmentsTest, MergeAll) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  ASSERT_EQ(testData.segments.size(), testData.segmentPtrs.size());
  const auto& p = testData.segmentPtrs;

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < p.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesce(p[i - 1], p[i]))
        .Times(1)
        .WillOnce(Return(true));
  }

  std::vector<std::pair<size_t, size_t>> expected = {{0UL, 5UL}};
  std::vector<std::pair<size_t, size_t>> result =
      coalescedIndices(p.cbegin(), p.cend(), shouldCoalesce);
  EXPECT_EQ(result, expected);
}

TEST(CoalesceSegmentsTest, MergeNone) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  ASSERT_EQ(testData.segments.size(), testData.segmentPtrs.size());
  const auto& p = testData.segmentPtrs;

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < p.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesce(p[i - 1], p[i]))
        .Times(1)
        .WillOnce(Return(false));
  }

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 1UL}, {1UL, 2UL}, {2UL, 3UL}, {3UL, 4UL}, {4UL, 5UL}};
  std::vector<std::pair<size_t, size_t>> result =
      coalescedIndices(p.cbegin(), p.cend(), shouldCoalesce);
  EXPECT_EQ(result, expected);
}

TEST(CoalesceSegmentsTest, MergeOdd) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  ASSERT_EQ(testData.segments.size(), testData.segmentPtrs.size());
  const auto& p = testData.segmentPtrs;

  auto isOdd = [](size_t i) { return i % 2 == 1; };

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < p.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesce(p[i - 1], p[i]))
        .Times(1)
        .WillOnce(Return(isOdd(i)));
  }

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 2UL}, {2UL, 4UL}, {4UL, 5UL}};
  std::vector<std::pair<size_t, size_t>> result =
      coalescedIndices(p.cbegin(), p.cend(), shouldCoalesce);
  EXPECT_EQ(result, expected);
}

TEST(CoalesceSegmentsTest, MergeEven) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  ASSERT_EQ(testData.segments.size(), testData.segmentPtrs.size());
  const auto& p = testData.segmentPtrs;
  auto isEven = [](size_t i) { return i % 2 == 0; };

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < p.size(); ++i) {
    EXPECT_CALL(shouldCoalesce, shouldCoalesce(p[i - 1], p[i]))
        .Times(1)
        .WillOnce(Return(isEven(i)));
  }

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 1UL}, {1UL, 3UL}, {3UL, 5UL}};
  std::vector<std::pair<size_t, size_t>> result =
      coalescedIndices(p.cbegin(), p.cend(), shouldCoalesce);
  EXPECT_EQ(result, expected);
}

TEST(CoalesceIfDistanceLETest, MultipleCases) {
  EXPECT_TRUE(willCoalesceIfDistanceLE(0, {0, 1}, {1, 1}));
  EXPECT_FALSE(willCoalesceIfDistanceLE(0, {0, 1}, {2, 1}));

  EXPECT_TRUE(willCoalesceIfDistanceLE(1, {0, 1}, {2, 1}));

  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 1}, {1, 1}));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {10, 1}, {11, 1}));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 10}, {19, 5}));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 10}, {20, 5}));
  EXPECT_FALSE(willCoalesceIfDistanceLE(10, {0, 10}, {21, 5}));

  EXPECT_TRUE(willCoalesceIfDistanceLE(0, {0, 0}, {0, 1}));
}

TEST(CoalesceIfDistanceLETest, SegmentsMustBeSorted) {
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {1, 1}, {0, 1}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {1, 1}, {0, 1}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {1000, 1}, {2, 1}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {1000, 1}, {2, 1}),
      ::facebook::velox::VeloxRuntimeError);
}

TEST(CoalesceIfDistanceLETest, SegmentsCantOverlap) {
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 1}, {0, 1}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 1}, {0, 1}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 2}, {1, 1}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 2}, {1, 1}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 2}, {1, 2}),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 2}, {1, 2}),
      ::facebook::velox::VeloxRuntimeError);
}

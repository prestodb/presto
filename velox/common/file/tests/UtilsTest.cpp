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

namespace {

class MockShouldCoalesce {
 public:
  virtual ~MockShouldCoalesce() = default;

  MOCK_METHOD(
      bool,
      shouldCoalesceSegments,
      (const ReadFile::Segment& a, const ReadFile::Segment& b),
      (const));

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

  bool operator()(const ReadFile::Segment& a, const ReadFile::Segment& b)
      const {
    return shouldCoalesce_.shouldCoalesceSegments(a, b);
  }

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
       CoalesceSegments(begin, end, shouldCoalesceWrapper)) {
    result.push_back(
        {std::distance(begin, coalescedBegin),
         std::distance(begin, coalescedEnd)});
  }
  return result;
}

enum class ComparisonType { SEGMENT, REGION };

bool willCoalesceIfDistanceLE(
    uint64_t distance,
    const std::pair<uint64_t, uint64_t>& rangeA,
    const std::pair<uint64_t, uint64_t>& rangeB,
    ComparisonType type) {
  if (type == ComparisonType::SEGMENT) {
    std::string bufA(rangeA.second /* size */, '-');
    std::string bufB(rangeB.second /* size */, '-');

    ReadFile::Segment segA{
        rangeA.first, folly::Range<char*>(bufA.data(), bufA.size()), {}};
    ReadFile::Segment segB{
        rangeB.first, folly::Range<char*>(bufB.data(), bufB.size()), {}};

    return CoalesceIfDistanceLE(distance)(segA, segB);
  } else {
    Region regA{rangeA.first, rangeA.second, {}};
    Region regB{rangeB.first, rangeB.second, {}};

    return CoalesceIfDistanceLE(distance)(regA, regB);
  }
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

std::vector<Region> toRegions(const std::vector<ReadFile::Segment>& segments) {
  std::vector<Region> regions;
  regions.reserve(segments.size());
  std::transform(
      segments.cbegin(),
      segments.cend(),
      std::back_inserter(regions),
      [](const auto& segment) {
        return Region(segment.offset, segment.buffer.size(), segment.label);
      });
  return regions;
}

} // namespace

TEST(CoalesceSegmentsTest, EmptyCase) {
  const auto testData = getSegments({});
  const auto& s = testData.segments;
  const auto r = toRegions(s);

  MockShouldCoalesce shouldCoalesce;
  EXPECT_CALL(shouldCoalesce, shouldCoalesceSegments(_, _)).Times(0);
  EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(_, _)).Times(0);

  std::vector<std::pair<size_t, size_t>> resultSegments =
      coalescedIndices(s.cbegin(), s.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(s.cbegin(), s.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {};
  EXPECT_EQ(resultSegments, expected);
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeAll) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  const auto& s = testData.segments;
  const auto r = toRegions(s);

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < s.size(); ++i) {
    EXPECT_CALL(
        shouldCoalesce, shouldCoalesceSegments(Ref(s[i - 1]), Ref(s[i])))
        .Times(1)
        .WillOnce(Return(true));
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(true));
  }

  std::vector<std::pair<size_t, size_t>> resultSegments =
      coalescedIndices(s.cbegin(), s.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {{0UL, 5UL}};
  EXPECT_EQ(resultSegments, expected);
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeNone) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  const auto& s = testData.segments;
  const auto r = toRegions(s);

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < s.size(); ++i) {
    EXPECT_CALL(
        shouldCoalesce, shouldCoalesceSegments(Ref(s[i - 1]), Ref(s[i])))
        .Times(1)
        .WillOnce(Return(false));
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(false));
  }

  std::vector<std::pair<size_t, size_t>> resultSegments =
      coalescedIndices(s.cbegin(), s.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 1UL}, {1UL, 2UL}, {2UL, 3UL}, {3UL, 4UL}, {4UL, 5UL}};
  EXPECT_EQ(resultSegments, expected);
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeOdd) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  const auto& s = testData.segments;
  const auto r = toRegions(s);

  auto isOdd = [](size_t i) { return i % 2 == 1; };

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < s.size(); ++i) {
    EXPECT_CALL(
        shouldCoalesce, shouldCoalesceSegments(Ref(s[i - 1]), Ref(s[i])))
        .Times(1)
        .WillOnce(Return(isOdd(i)));
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(isOdd(i)));
  }

  std::vector<std::pair<size_t, size_t>> resultSegments =
      coalescedIndices(s.cbegin(), s.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 2UL}, {2UL, 4UL}, {4UL, 5UL}};
  EXPECT_EQ(resultSegments, expected);
  EXPECT_EQ(resultRegions, expected);
}

TEST(CoalesceSegmentsTest, MergeEven) {
  const auto testData = getSegments({"aaaa", "bbbb", "c", "dd", "eee"});
  const auto& s = testData.segments;
  const auto r = toRegions(s);
  auto isEven = [](size_t i) { return i % 2 == 0; };

  MockShouldCoalesce shouldCoalesce;
  for (size_t i = 1; i < s.size(); ++i) {
    EXPECT_CALL(
        shouldCoalesce, shouldCoalesceSegments(Ref(s[i - 1]), Ref(s[i])))
        .Times(1)
        .WillOnce(Return(isEven(i)));
    EXPECT_CALL(shouldCoalesce, shouldCoalesceRegions(Ref(r[i - 1]), Ref(r[i])))
        .Times(1)
        .WillOnce(Return(isEven(i)));
  }

  std::vector<std::pair<size_t, size_t>> resultSegments =
      coalescedIndices(s.cbegin(), s.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> resultRegions =
      coalescedIndices(r.cbegin(), r.cend(), shouldCoalesce);

  std::vector<std::pair<size_t, size_t>> expected = {
      {0UL, 1UL}, {1UL, 3UL}, {3UL, 5UL}};
  EXPECT_EQ(resultSegments, expected);
  EXPECT_EQ(resultRegions, expected);
}

class CoalesceIfDistanceLETest : public testing::TestWithParam<ComparisonType> {
};

TEST_P(CoalesceIfDistanceLETest, MultipleCases) {
  EXPECT_TRUE(willCoalesceIfDistanceLE(0, {0, 1}, {1, 1}, GetParam()));
  EXPECT_FALSE(willCoalesceIfDistanceLE(0, {0, 1}, {2, 1}, GetParam()));

  EXPECT_TRUE(willCoalesceIfDistanceLE(1, {0, 1}, {2, 1}, GetParam()));

  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 1}, {1, 1}, GetParam()));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {10, 1}, {11, 1}, GetParam()));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 10}, {19, 5}, GetParam()));
  EXPECT_TRUE(willCoalesceIfDistanceLE(10, {0, 10}, {20, 5}, GetParam()));
  EXPECT_FALSE(willCoalesceIfDistanceLE(10, {0, 10}, {21, 5}, GetParam()));

  EXPECT_TRUE(willCoalesceIfDistanceLE(0, {0, 0}, {0, 1}, GetParam()));
}

TEST_P(CoalesceIfDistanceLETest, SegmentsMustBeSorted) {
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {1, 1}, {0, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {1, 1}, {0, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {1000, 1}, {2, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {1000, 1}, {2, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
}

TEST_P(CoalesceIfDistanceLETest, SegmentsCantOverlap) {
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 1}, {0, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 1}, {0, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 2}, {1, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 2}, {1, 1}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(0, {0, 2}, {1, 2}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_THROW(
      willCoalesceIfDistanceLE(10, {0, 2}, {1, 2}, GetParam()),
      ::facebook::velox::VeloxRuntimeError);
}

INSTANTIATE_TEST_SUITE_P(
    CoalesceIfDistanceLESuite,
    CoalesceIfDistanceLETest,
    ValuesIn(std::vector<ComparisonType>(
        {ComparisonType::SEGMENT, ComparisonType::REGION})));

class ReadToSegmentsTest : public testing::TestWithParam<bool> {};

TEST_P(ReadToSegmentsTest, CanReadToContiguousSegments) {
  auto testData = getSegments({"this", "is", "an", "awesome", "test"});
  ASSERT_EQ(testData.segments.size(), 5);
  const auto& s = testData.segments;

  auto readToSegments =
      ReadToSegments(s.begin(), s.end(), getReader(GetParam()));

  EXPECT_EQ(
      testData.buffers,
      (std::vector<std::string>{"this", "is", "an", "awesome", "test"}));

  readToSegments.read();

  EXPECT_EQ(
      testData.buffers,
      (std::vector<std::string>{"aaaa", "ab", "bb", "bbccccc", "dddd"}));
}

TEST_P(ReadToSegmentsTest, CanReadToNonContiguousSegments) {
  auto testData = getSegments({"this", "is", "an", "awesome", "test"}, {1, 3});
  ASSERT_EQ(testData.segments.size(), 3);
  const auto& s = testData.segments;

  auto readToSegments =
      ReadToSegments(s.begin(), s.end(), getReader(GetParam()));

  EXPECT_EQ(
      testData.buffers,
      (std::vector<std::string>{"this", "is", "an", "awesome", "test"}));

  readToSegments.read();

  EXPECT_EQ(
      testData.buffers,
      (std::vector<std::string>{"aaaa", "is", "bb", "awesome", "dddd"}));
}

TEST_P(ReadToSegmentsTest, NoSegmentsIsNoOp) {
  auto testData = getSegments({"a", "b"});
  ASSERT_EQ(testData.segments.size(), 2);
  const auto& s = testData.segments;

  // Set the desired read size to 0, but point to buffer to check that we don't
  // override
  testData.segments[0].buffer.reset(testData.buffers[0].data(), 0);
  testData.segments[1].buffer.reset(testData.buffers[1].data(), 0);

  auto readToSegments =
      ReadToSegments(s.begin(), s.end(), getReader(GetParam()));

  EXPECT_EQ(testData.buffers, (std::vector<std::string>{"a", "b"}));

  // No op
  readToSegments.read();

  EXPECT_EQ(testData.buffers, (std::vector<std::string>{"a", "b"}));
}

INSTANTIATE_TEST_SUITE_P(
    ReadToSegmentsSuite,
    ReadToSegmentsTest,
    ValuesIn(
        std::vector<bool /* Should generated chained IOBuf */>({false, true})));

class ReadToIOBufsTest : public testing::TestWithParam<bool> {};

TEST_P(ReadToIOBufsTest, CanRead) {
  std::vector<Region> r = {{0, 1, {}}, {5, 1, {}}, {10, 6}, {16, 5}};
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

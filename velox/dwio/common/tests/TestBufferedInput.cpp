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
#include "velox/dwio/common/BufferedInput.h"

using namespace facebook::velox::dwio::common;
using namespace ::testing;

namespace {

class ReadFileMock : public ::facebook::velox::ReadFile {
 public:
  virtual ~ReadFileMock() override = default;

  MOCK_METHOD(
      std::string_view,
      pread,
      (uint64_t offset, uint64_t length, void* FOLLY_NONNULL buf),
      (const, override));

  MOCK_METHOD(bool, shouldCoalesce, (), (const, override));
  MOCK_METHOD(uint64_t, size, (), (const, override));
  MOCK_METHOD(uint64_t, memoryUsage, (), (const, override));
  MOCK_METHOD(std::string, getName, (), (const, override));
  MOCK_METHOD(uint64_t, getNaturalReadSize, (), (const, override));
  MOCK_METHOD(
      void,
      preadv,
      (const std::vector<Segment>& segments),
      (const, override));
};

void expectPreads(
    ReadFileMock& file,
    std::string_view content,
    std::vector<std::pair<int, int>> reads) {
  EXPECT_CALL(file, getName()).WillRepeatedly(Return("mock_name"));
  EXPECT_CALL(file, size()).WillRepeatedly(Return(content.size()));
  for (auto [offset, size] : reads) {
    ASSERT_GE(content.size(), offset + size);
    EXPECT_CALL(file, pread(offset, size, _))
        .Times(1)
        .WillOnce(
            [content](uint64_t offset, uint64_t length, void* buf)
                -> std::string_view {
              memcpy(buf, content.data() + offset, length);
              return {content.data() + offset, length};
            });
  }
}

void expectPreadvs(
    ReadFileMock& file,
    std::string_view content,
    std::vector<std::pair<int, int>> reads) {
  EXPECT_CALL(file, getName()).WillRepeatedly(Return("mock_name"));
  EXPECT_CALL(file, size()).WillRepeatedly(Return(content.size()));
  EXPECT_CALL(file, preadv(_))
      .Times(1)
      .WillOnce([content,
                 reads](const std::vector<::facebook::velox::ReadFile::Segment>&
                            segments) {
        ASSERT_EQ(segments.size(), reads.size());
        for (size_t i = 0; i < reads.size(); ++i) {
          const auto& segment = segments[i];
          const auto& read = reads[i];
          ASSERT_EQ(segment.offset, read.first);
          ASSERT_EQ(segment.buffer.size(), read.second);
          ASSERT_LE(segment.offset + segment.buffer.size(), content.size());
          ASSERT_EQ(segment.offset, read.first);
          ASSERT_EQ(segment.buffer.size(), read.second);
          memcpy(
              segment.buffer.data(),
              content.data() + segment.offset,
              segment.buffer.size());
        }
      });
}

std::optional<std::string> getNext(SeekableInputStream& input) {
  const void* buf = nullptr;
  int32_t size;
  if (input.Next(&buf, &size)) {
    return std::string(
        static_cast<const char*>(buf), static_cast<size_t>(size));
  } else {
    return std::nullopt;
  }
}

} // namespace

TEST(TestBufferedInput, ZeroLengthStream) {
  auto readFile =
      std::make_shared<facebook::velox::InMemoryReadFile>(std::string());
  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  BufferedInput input(readFile, *pool);
  auto ret = input.enqueue({0, 0});
  EXPECT_NE(ret, nullptr);
  const void* buf = nullptr;
  int32_t size = 1;
  EXPECT_FALSE(ret->Next(&buf, &size));
  EXPECT_EQ(size, 0);
}

TEST(TestBufferedInput, UseRead) {
  std::string content = "hello";
  auto readFileMock = std::make_shared<ReadFileMock>();
  expectPreads(*readFileMock, content, {{0, 5}});
  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  // Use read: by default
  BufferedInput input(readFileMock, *pool);
  auto ret = input.enqueue({0, 5});
  ASSERT_NE(ret, nullptr);

  input.load(LogType::TEST);

  auto next = getNext(*ret);
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next.value(), content);
}

TEST(TestBufferedInput, UseVRead) {
  std::string content = "hello";
  auto readFileMock = std::make_shared<ReadFileMock>();
  expectPreadvs(*readFileMock, content, {{0, 5}});
  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  // Use vread
  BufferedInput input(
      readFileMock,
      *pool,
      MetricsLog::voidLog(),
      nullptr,
      10,
      /* wsVRLoad = */ true);
  auto ret = input.enqueue({0, 5});
  ASSERT_NE(ret, nullptr);

  input.load(LogType::TEST);

  auto next = getNext(*ret);
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(next.value(), content);
}

TEST(TestBufferedInput, WillMerge) {
  std::string content = "hello world";
  auto readFileMock = std::make_shared<ReadFileMock>();

  // Will merge because the distance is 1 and max distance to merge is 10.
  // Expect only one call.
  expectPreads(*readFileMock, content, {{0, 11}});

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  BufferedInput input(
      readFileMock,
      *pool,
      MetricsLog::voidLog(),
      nullptr,
      10, // Will merge if distance <= 10
      /* wsVRLoad = */ false);

  auto ret1 = input.enqueue({0, 5});
  auto ret2 = input.enqueue({6, 5});
  ASSERT_NE(ret1, nullptr);
  ASSERT_NE(ret2, nullptr);

  input.load(LogType::TEST);

  auto next1 = getNext(*ret1);
  ASSERT_TRUE(next1.has_value());
  EXPECT_EQ(next1.value(), "hello");

  auto next2 = getNext(*ret2);
  ASSERT_TRUE(next2.has_value());
  EXPECT_EQ(next2.value(), "world");
}

TEST(TestBufferedInput, WontMerge) {
  std::string content = "hello  world"; // two spaces
  auto readFileMock = std::make_shared<ReadFileMock>();

  // Won't merge because the distance is 2 and max distance to merge is 1.
  // Expect two calls
  expectPreads(*readFileMock, content, {{0, 5}, {7, 5}});

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  BufferedInput input(
      readFileMock,
      *pool,
      MetricsLog::voidLog(),
      nullptr,
      1, // Will merge if distance <= 1
      /* wsVRLoad = */ false);

  auto ret1 = input.enqueue({0, 5});
  auto ret2 = input.enqueue({7, 5});
  ASSERT_NE(ret1, nullptr);
  ASSERT_NE(ret2, nullptr);

  input.load(LogType::TEST);

  auto next1 = getNext(*ret1);
  ASSERT_TRUE(next1.has_value());
  EXPECT_EQ(next1.value(), "hello");

  auto next2 = getNext(*ret2);
  ASSERT_TRUE(next2.has_value());
  EXPECT_EQ(next2.value(), "world");
}

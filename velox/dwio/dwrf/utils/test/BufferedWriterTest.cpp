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

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/dwrf/utils/BufferedWriter.h"

using namespace ::testing;

namespace facebook::velox::dwrf::utils {
namespace {
using WriteFn = std::function<void(char*, size_t)>;
using GetFn = std::function<char(size_t)>;

class BufferedWriterTest : public testing::TestWithParam<bool> {
 protected:
  BufferedWriterTest()
      : usePool_(GetParam()),
        pool_(memory::memoryManager()->addLeafPool("BufferedWriterTest")) {}

  void SetUp() override {}

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  // Indicates test with buffered writer interface which takes memory pool or
  // not.
  const bool usePool_;
  const std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_P(BufferedWriterTest, Basic) {
  const int bufferSize = 1024;

  struct {
    // NOTE: if it is 0, then it is a flush op and if it is -1, then it is a
    // close op.
    std::vector<int> addBytes;
    bool expectedCrashed;
    int expectedFlushedBytes;

    std::string debugString() const {
      return fmt::format(
          "addBytes:{}, expectedCrashed:{}, expectedFlushedBytes:{}",
          folly::join(",", addBytes),
          expectedCrashed,
          expectedFlushedBytes);
    }
  } testSettings[] = {
      {{1, 1, 0}, false, 2},
      {{1, 0, 1}, true, 2},
      {{0, 1, 1, 0}, false, 2},
      {{0, 1, 0, 1}, true, 2},
      {{0, 1024}, false, 1024},
      {{1024}, false, 1024},
      {{1024, 0}, false, 1024},
      {{1024, 0, 1}, true, 1025},
      {{1024, 0, 1, 0}, false, 1025},
      {{1024, 1023}, true, 2047},
      {{1024, 1024}, false, 2048},
      {{1024, 0, 1024}, false, 2048},
      {{1024, 0, 1024, 0, 0}, false, 2048},

      {{1, 1, 0, -1}, false, 2},
      {{1, 0, 1, -1}, false, 2},
      {{0, 1, 1, 0, -1}, false, 2},
      {{0, 1, 0, 1, -1}, false, 2},
      {{0, 1024, -1}, false, 1024},
      {{1024, -1}, false, 1024},
      {{1024, 0, -1}, false, 1024},
      {{1024, 0, 1, -1}, false, 1025},
      {{1024, 0, 1, 0, -1}, false, 1025},
      {{1024, 1023, -1}, false, 2047},
      {{1024, 1024, -1}, false, 2048},
      {{1024, 0, 1024, -1}, false, 2048},
      {{1024, 0, 1024, 0, 0, -1}, false, 2048}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::unique_ptr<dwio::common::DataBuffer<char>> buffer;
    if (!usePool_) {
      buffer =
          std::make_unique<dwio::common::DataBuffer<char>>(*pool_, bufferSize);
    }
    size_t flushCount{0};
    auto writeFn = [&](char* buf, size_t size) { flushCount += size; };
    std::unique_ptr<BufferedWriter<char, WriteFn>> writer;
    if (usePool_) {
      writer = std::make_unique<BufferedWriter<char, WriteFn>>(
          *pool_, bufferSize, writeFn);
    } else {
      writer = std::make_unique<BufferedWriter<char, WriteFn>>(
          *pool_, bufferSize, writeFn);
    }
    ASSERT_EQ(
        writer->toString(),
        "BufferedWriter[pos[0] capacity[1024] closed[false]]");
    for (const auto bytes : testData.addBytes) {
      if (bytes == 0) {
        writer->flush();
        continue;
      }
      if (bytes == -1) {
        writer->close();
        continue;
      }
      for (int i = 0; i < bytes; ++i) {
        writer->add('a');
      }
    }
    if (testData.expectedCrashed) {
      EXPECT_DEATH(writer.reset(), "");
      writer->close();
    } else {
      writer.reset();
    }
    ASSERT_EQ(flushCount, testData.expectedFlushedBytes);
  }
}

TEST_P(BufferedWriterTest, flushFailureTest) {
  const int bufferSize = 1024;
  std::unique_ptr<dwio::common::DataBuffer<char>> buffer;
  if (!usePool_) {
    buffer =
        std::make_unique<dwio::common::DataBuffer<char>>(*pool_, bufferSize);
  }
  auto writeFailureInjectionFn = [&](char* buf, size_t size) {
    VELOX_FAIL("flushFailureTest");
  };
  std::unique_ptr<BufferedWriter<char, WriteFn>> writer;
  if (usePool_) {
    writer = std::make_unique<BufferedWriter<char, WriteFn>>(
        *pool_, bufferSize, writeFailureInjectionFn);
  } else {
    writer = std::make_unique<BufferedWriter<char, WriteFn>>(
        *pool_, bufferSize, writeFailureInjectionFn);
  }
  for (int i = 0; i < bufferSize - 1; ++i) {
    writer->add('a');
  }
  VELOX_ASSERT_THROW(writer->flush(), "flushFailureTest");
  writer->close();
}

TEST_P(BufferedWriterTest, bufferedWrite) {
  const int bufferSize = 1024;

  auto getFn = [&](size_t index) { return 'a'; };

  struct {
    int start;
    int end;

    std::string debugString() const {
      return fmt::format("start:{}, end:{}", start, end);
    }
  } testSettings[] = {
      {0, 1023},
      {0, 1024},
      {0, 1025},
      {0, 0},
      {0, 1},
      {20, 1023},
      {20, 1024},
      {20, 1025},
      {20, 20},
      {20, 21},
      {20, 1023 + 1024},
      {20, 1024 + 1024},
      {20, 1025 + 1024},
      {20, 20 + 1024},
      {20, 21 + 1024}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    size_t flushCount{0};
    auto writeFn = [&](char* buf, size_t size) { flushCount += size; };
    std::unique_ptr<BufferedWriter<char, WriteFn>> writer;
    if (usePool_) {
      bufferedWrite<char>(
          *pool_, bufferSize, testData.start, testData.end, getFn, writeFn);
    } else {
      std::unique_ptr<dwio::common::DataBuffer<char>> buffer =
          std::make_unique<dwio::common::DataBuffer<char>>(*pool_, bufferSize);
      bufferedWrite<char>(
          *buffer, testData.start, testData.end, getFn, writeFn);
    }
    ASSERT_EQ(flushCount, testData.end - testData.start);
  }
}

TEST_P(BufferedWriterTest, closeTest) {
  const int bufferSize = 1024;
  std::unique_ptr<dwio::common::DataBuffer<char>> buffer;
  if (!usePool_) {
    buffer =
        std::make_unique<dwio::common::DataBuffer<char>>(*pool_, bufferSize);
  }
  size_t flushCount{0};
  auto writeFn = [&](char* buf, size_t size) { flushCount += size; };
  std::unique_ptr<BufferedWriter<char, WriteFn>> writer;
  if (usePool_) {
    writer = std::make_unique<BufferedWriter<char, WriteFn>>(
        *pool_, bufferSize, writeFn);
  } else {
    writer = std::make_unique<BufferedWriter<char, WriteFn>>(*buffer, writeFn);
  }
  writer->add('a');
  ASSERT_EQ(
      writer->toString(),
      "BufferedWriter[pos[1] capacity[1024] closed[false]]");
  writer->close();
  ASSERT_EQ(flushCount, 1);
  // Verify all the calls on a closed object throw.
  VELOX_ASSERT_THROW(writer->close(), "");
  VELOX_ASSERT_THROW(writer->abort(), "");
  VELOX_ASSERT_THROW(writer->add('a'), "");
  VELOX_ASSERT_THROW(writer->flush(), "");
}

TEST_P(BufferedWriterTest, abortTest) {
  const int bufferSize = 1024;
  std::unique_ptr<dwio::common::DataBuffer<char>> buffer;
  if (!usePool_) {
    buffer =
        std::make_unique<dwio::common::DataBuffer<char>>(*pool_, bufferSize);
  }
  size_t flushCount{0};
  auto writeFn = [&](char* buf, size_t size) { flushCount += size; };
  std::unique_ptr<BufferedWriter<char, WriteFn>> writer;
  if (usePool_) {
    writer = std::make_unique<BufferedWriter<char, WriteFn>>(
        *pool_, bufferSize, writeFn);
  } else {
    writer = std::make_unique<BufferedWriter<char, WriteFn>>(*buffer, writeFn);
  }
  writer->add('a');
  ASSERT_EQ(
      writer->toString(),
      "BufferedWriter[pos[1] capacity[1024] closed[false]]");
  writer->abort();
  // Verify nothing has been flushed on abort.
  ASSERT_EQ(flushCount, 0);
  // Verify all the calls on an abort object throw.
  VELOX_ASSERT_THROW(writer->close(), "");
  VELOX_ASSERT_THROW(writer->abort(), "");
  VELOX_ASSERT_THROW(writer->add('a'), "");
  VELOX_ASSERT_THROW(writer->flush(), "");
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    BufferedWriterTestSuite,
    BufferedWriterTest,
    testing::ValuesIn({false, true}));
} // namespace
} // namespace facebook::velox::dwrf::utils

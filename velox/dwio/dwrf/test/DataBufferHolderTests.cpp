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
#include "velox/dwio/dwrf/common/DataBufferHolder.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::memory;

TEST(DataBufferHolderTests, InputCheck) {
  auto pool = addDefaultLeafMemoryPool();
  ASSERT_THROW((DataBufferHolder{*pool, 0}), exception::LoggedException);
  ASSERT_THROW(
      (DataBufferHolder{*pool, 1024, 2048}), exception::LoggedException);
  ASSERT_THROW(
      (DataBufferHolder{*pool, 1024, 1024, 1.1f}), exception::LoggedException);

  { DataBufferHolder holder{*pool, 1024}; }
  { DataBufferHolder holder{*pool, 1024, 512}; }
  { DataBufferHolder holder{*pool, 1024, 512, 3.0f}; }
}

TEST(DataBufferHolderTests, TakeAndGetBuffer) {
  auto pool = addDefaultLeafMemoryPool();
  MemorySink sink{*pool, 1024};
  DataBufferHolder holder{*pool, 1024, 0, 2.0f, &sink};
  DataBuffer<char> buffer{*pool, 512};
  std::memset(buffer.data(), 'a', 512);
  holder.take(buffer);
  ASSERT_EQ(holder.size(), 512);
  ASSERT_EQ(sink.size(), 512);
  std::memset(buffer.data(), 'b', 512);
  holder.take(buffer);
  ASSERT_EQ(holder.size(), 1024);
  ASSERT_EQ(sink.size(), 1024);
  auto data = sink.getData();
  for (size_t i = 0; i < 512; ++i) {
    ASSERT_EQ(data[i], 'a');
  }
  for (size_t i = 512; i < 1024; ++i) {
    ASSERT_EQ(data[i], 'b');
  }
  ASSERT_EQ(holder.getBuffers().size(), 0);
}

TEST(DataBufferHolderTests, TruncateBufferHolder) {
  auto pool = addDefaultLeafMemoryPool();
  DataBufferHolder holder{*pool, 1024};
  constexpr size_t BUF_SIZE = 10;
  DataBuffer<char> buffer{*pool, BUF_SIZE};
  std::memset(buffer.data(), 'a', BUF_SIZE);
  for (int32_t i = 0; i < 12; i++) {
    holder.take(buffer);
  }
  ASSERT_EQ(holder.size(), 120);
  ASSERT_EQ(holder.getBuffers().size(), 12);

  constexpr size_t TRUNCATED_SIZE = 43;
  holder.truncate(TRUNCATED_SIZE);
  ASSERT_EQ(holder.size(), TRUNCATED_SIZE);
  ASSERT_EQ(holder.getBuffers().size(), 5);

  size_t curSize = 0;
  for (auto& buf : holder.getBuffers()) {
    size_t expected = std::min(BUF_SIZE, TRUNCATED_SIZE - curSize);
    ASSERT_EQ(expected, buf.size());
    curSize += buf.size();
  }
}

TEST(DataBufferHolderTests, TakeAndGetBufferNoOutput) {
  auto pool = addDefaultLeafMemoryPool();
  DataBufferHolder holder{*pool, 1024};
  DataBuffer<char> buffer{*pool, 512};
  std::memset(buffer.data(), 'a', 512);
  holder.take(buffer);
  ASSERT_EQ(holder.size(), 512);
  std::memset(buffer.data(), 'b', 512);
  holder.take(buffer);
  ASSERT_EQ(holder.size(), 1024);

  DataBuffer<char> output{*pool, 128};
  holder.spill(output);
  ASSERT_EQ(output.size(), 1024);
  for (size_t i = 0; i < 512; ++i) {
    ASSERT_EQ(output.data()[i], 'a');
  }
  for (size_t i = 512; i < 1024; ++i) {
    ASSERT_EQ(output.data()[i], 'b');
  }

  auto& buffers = holder.getBuffers();
  ASSERT_EQ(buffers.size(), 2);
  for (size_t i = 0; i < 2; ++i) {
    auto& buf = buffers.at(i);
    ASSERT_EQ(buf.size(), 512);
    ASSERT_EQ(buf.data()[i], 'a' + i);
  }
}

TEST(DataBufferHolderTests, Reset) {
  auto pool = addDefaultLeafMemoryPool();
  DataBufferHolder holder{*pool, 1024};
  DataBuffer<char> buffer{*pool, 512};
  std::memset(buffer.data(), 'a', 512);
  holder.take(buffer);
  ASSERT_EQ(holder.size(), 512);
  holder.suppress();
  ASSERT_TRUE(holder.isSuppressed());
  holder.reset();
  ASSERT_EQ(holder.size(), 0);
  ASSERT_FALSE(holder.isSuppressed());
}

TEST(DataBufferHolderTests, TryResize) {
  auto pool = addDefaultLeafMemoryPool();
  DataBufferHolder holder{*pool, 1024, 128};

  auto runTest = [&pool, &holder](
                     uint64_t size,
                     uint64_t capacity,
                     uint64_t headerSize,
                     uint64_t increment,
                     bool expected,
                     uint64_t expectedSize) {
    DataBuffer<char> buffer{*pool, size};
    ASSERT_EQ(size, buffer.size());
    if (capacity > size) {
      buffer.reserve(capacity);
    }
    ASSERT_EQ(capacity, buffer.capacity());
    ASSERT_EQ(holder.tryResize(buffer, headerSize, increment), expected);
    ASSERT_EQ(buffer.size(), expectedSize);
    ASSERT_EQ(buffer.size(), buffer.capacity());
  };

  // initial size 0
  runTest(0, 0, 0, 1, true, 128);

  runTest(0, 256, 0, 1, true, 256);

  runTest(0, 1024, 0, 1, true, 1024);

  runTest(0, 0, 0, 512, true, 512);

  runTest(0, 256, 0, 512, true, 512);

  runTest(0, 256, 0, 2048, true, 1024);

  // initial size in range
  runTest(256, 256, 0, 1, true, 512);

  runTest(256, 256, 0, 257, true, 1024);

  runTest(256, 256, 0, 769, true, 1024);

  runTest(256, 512, 0, 1, true, 512);

  runTest(256, 512, 0, 257, true, 1024);

  // initial size already at max
  runTest(1024, 1024, 0, 512, false, 1024);

  size_t headerSize = 3;

  // initial size 0
  runTest(0, 0, headerSize, 1, true, 128 + headerSize);

  runTest(0, 256 + headerSize, headerSize, 1, true, 256 + headerSize);

  runTest(0, 1024 + headerSize, headerSize, 1, true, 1024 + headerSize);

  runTest(0, 0, headerSize, 512, true, 512 + headerSize);

  runTest(0, 256, headerSize, 512, true, 512 + headerSize);

  runTest(0, 256, headerSize, 2048, true, 1024 + headerSize);

  // initial size in range
  runTest(
      256 + headerSize,
      256 + headerSize,
      headerSize,
      1,
      true,
      512 + headerSize);

  runTest(
      256 + headerSize,
      256 + headerSize,
      headerSize,
      257,
      true,
      1024 + headerSize);

  runTest(
      256 + headerSize,
      256 + headerSize,
      headerSize,
      769,
      true,
      1024 + headerSize);

  runTest(
      256 + headerSize,
      512 + headerSize,
      headerSize,
      1,
      true,
      512 + headerSize);

  runTest(
      256 + headerSize,
      512 + headerSize,
      headerSize,
      257,
      true,
      1024 + headerSize);

  // initial size already at max
  runTest(
      1024 + headerSize,
      1024 + headerSize,
      headerSize,
      1,
      false,
      1024 + headerSize);
}

TEST(DataBufferHolderTests, TestGrowRatio) {
  auto pool = addDefaultLeafMemoryPool();
  DataBufferHolder holder{*pool, 1024, 16, 4.0f};
  DataBuffer<char> buffer{*pool, 16};
  ASSERT_TRUE(holder.tryResize(buffer, 0, 1));
  ASSERT_EQ(buffer.size(), 64);
}

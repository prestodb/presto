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

#include "velox/dwio/dwrf/common/OutputStream.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/test/OrcTest.h"

#include <gtest/gtest.h>

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::memory;
using namespace facebook::velox::dwrf;

TEST(BufferedOutputStream, blockAligned) {
  auto pool = addDefaultLeafMemoryPool();
  MemorySink memSink(*pool, 1024);

  uint64_t block = 10;
  DataBufferHolder holder{*pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  BufferedOutputStream bufStream(holder);
  for (int32_t i = 0; i < 100; ++i) {
    char* buf;
    int32_t len;
    ASSERT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
    ASSERT_EQ(10, len);
    for (int32_t j = 0; j < 10; ++j) {
      buf[j] = static_cast<char>('a' + j);
    }
  }

  bufStream.flush();
  ASSERT_EQ(1000, memSink.size());
  for (int32_t i = 0; i < 1000; ++i) {
    ASSERT_EQ(memSink.getData()[i], 'a' + i % 10);
  }
}

TEST(BufferedOutputStream, blockNotAligned) {
  auto pool = addDefaultLeafMemoryPool();
  MemorySink memSink(*pool, 1024);

  uint64_t block = 10;
  DataBufferHolder holder{*pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  BufferedOutputStream bufStream(holder);

  char* buf;
  int32_t len;
  ASSERT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
  ASSERT_EQ(10, len);

  for (int32_t i = 0; i < 7; ++i) {
    buf[i] = static_cast<char>('a' + i);
  }

  bufStream.BackUp(3);
  bufStream.flush();

  ASSERT_EQ(7, memSink.size());
  for (int32_t i = 0; i < 7; ++i) {
    ASSERT_EQ(memSink.getData()[i], 'a' + i);
  }

  ASSERT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
  ASSERT_EQ(10, len);

  for (int32_t i = 0; i < 5; ++i) {
    buf[i] = static_cast<char>('a' + i);
  }

  bufStream.BackUp(5);
  bufStream.flush();

  ASSERT_EQ(12, memSink.size());
  for (int32_t i = 0; i < 7; ++i) {
    ASSERT_EQ(memSink.getData()[i], 'a' + i);
  }

  for (int32_t i = 0; i < 5; ++i) {
    ASSERT_EQ(memSink.getData()[i + 7], 'a' + i);
  }
}

TEST(BufferedOutputStream, protoBufSerialization) {
  auto pool = addDefaultLeafMemoryPool();
  MemorySink memSink(*pool, 1024);

  uint64_t block = 10;
  DataBufferHolder holder{*pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  BufferedOutputStream bufStream(holder);

  proto::PostScript ps;
  ps.set_footerlength(197934);
  ps.set_compression(proto::ZLIB);
  ps.set_writerversion(123);

  ASSERT_TRUE(ps.SerializeToZeroCopyStream(&bufStream));
  bufStream.flush();
  ASSERT_EQ(ps.ByteSizeLong(), memSink.size());

  proto::PostScript ps2;
  ps2.ParseFromArray(memSink.getData(), static_cast<int32_t>(memSink.size()));

  ASSERT_EQ(ps.footerlength(), ps2.footerlength());
  ASSERT_EQ(ps.compression(), ps2.compression());
  ASSERT_EQ(ps.writerversion(), ps2.writerversion());
}

TEST(BufferedOutputStream, increaseSize) {
  auto pool = addDefaultLeafMemoryPool();
  MemorySink memSink(*pool, 1024);

  uint64_t max = 512;
  uint64_t min = 16;
  DataBufferHolder holder{*pool, max, min, DEFAULT_PAGE_GROW_RATIO, &memSink};
  BufferedOutputStream bufStream(holder);

  char* buf;
  int32_t len;
  ASSERT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
  ASSERT_EQ(16, len);

  for (int32_t i = 0; i < len; ++i) {
    buf[i] = static_cast<char>('a' + i);
  }

  ASSERT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
  ASSERT_EQ(16, len);
  ASSERT_EQ(0, memSink.size());

  for (int32_t i = 0; i < len; ++i) {
    buf[i] = static_cast<char>('b' + i);
  }

  ASSERT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len));
  ASSERT_EQ(32, len);
  ASSERT_EQ(0, memSink.size());

  for (int32_t i = 0; i < len; ++i) {
    buf[i] = static_cast<char>('c' + i);
  }

  ASSERT_TRUE(bufStream.Next(reinterpret_cast<void**>(&buf), &len, 128));
  ASSERT_EQ(192, len);
  ASSERT_EQ(0, memSink.size());

  for (int32_t i = 0; i < len; ++i) {
    buf[i] = static_cast<char>('d' + i);
  }

  bufStream.flush();
  ASSERT_EQ(256, memSink.size());

  std::array<uint64_t, 4> expected = {16, 16, 32, 192};
  size_t pos = 0;
  for (size_t i = 0; i < expected.size(); ++i) {
    for (size_t j = 0; j < expected[i]; ++j) {
      ASSERT_EQ(
          memSink.getData()[pos++], static_cast<char>(('a' + i + j) % 0x100));
    }
  }
}

TEST(BufferedOutputStream, recordPosition) {
  auto pool = addDefaultLeafMemoryPool();
  MemorySink memSink(*pool, 1024);
  uint64_t block = 256;
  uint64_t initial = 128;
  DataBufferHolder holder{
      *pool, block, initial, DEFAULT_PAGE_GROW_RATIO, &memSink};
  BufferedOutputStream bufStream(holder);

  TestPositionRecorder recorder;
  EXPECT_EQ(bufStream.size(), 0);
  bufStream.recordPosition(recorder, 3, 2);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 1);
    EXPECT_EQ(pos.at(0), 0);
  }

  int32_t size;
  void* data;
  bufStream.Next(&data, &size);
  EXPECT_EQ(size, initial);
  recorder.addEntry();
  EXPECT_EQ(bufStream.size(), initial);
  bufStream.recordPosition(recorder, size, 100);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 1);
    EXPECT_EQ(pos.at(0), 100);
  }

  bufStream.Next(&data, &size);
  EXPECT_EQ(size, block - initial);
  recorder.addEntry();
  EXPECT_EQ(bufStream.size(), block);
  bufStream.recordPosition(recorder, size, 100);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 1);
    EXPECT_EQ(pos.at(0), initial + 100);
  }

  bufStream.Next(&data, &size);
  EXPECT_EQ(size, block);
  recorder.addEntry();
  EXPECT_EQ(bufStream.size(), block + block);
  bufStream.recordPosition(recorder, size, 100);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 1);
    EXPECT_EQ(pos.at(0), block + 100);
  }

  recorder.addEntry();
  bufStream.recordPosition(recorder, size, 100, 4);
  auto& pos = recorder.getPositions(4);
  EXPECT_EQ(pos.size(), 1);
  EXPECT_EQ(pos.at(0), block + 100);
}

TEST(AppendOnlyBufferedStream, Basic) {
  auto pool = addDefaultLeafMemoryPool();
  MemorySink memSink(*pool, 1024);
  uint64_t block = 10;
  DataBufferHolder holder{*pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  auto bufStream = std::make_unique<BufferedOutputStream>(holder);
  AppendOnlyBufferedStream appendable(std::move(bufStream));

  std::array<char, 1024> data;
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = 'a' + i % 26;
  }

  appendable.write(data.data(), 173);
  ASSERT_EQ(appendable.size(), 173);

  TestPositionRecorder recorder;
  appendable.recordPosition(recorder);
  auto& pos = recorder.getPositions();
  ASSERT_EQ(pos.size(), 1);
  ASSERT_EQ(pos.at(0), 173);

  appendable.flush();
  ASSERT_EQ(memSink.size(), 173);
}

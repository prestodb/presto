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

#include "velox/common/base/Nulls.h"
#include "velox/dwio/dwrf/common/RLEv1.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/test/OrcTest.h"

#include <folly/Random.h>
#include <gtest/gtest.h>

const int32_t DEFAULT_MEM_STREAM_SIZE = 1024 * 1024; // 1M

using namespace facebook::velox::dwio::common;

namespace facebook::velox::dwrf {

void generateData(
    uint64_t numValues,
    int64_t start,
    int64_t delta,
    bool random,
    int64_t* data,
    uint64_t numNulls = 0,
    uint64_t* nulls = nullptr) {
  if (numNulls != 0 && nulls != nullptr) {
    if (numNulls == numValues) {
      memset(nulls, bits::kNullByte, bits::nbytes(numValues));
    } else {
      memset(nulls, bits::kNotNullByte, bits::nbytes(numValues));
      while (numNulls > 0) {
        uint64_t pos = folly::Random::rand32(numValues);
        if (!bits::isBitNull(nulls, pos)) {
          bits::setNull(nulls, pos);
          --numNulls;
        }
      }
    }
  }

  for (uint64_t i = 0; i < numValues; ++i) {
    if (!nulls || !bits::isBitNull(nulls, i)) {
      if (!random) {
        data[i] = start + delta * static_cast<int64_t>(i);
      } else {
        data[i] = folly::Random::rand32();
      }
    }
  }
}

template <bool isSigned, typename Type>
void decodeAndVerify(
    const MemorySink& memSink,
    Type* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  RleDecoderV1<isSigned> decoder(
      std::make_unique<SeekableArrayInputStream>(
          memSink.getData(), memSink.size()),
      true,
      dwio::common::INT_BYTE_SIZE);

  int64_t* decodedData = new int64_t[numValues];
  decoder.next(decodedData, numValues, nulls);

  for (uint64_t i = 0; i < numValues; ++i) {
    if (!nulls || !bits::isBitNull(nulls, i)) {
      EXPECT_EQ(data[i], decodedData[i]);
    }
  }

  delete[] decodedData;
}

class RleEncoderV1Test : public testing::Test {};

TEST(RleEncoderV1Test, encodeMinAndMax) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<false> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  auto data = folly::make_array(INT64_MIN, INT64_MAX, INT64_MIN);
  encoder.add(data.data(), Ranges::of(0, 2), nullptr);
  EXPECT_TRUE(encoder.isOverflow);

  encoder.add(data.data(), Ranges::of(2, 3), nullptr);
  EXPECT_TRUE(encoder.isOverflow);
  encoder.flush();

  decodeAndVerify<false>(memSink, data.data(), 2, nullptr);
}

TEST(RleEncoderV1Test, encodeMinAndMaxint32) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<true> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  auto data = folly::make_array(INT32_MIN, INT32_MAX, INT32_MIN);
  encoder.add(data.data(), Ranges::of(0, 2), nullptr);
  EXPECT_FALSE(encoder.isOverflow);

  encoder.add(data.data(), Ranges::of(2, 3), nullptr);
  EXPECT_FALSE(encoder.isOverflow);

  encoder.flush();

  decodeAndVerify<true>(memSink, data.data(), 2, nullptr);
}

TEST(RleEncoderV1Test, deltaIncreasingSequanceUnsigned) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<false> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  int64_t* data = new int64_t[1024];
  generateData(1024, 0, 1, false, data);
  encoder.add(data, Ranges::of(0, 1024), nullptr);
  encoder.flush();

  decodeAndVerify<false>(memSink, data, 1024, nullptr);
  delete[] data;
}

TEST(RleEncoderV1Test, deltaIncreasingSequanceUnsignedNull) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<false> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  uint64_t* nulls = new uint64_t[256];
  int64_t* data = new int64_t[1024];
  generateData(1024, 0, 1, false, data, 100, nulls);
  encoder.add(data, Ranges::of(0, 1024), nulls);
  encoder.flush();

  decodeAndVerify<false>(memSink, data, 1024, nulls);
  delete[] data;
  delete[] nulls;
}

TEST(RleEncoderV1Test, deltaDecreasingSequanceUnsigned) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<false> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  int64_t* data = new int64_t[1024];
  generateData(1024, 5000, -3, false, data);
  encoder.add(data, Ranges::of(0, 1024), nullptr);
  encoder.flush();

  decodeAndVerify<false>(memSink, data, 1024, nullptr);
  delete[] data;
}

TEST(RleEncoderV1Test, deltaDecreasingSequanceSigned) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<true> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  int64_t* data = new int64_t[1024];
  generateData(1024, 100, -3, false, data);
  encoder.add(data, Ranges::of(0, 1024), nullptr);
  encoder.flush();

  decodeAndVerify<true>(memSink, data, 1024, nullptr);
  delete[] data;
}

TEST(RleEncoderV1Test, deltaDecreasingSequanceSignedNull) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<true> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  uint64_t* nulls = new uint64_t[256];
  int64_t* data = new int64_t[1024];
  generateData(1024, 100, -3, false, data, 500, nulls);
  encoder.add(data, Ranges::of(0, 1024), nulls);
  encoder.flush();

  decodeAndVerify<true>(memSink, data, 1024, nulls);
  delete[] data;
  delete[] nulls;
}

TEST(RleEncoderV1Test, randomSequanceSigned) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<true> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  int64_t* data = new int64_t[1024];
  generateData(1024, 0, 0, true, data);
  encoder.add(data, Ranges::of(0, 1024), nullptr);
  encoder.flush();

  decodeAndVerify<true>(memSink, data, 1024, nullptr);
  delete[] data;
}

TEST(RleEncoderV1Test, allNull) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<true> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  uint64_t* nulls = new uint64_t[256];
  int64_t* data = new int64_t[1024];
  generateData(1024, 100, -3, false, data, 1024, nulls);
  encoder.add(data, Ranges::of(0, 1024), nulls);
  encoder.flush();

  decodeAndVerify<true>(memSink, data, 1024, nulls);
  delete[] data;
  delete[] nulls;
}

TEST(RleEncoderV1Test, recordPosition) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<true> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  constexpr size_t size = 256;
  std::array<int64_t, size> data;
  generateData(size, 100, 1, false, data.data());
  encoder.add(data.data(), Ranges::of(0, size), nullptr);

  TestPositionRecorder recorder;
  encoder.recordPosition(recorder);
  auto& pos = recorder.getPositions();
  EXPECT_EQ(pos.size(), 2);
  EXPECT_EQ(pos.at(1), size - 130);
}

TEST(RleEncoderV1Test, backfillPosition) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};

  RleEncoderV1<true> encoder(
      std::make_unique<BufferedOutputStream>(holder), true, 8);

  constexpr size_t size = 256;
  std::array<int64_t, size> data;
  generateData(size, 100, 1, false, data.data());
  encoder.add(data.data(), Ranges::of(0, size), nullptr);

  TestPositionRecorder recorder;
  encoder.recordPosition(recorder);
  {
    auto& pos = recorder.getPositions(0);
    ASSERT_EQ(pos.size(), 2);
    ASSERT_EQ(pos.at(1), size - 130);
  }
  recorder.addEntry();
  encoder.recordPosition(recorder, 1);
  {
    auto& pos = recorder.getPositions(1);
    ASSERT_EQ(pos.size(), 2);
    ASSERT_EQ(pos.at(1), size - 130);
  }
  std::array<int64_t, size * 2> moreData;
  generateData(size * 2, 200, 1, false, moreData.data());
  encoder.add(moreData.data(), Ranges::of(0, size * 2), nullptr);
  recorder.addEntry();
  encoder.recordPosition(recorder, 2);
  {
    auto& pos = recorder.getPositions(2);
    ASSERT_EQ(pos.size(), 2);
    ASSERT_EQ(pos.at(1), 122);
  }
  encoder.recordPosition(recorder, 1);
  {
    auto& pos = recorder.getPositions(0);
    EXPECT_EQ(pos.size(), 2);
    EXPECT_EQ(pos.at(1), size - 130);
  }
  {
    auto& pos = recorder.getPositions(1);
    EXPECT_EQ(pos.size(), 4);
    EXPECT_EQ(pos.at(1), size - 130);
    EXPECT_EQ(pos.at(3), 122);
  }
  {
    auto& pos = recorder.getPositions(2);
    EXPECT_EQ(pos.size(), 2);
    EXPECT_EQ(pos.at(1), 122);
  }
}

} // namespace facebook::velox::dwrf

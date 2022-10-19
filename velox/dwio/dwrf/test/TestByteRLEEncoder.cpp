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

#include "velox/dwio/dwrf/common/ByteRLE.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"

#include <folly/Random.h>
#include <gtest/gtest.h>
#include <velox/common/memory/Memory.h>

using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;

const int32_t DEFAULT_MEM_STREAM_SIZE = 1024 * 1024; // 1M

void generateNotNull(uint64_t numValues, uint64_t numNulls, uint64_t* nulls) {
  if (numNulls != 0 && nulls != nullptr) {
    memset(nulls, bits::kNotNullByte, bits::nbytes(numValues));
    while (numNulls > 0) {
      uint64_t pos = static_cast<uint64_t>(folly::Random::rand32()) % numValues;
      if (!bits::isBitNull(nulls, pos)) {
        bits::setNull(nulls, pos);
        --numNulls;
      }
    }
  }
}

void generateData(
    uint64_t numValues,
    char* data,
    uint64_t numNulls = 0,
    uint64_t* nulls = nullptr) {
  generateNotNull(numValues, numNulls, nulls);
  for (uint64_t i = 0; i < numValues; ++i) {
    data[i] = static_cast<char>(folly::Random::rand32() % 256);
  }
}

void generateBoolData(
    uint64_t numValues,
    char* data,
    uint64_t numNulls = 0,
    uint64_t* nulls = nullptr) {
  generateNotNull(numValues, numNulls, nulls);
  for (uint64_t i = 0; i < numValues; ++i) {
    data[i] = static_cast<char>(folly::Random::rand32() % 2);
  }
}

void decodeAndVerify(
    const MemorySink& memSink,
    char* data,
    uint64_t numValues,
    uint64_t* nulls) {
  std::unique_ptr<SeekableInputStream> inStream(
      new SeekableArrayInputStream(memSink.getData(), memSink.size()));

  std::unique_ptr<ByteRleDecoder> decoder =
      createByteRleDecoder(std::move(inStream), EncodingKey{0, 0});

  char* decodedData = new char[numValues];
  decoder->next(decodedData, numValues, nulls);

  for (uint64_t i = 0; i < numValues; ++i) {
    if (!nulls || !bits::isBitNull(nulls, i)) {
      EXPECT_EQ(data[i], decodedData[i]);
    }
  }

  delete[] decodedData;
}

void decodeAndVerifyBoolean(
    const MemorySink& memSink,
    char* data,
    uint64_t numValues,
    uint64_t* nulls) {
  std::unique_ptr<SeekableInputStream> inStream(
      new SeekableArrayInputStream(memSink.getData(), memSink.size()));

  std::unique_ptr<ByteRleDecoder> decoder =
      createBooleanRleDecoder(std::move(inStream), EncodingKey{0, 0});

  char* decodedData = new char[bits::nbytes(numValues)];
  decoder->next(decodedData, numValues, nulls);

  for (uint64_t i = 0; i < numValues; ++i) {
    if (!nulls || !bits::isBitNull(nulls, i)) {
      bool expect = data[i] != 0;
      bool actual = bits::isBitSet(decodedData, i);
      EXPECT_EQ(expect, actual) << "at index " << i;
    }
  }

  delete[] decodedData;
}

TEST(ByteRleEncoder, random_chars) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  auto outStream = std::make_unique<BufferedOutputStream>(holder);

  std::unique_ptr<ByteRleEncoder> encoder =
      createByteRleEncoder(std::move(outStream));

  char* data = new char[102400];
  generateData(102400, data);
  encoder->add(data, common::Ranges::of(0, 102400), nullptr);
  encoder->flush();

  decodeAndVerify(memSink, data, 102400, nullptr);
  delete[] data;
}

TEST(ByteRleEncoder, random_chars_with_null) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  auto outStream = std::make_unique<BufferedOutputStream>(holder);

  std::unique_ptr<ByteRleEncoder> encoder =
      createByteRleEncoder(std::move(outStream));

  uint64_t* nulls = new uint64_t[1600];
  char* data = new char[102400];
  generateData(102400, data, 377, nulls);
  encoder->add(data, common::Ranges::of(0, 102400), nulls);
  encoder->flush();

  decodeAndVerify(memSink, data, 102400, nulls);
  delete[] data;
  delete[] nulls;
}

TEST(BooleanRleEncoder, random_bits_not_aligned) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  auto outStream = std::make_unique<BufferedOutputStream>(holder);

  std::unique_ptr<ByteRleEncoder> encoder =
      createBooleanRleEncoder(std::move(outStream));

  char* data = new char[1779];
  generateBoolData(1779, data);
  encoder->add(data, common::Ranges::of(0, 1779), nullptr);
  encoder->flush();

  decodeAndVerifyBoolean(memSink, data, 1779, nullptr);
  delete[] data;
}

TEST(BooleanRleEncoder, random_bits_aligned) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  auto outStream = std::make_unique<BufferedOutputStream>(holder);

  std::unique_ptr<ByteRleEncoder> encoder =
      createBooleanRleEncoder(std::move(outStream));

  char* data = new char[8000];
  generateBoolData(8000, data);
  encoder->add(data, common::Ranges::of(0, 8000), nullptr);
  encoder->flush();

  decodeAndVerifyBoolean(memSink, data, 8000, nullptr);
  delete[] data;
}

TEST(BooleanRleEncoder, random_bits_aligned_with_null) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  MemorySink memSink(pool, DEFAULT_MEM_STREAM_SIZE);

  uint64_t block = 1024;
  DataBufferHolder holder{pool, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  auto outStream = std::make_unique<BufferedOutputStream>(holder);

  std::unique_ptr<ByteRleEncoder> encoder =
      createBooleanRleEncoder(std::move(outStream));

  uint64_t* nulls = new uint64_t[125];
  char* data = new char[8000];
  generateBoolData(8000, data, 515, nulls);
  encoder->add(data, common::Ranges::of(0, 8000), nulls);
  encoder->flush();

  decodeAndVerifyBoolean(memSink, data, 8000, nulls);
  delete[] data;
  delete[] nulls;
}

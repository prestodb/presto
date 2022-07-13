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

#include <folly/Random.h>
#include "velox/common/base/Nulls.h"
#include "velox/dwio/common/IntDecoder.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/common/EncoderUtil.h"
#include "velox/dwio/dwrf/common/IntEncoder.h"
#include "velox/dwio/dwrf/test/OrcTest.h"

#include <gtest/gtest.h>

using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;

template <typename T, bool isSigned, bool vInt>
void testInts(std::function<T()> generator) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  constexpr size_t count = 10240;
  DataBuffer<T> buffer{pool, count};
  std::array<uint64_t, count / 64> nulls;
  for (size_t i = 0; i < count; ++i) {
    buffer[i] = generator();
    bits::setNull(nulls.data(), i, !(i % 2));
  }

  constexpr size_t capacity =
      count * (vInt ? folly::kMaxVarintLength64 : sizeof(T));
  MemorySink sink{pool, capacity};
  DataBufferHolder holder{pool, capacity, 0, DEFAULT_PAGE_GROW_RATIO, &sink};
  auto output = std::make_unique<BufferedOutputStream>(holder);
  auto encoder =
      createDirectEncoder<isSigned>(std::move(output), vInt, sizeof(T));

  encoder->add(buffer.data(), Ranges::of(0, count), nulls.data());

  TestPositionRecorder recorder;
  recorder.addEntry();
  recorder.addEntry();
  encoder->recordPosition(recorder);
  encoder->recordPosition(recorder, 1);

  EXPECT_EQ(recorder.getPositions(0).size(), 0);
  constexpr size_t expectedSize = capacity / 2;
  auto& pos = recorder.getPositions(1);
  EXPECT_EQ(pos.size(), 1);
  if (vInt) {
    EXPECT_GT(pos.at(0), 0);
  } else {
    EXPECT_EQ(pos.at(0), expectedSize);
  }
  auto& latestPos = recorder.getPositions();
  EXPECT_EQ(latestPos.size(), 1);
  if (vInt) {
    EXPECT_GT(latestPos.at(0), 0);
  } else {
    EXPECT_EQ(latestPos.at(0), expectedSize);
  }

  encoder->flush();
  EXPECT_EQ(sink.size(), pos.at(0));
  EXPECT_EQ(sink.size(), latestPos.at(0));

  auto input = std::make_unique<SeekableArrayInputStream>(
      sink.getData(), expectedSize, expectedSize);
  auto decoder =
      createDirectDecoder<isSigned>(std::move(input), vInt, sizeof(T));

  std::array<int64_t, count / 2> vals;
  decoder->next(vals.data(), count / 2, nullptr);
  for (size_t i = 0; i < count / 2; ++i) {
    ASSERT_EQ(buffer[i * 2 + 1], static_cast<T>(vals[i]));
  }

  input = std::make_unique<SeekableArrayInputStream>(
      sink.getData(), expectedSize, 100);
  decoder = createDirectDecoder<isSigned>(std::move(input), vInt, sizeof(T));
  int32_t numRead = 0;
  int32_t stride = 1;
  while (numRead < count / 2) {
    if (numRead + stride >= count / 2) {
      break;
    }
    decoder->skipLongsFast(stride);
    numRead += stride;
    stride = (stride * 2 + 1) % 31;
    int64_t num;
    decoder->next(&num, 1, nullptr);
    ASSERT_EQ(buffer[numRead * 2 + 1], static_cast<T>(num));
    ++numRead;
  }

  // Bulk read consecutive.
  input = std::make_unique<SeekableArrayInputStream>(
      sink.getData(), expectedSize, 100);
  decoder = createDirectDecoder<isSigned>(std::move(input), vInt, sizeof(T));
  std::vector<uint64_t> result(count / 2);
  decoder->bulkRead(count / 2, result.data());
  for (auto i = 0; i < count / 2; ++i) {
    ASSERT_EQ(buffer[i * 2 + 1], result[i]);
  }
  std::vector<int32_t> rows;
  folly::Random::DefaultGenerator rng;
  rng.seed(1);
  for (auto i = 0; i < count / 2; ++i) {
    rows.push_back(i);
    auto random = folly::Random::rand32(rng) % 20;
    if (random < 13) {
      continue;
    }
    i += random - 10;
  }
  input = std::make_unique<SeekableArrayInputStream>(
      sink.getData(), expectedSize, 100);
  decoder = createDirectDecoder<isSigned>(std::move(input), vInt, sizeof(T));
  decoder->bulkReadRows(rows, result.data());
  for (auto i = 0; i < rows.size(); ++i) {
    ASSERT_EQ(buffer[rows[i] * 2 + 1], result[i]);
  }
}

TEST(TestDirect, fixedWidthShort) {
  testInts<int16_t, true, false>([]() -> int16_t {
    return static_cast<int16_t>(folly::Random::rand32());
  });
}

TEST(TestDirect, fixedWidthInt) {
  testInts<int32_t, true, false>(
      []() -> int32_t { return folly::Random::rand32(); });
}

TEST(TestDirect, fixedWidthLong) {
  testInts<int64_t, true, false>(
      []() -> int64_t { return folly::Random::rand64(); });
}

TEST(TestDirect, vIntSignedShort) {
  folly::Random::DefaultGenerator rng;
  rng.seed(2);
  testInts<int16_t, true, true>(
      [&]() -> int16_t { return folly::Random::rand32(rng); });
}

TEST(TestDirect, vIntSignedInt) {
  folly::Random::DefaultGenerator rng;
  rng.seed(2);
  testInts<int64_t, true, true>(
      [&]() -> int32_t { return folly::Random::rand32(rng); });
}

TEST(TestDirect, vIntSignedLong) {
  folly::Random::DefaultGenerator rng;
  rng.seed(2);
  int32_t count = 0;
  // The Generates test data with consecutive numbers within the same size range
  // to test bulk decode.
  testInts<int64_t, true, true>([&]() -> int64_t {
    auto mod = ++count % 33;
    auto numBytes = mod < 9 ? 1 : mod < 14 ? 2 : mod < 17 ? 3 : mod % 9;
    auto value = folly::Random::rand64(rng) & ((1UL << (7 * numBytes)) - 1);
    return folly::Random::rand32(rng) & 1 ? -value : value;
  });
}

TEST(TestDirect, vIntUnsignedLong) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1);
  int32_t count = 0;
  // The Generates test data with consecutive numbers within the same size range
  // to test bulk decode.
  testInts<int64_t, false, true>([&]() -> int64_t {
    auto mod = ++count % 33;
    auto numBytes = mod < 9 ? 1 : mod < 14 ? 2 : mod < 17 ? 3 : mod % 9;
    return folly::Random::rand64(rng) & ((1UL << (7 * numBytes)) - 1);
  });
}

template <bool isSigned>
void testCorruptedVarInts() {
  std::vector<uint8_t> invalidInt{
      0X01, // 1st number - 1
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0x81 // Invalid number.
  };
  auto input = std::make_unique<SeekableArrayInputStream>(
      invalidInt.data(), invalidInt.size());
  auto decoder =
      createDirectDecoder<isSigned>(std::move(input), true, sizeof(int64_t));

  std::array<int64_t, 2> vals;
  // First value is always read on the slow path.
  decoder->next(vals.data(), 1, nullptr);
  EXPECT_THROW(decoder->next(vals.data(), 1, nullptr);
               , exception::LoggedException);
}

TEST(TestDirect, corruptedInts) {
  testCorruptedVarInts<false>();
  testCorruptedVarInts<true>();
}

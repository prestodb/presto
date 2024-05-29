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

#include <iostream>

#include <gtest/gtest.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Semaphore.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/time/Timer.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/common/tests/BlockTest.h"

#include <folly/Random.h>

using namespace facebook::velox;
using namespace facebook::velox::wave;

constexpr int32_t kNumPartitionBlocks = 100;
struct PartitionRun {
  uint16_t* keys[kNumPartitionBlocks];
  int32_t numRows[kNumPartitionBlocks];
  int32_t* ranks[kNumPartitionBlocks];
  int32_t* partitionStarts[kNumPartitionBlocks];
  int32_t* partitionedRows[kNumPartitionBlocks];
};

class BlockTest : public testing::Test {
 protected:
  void SetUp() override {
    device_ = getDevice();
    setDevice(device_);
    allocator_ = getAllocator(device_);
    arena_ = std::make_unique<GpuArena>(1 << 28, allocator_);
  }

  void prefetch(Stream& stream, WaveBufferPtr buffer) {
    stream.prefetch(device_, buffer->as<char>(), buffer->capacity());
  }
  void testBoolToIndices(bool use256) {
    /// We make a set of 256 flags and corresponding 256 indices of true flags.
    constexpr int32_t kNumBlocks = 20480;
    constexpr int32_t kBlockSize = 256;
    constexpr int32_t kNumFlags = kBlockSize * kNumBlocks;
    auto flagsBuffer = arena_->allocate<uint8_t>(kNumFlags);
    auto indicesBuffer = arena_->allocate<int32_t>(kNumFlags);
    auto sizesBuffer = arena_->allocate<int32_t>(kNumBlocks);
    BlockTestStream stream;

    std::vector<int32_t> referenceIndices(kNumFlags);
    std::vector<int32_t> referenceSizes(kNumBlocks);
    uint8_t* flags = flagsBuffer->as<uint8_t>();
    for (auto i = 0ul; i < kNumFlags; ++i) {
      if ((i >> 8) % 17 == 0) {
        flags[i] = 0;
      } else if ((i >> 8) % 23 == 0) {
        flags[i] = 1;
      } else {
        flags[i] = (i * 1121) % 73 > 50;
      }
    }
    for (auto b = 0; b < kNumBlocks; ++b) {
      auto start = b * kBlockSize;
      int32_t counter = start;
      for (auto i = 0; i < kBlockSize; ++i) {
        if (flags[start + i]) {
          referenceIndices[counter++] = start + i;
        }
      }
      referenceSizes[b] = counter - start;
    }

    prefetch(stream, flagsBuffer);
    prefetch(stream, indicesBuffer);
    prefetch(stream, sizesBuffer);
    stream.wait();
    auto indicesPointers = arena_->allocate<void*>(kNumBlocks);
    auto flagsPointers = arena_->allocate<void*>(kNumBlocks);
    for (auto i = 0; i < kNumBlocks; ++i) {
      flagsPointers->as<uint8_t*>()[i] = flags + (i * kBlockSize);
      indicesPointers->as<int32_t*>()[i] =
          indicesBuffer->as<int32_t>() + (i * kBlockSize);
    }

    prefetch(stream, flagsBuffer);
    prefetch(stream, indicesBuffer);
    prefetch(stream, sizesBuffer);
    stream.wait();
    auto startMicros = getCurrentTimeMicro();
    if (use256) {
      stream.testBool256ToIndices(
          kNumBlocks,
          flagsPointers->as<uint8_t*>(),
          indicesPointers->as<int32_t*>(),
          sizesBuffer->as<int32_t>());

    } else {
      stream.testBoolToIndices(
          kNumBlocks,
          flagsPointers->as<uint8_t*>(),
          indicesPointers->as<int32_t*>(),
          sizesBuffer->as<int32_t>());
    }
    stream.wait();
    auto elapsed = getCurrentTimeMicro() - startMicros;
    for (auto b = 0; b < kNumBlocks; ++b) {
      auto* reference = referenceIndices.data() + b * kBlockSize;
      auto* actual = indicesBuffer->as<int32_t>() + b * kBlockSize;
      auto* referenceSizesData = referenceSizes.data();
      auto* actualSizes = sizesBuffer->as<int32_t>();
      ASSERT_EQ(
          0, ::memcmp(reference, actual, referenceSizes[b] * sizeof(int32_t)));
      ASSERT_EQ(referenceSizesData[b], actualSizes[b]);
    }
    std::cout << "Flags " << (use256 ? "256" : "") << " to indices: " << elapsed
              << "us, " << kNumFlags / static_cast<float>(elapsed) << " Mrows/s"
              << std::endl;

    auto temp = arena_->allocate<char>(
        BlockTestStream::boolToIndicesSize() * kNumBlocks);
    prefetch(stream, temp);
    prefetch(stream, flagsBuffer);
    prefetch(stream, indicesBuffer);
    prefetch(stream, sizesBuffer);
    stream.wait();

    startMicros = getCurrentTimeMicro();
    if (use256) {
      stream.testBool256ToIndicesNoShared(
          kNumBlocks,
          flagsPointers->as<uint8_t*>(),
          indicesPointers->as<int32_t*>(),
          sizesBuffer->as<int32_t>(),
          temp->as<char>());
    } else {
      stream.testBoolToIndicesNoShared(
          kNumBlocks,
          flagsPointers->as<uint8_t*>(),
          indicesPointers->as<int32_t*>(),
          sizesBuffer->as<int32_t>(),
          temp->as<char>());
    }
    stream.wait();
    elapsed = getCurrentTimeMicro() - startMicros;
    std::cout << "Flags " << (use256 ? "256" : "")
              << " to indices: " << " to indices no smem: " << elapsed << "us, "
              << kNumFlags / static_cast<float>(elapsed) << " Mrows/s"
              << std::endl;
  }

  void makePartitionRun(
      int32_t numRows,
      int32_t numPartitions,
      PartitionRun*& run,
      WaveBufferPtr& buffer) {
    auto rowsRounded = bits::roundUp(numRows, 8);
    auto partitionsRounded = bits::roundUp(numPartitions, 8);
    int64_t bytes = sizeof(PartitionRun) +
        kNumPartitionBlocks *
            (rowsRounded * sizeof(int32_t) * 4 +
             partitionsRounded * sizeof(int32_t));
    if (!buffer || buffer->capacity() < bytes) {
      buffer = arena_->allocate<char>(bytes);
    }
    run = buffer->as<PartitionRun>();
    auto chars = buffer->as<char>() + sizeof(PartitionRun);
    for (auto block = 0; block < kNumPartitionBlocks; ++block) {
      run->keys[block] = reinterpret_cast<uint16_t*>(chars);
      run->numRows[block] = numRows;
      chars += rowsRounded * sizeof(uint16_t);
      run->partitionStarts[block] = reinterpret_cast<int32_t*>(chars);
      chars += numPartitions * sizeof(int32_t);
      run->ranks[block] = reinterpret_cast<int32_t*>(chars);
      chars += sizeof(int32_t) * numRows;
      run->partitionedRows[block] = reinterpret_cast<int32_t*>(chars);
      chars += sizeof(int32_t) * numRows;
      for (auto i = 0; i < numRows; ++i) {
        run->keys[block][i] = (block + i * 2017) % numPartitions;
      }
    }
    VELOX_CHECK_LE(chars - buffer->as<char>(), bytes);
  }

  void checkPartitionRun(const PartitionRun& run, int32_t numPartitions) {
    // Check that every row is once in its proper partition.
    for (auto block = 0; block < kNumPartitionBlocks; ++block) {
      std::vector<bool> flags(run.numRows[block], false);
      for (auto part = 0; part < numPartitions; ++part) {
        for (auto i = (part == 0 ? 0 : run.partitionStarts[block][part - 1]);
             i < run.partitionStarts[block][part];
             ++i) {
          auto row = run.partitionedRows[block][i];
          EXPECT_LT(row, run.numRows[block]);
          EXPECT_FALSE(flags[row]);
          EXPECT_EQ(part, run.keys[block][row]);
          flags[row] = true;
        }
      }
      // Expect that all flags are set.
      for (auto i = 0; i < run.numRows[block]; ++i) {
        EXPECT_TRUE(flags[i]);
      }
    }
  }

  void scatterBitsTest(int32_t numBits, int32_t setPct, bool useSmem = false) {
    auto numWords = bits::nwords(numBits);
    auto maskBuffer = arena_->allocate<uint64_t>(numWords);
    auto sourceBuffer = arena_->allocate<uint64_t>(numWords + 1);
    auto resultBuffer = arena_->allocate<uint64_t>(numWords);
    auto smemBuffer = arena_->allocate<uint32_t>(
        BlockTestStream::scatterBitsSize(256) / sizeof(int32_t));
    VELOX_CHECK_LE(1000, numBits);
    memset(maskBuffer->as<char>(), 0, maskBuffer->capacity());
    BlockTestStream stream;

    auto maskData = maskBuffer->as<uint64_t>();
    folly::Random::DefaultGenerator rng;
    rng.seed(1);
    for (auto bit = 0; bit < numBits; ++bit) {
      if (folly::Random::rand32(rng) % 100 < setPct) {
        bits::setBit(maskData, bit);
      }
    }
    // Ranges of tens of set and unset bits.
    bits::fillBits(maskData, numBits - 130, numBits - 2, true);
    bits::fillBits(maskData, numBits - 601, numBits - 403, true);

    // Range of mostly set bits, 1/50 is not set.
    for (auto bit = numBits - 1000; bit > 400; --bit) {
      if (folly::Random::rand32(rng) % 50) {
        bits::setBit(maskData, bit);
      }
    }
    // Alternating groups of 5 bits with 0-3 bis set.
    for (auto i = 0; i < 305; i += 5) {
      auto numSet = (i / 5) % 4;
      for (auto j = 0; j < numSet; ++j) {
        bits::setBit(maskData, i + j, true);
      }
    }

    auto numInMask = bits::countBits(maskData, 0, numBits);
    auto* source = sourceBuffer->as<uint64_t>();
    uint64_t seed = 0x123456789abcdef0LL;
    for (auto i = 0; i < numWords; ++i) {
      source[i] = seed;
      seed *= 0x5cdf;
    }
    std::vector<uint64_t> reference(numWords);
    auto sourceAsChar = sourceBuffer->as<char>() + 1;
    uint64_t cpuTime = 0;
    {
      MicrosecondTimer t(&cpuTime);
      bits::scatterBits(
          numInMask,
          numBits,
          sourceAsChar,
          maskData,
          reinterpret_cast<char*>(reference.data()));
    }
    prefetch(stream, maskBuffer);
    prefetch(stream, resultBuffer);
    prefetch(stream, smemBuffer);
    prefetch(stream, sourceBuffer);
    stream.wait();
    uint64_t gpuTime = 0;
    {
      MicrosecondTimer t(&gpuTime);
      stream.scatterBits(
          numInMask,
          numBits,
          sourceAsChar,
          maskData,
          resultBuffer->as<char>(),
          smemBuffer->as<int32_t>());
      stream.wait();
    }
    auto resultAsChar = resultBuffer->as<char>();
    for (int32_t i = 0; i < numBits; ++i) {
      EXPECT_EQ(
          bits::isBitSet(reference.data(), i), bits::isBitSet(resultAsChar, i));
    }

    std::cout << fmt::format(
                     "scatterBits {} {}% set: cpu1t {} Mb/s gpu256t {} Mb/s",
                     numBits,
                     setPct,
                     numBits / static_cast<float>(cpuTime),
                     numBits / static_cast<float>(gpuTime))
              << std::endl;
  }

  Device* device_;
  GpuAllocator* allocator_;
  std::unique_ptr<GpuArena> arena_;
};

TEST_F(BlockTest, boolToIndices) {
  testBoolToIndices(false);
  testBoolToIndices(true);
}

TEST_F(BlockTest, shortRadixSort) {
  // We make a set of 8K uint16_t keys  and uint16_t values.
  constexpr int32_t kNumBlocks = 1024;
  constexpr int32_t kBlockSize = 1024;
  constexpr int32_t kValuesPerThread = 8;
  constexpr int32_t kValuesPerBlock = kBlockSize * kValuesPerThread;
  constexpr int32_t kNumValues = kBlockSize * kNumBlocks * kValuesPerThread;
  auto keysBuffer = arena_->allocate<uint16_t>(kNumValues);
  auto valuesBuffer = arena_->allocate<int16_t>(kNumValues);
  BlockTestStream stream;

  std::vector<uint16_t> referenceKeys(kNumValues);
  std::vector<uint16_t> referenceValues(kNumValues);
  uint16_t* keys = keysBuffer->as<uint16_t>();
  uint16_t* values = valuesBuffer->as<uint16_t>();
  for (auto i = 0; i < kNumValues; ++i) {
    keys[i] = i * 2017;
    values[i] = i;
  }

  for (auto b = 0; b < kNumBlocks; ++b) {
    auto start = b * kValuesPerBlock;
    std::vector<uint16_t> indices(kValuesPerBlock);
    std::iota(indices.begin(), indices.end(), 0);
    std::sort(indices.begin(), indices.end(), [&](auto left, auto right) {
      return keys[start + left] < keys[start + right];
    });
    for (auto i = 0; i < kValuesPerBlock; ++i) {
      referenceValues[start + i] = values[start + indices[i]];
    }
  }

  prefetch(stream, valuesBuffer);
  prefetch(stream, keysBuffer);

  auto keysPointers = arena_->allocate<void*>(kNumBlocks);
  auto valuesPointers = arena_->allocate<void*>(kNumBlocks);
  for (auto i = 0; i < kNumBlocks; ++i) {
    keysPointers->as<uint16_t*>()[i] = keys + (i * kValuesPerBlock);
    valuesPointers->as<uint16_t*>()[i] =
        valuesBuffer->as<uint16_t>() + (i * kValuesPerBlock);
  }
  auto keySegments = keysPointers->as<uint16_t*>();
  auto valueSegments = valuesPointers->as<uint16_t*>();
  prefetch(stream, keysPointers);
  prefetch(stream, valuesPointers);
  stream.wait();
  auto startMicros = getCurrentTimeMicro();
  stream.testSort16(kNumBlocks, keySegments, valueSegments);
  stream.wait();
  auto elapsed = getCurrentTimeMicro() - startMicros;
  for (auto b = 0; b < kNumBlocks; ++b) {
    ASSERT_EQ(
        0,
        ::memcmp(
            referenceValues.data() + b * kValuesPerBlock,
            valueSegments[b],
            kValuesPerBlock * sizeof(uint16_t)));
  }
  std::cout << "sort16: " << elapsed << "us, "
            << kNumValues / static_cast<float>(elapsed) << " Mrows/s"
            << std::endl;

  // Reset the test values for second test.
  for (auto i = 0; i < kNumValues; ++i) {
    keys[i] = i * 2017;
    values[i] = i;
  }
  auto temp =
      arena_->allocate<char>(kNumBlocks * BlockTestStream::sort16SharedSize());
  prefetch(stream, temp);
  prefetch(stream, valuesBuffer);
  prefetch(stream, keysBuffer);
  prefetch(stream, keysPointers);
  prefetch(stream, valuesPointers);
  stream.wait();
  startMicros = getCurrentTimeMicro();
  stream.testSort16NoShared(
      kNumBlocks, keySegments, valueSegments, temp->as<char>());
  stream.wait();
  elapsed = getCurrentTimeMicro() - startMicros;
  std::cout << "sort16 no shared: " << elapsed << "us, "
            << kNumValues / static_cast<float>(elapsed) << " Mrows/s"
            << std::endl;
}

TEST_F(BlockTest, partition) {
  // We make severl sets of keys and temp and result buffers. These
  // are in unified memory. We run the partition for all and check the
  // outcome on the host. We run at several different partition counts
  // and batch sizes. All experiments are submitted as kNumPartitionBlocks
  // concurrent thread blocks of 256 threads.
  BlockTestStream stream;
  std::vector<int32_t> partitionCounts = {1, 2, 32, 333, 1000, 8000};
  std::vector<int32_t> runSizes = {100, 1000, 10000, 30000};
  WaveBufferPtr buffer;
  PartitionRun* run;
  for (auto parts : partitionCounts) {
    for (auto rows : runSizes) {
      makePartitionRun(rows, parts, run, buffer);
      prefetch(stream, buffer);
      auto startMicros = getCurrentTimeMicro();
      stream.partitionShorts(
          kNumPartitionBlocks,
          run->keys,
          run->numRows,
          parts,
          run->ranks,
          run->partitionStarts,
          run->partitionedRows);
      stream.wait();
      auto time = getCurrentTimeMicro() - startMicros;
      std::cout << fmt::format(
                       "Partition {} batch={}  fanout={}  rate={} Mrows/s",
                       kNumPartitionBlocks,
                       rows,
                       parts,
                       kNumPartitionBlocks * static_cast<float>(rows) / time)
                << std::endl;
      checkPartitionRun(*run, parts);
    }
  }
}

TEST_F(BlockTest, scatterBits) {
  scatterBitsTest(1999, 0);
  scatterBitsTest(1999, 100);
  scatterBitsTest(1999, 42);
  scatterBitsTest(1999, 96);
  scatterBitsTest(12345999, 0);
  scatterBitsTest(12345999, 100);
  scatterBitsTest(12345999, 42);
  scatterBitsTest(12345999, 96);
}

TEST_F(BlockTest, nonNull) {
  constexpr int32_t kNumRows = 4 << 20;
  WaveBufferPtr nullsBuffer =
      arena_->allocate<uint64_t>(bits::nwords(kNumRows));
  WaveBufferPtr indicesBuffer = arena_->allocate<int32_t>(kNumRows);
  WaveBufferPtr rowsBuffer = arena_->allocate<int32_t>(kNumRows);
  WaveBufferPtr temp = arena_->allocate<int32_t>(1000);
  auto nulls = nullsBuffer->as<uint64_t>();
  const char* text =
      "les loopiettes, les loopiettes, comme elles sont chouettes"
      " parfois ci, parfois ca, parfois tu 'l sais pas";
  auto len = strlen(text) - 8;
  for (auto i = 0; i < kNumRows; i += 4096) {
    // Every run of 4096 bits, i.e. 64 words has 16 nulls, 16 nonnulls and 32
    // mostly nonn-nulls.
    auto run = nulls + (i / 64);
    memset(run, 0, 128);
    memset(run + 16, 0xff, 128);
    for (auto j = 32; j < 64; ++j) {
      run[j] = bits::hashMix(
                   1, *reinterpret_cast<const int64_t*>(text + (j % len))) |
          bits::hashMix(
                   0x1008, *reinterpret_cast<const int64_t*>(text + (i % len)));
    }
  }
  // Add a single non-null before the first accessed row.
  nulls[0] |= 1;
  auto rows = rowsBuffer->as<int32_t>();
  int32_t row = 2;
  int32_t numRows = 0;
  bool dense = true;
  while (row < kNumRows) {
    rows[numRows++] = row;

    if (dense) {
      ++row;
    } else {
      row += 1 + (numRows % 9) + (numRows % 111 == 0 ? 131 : 0);
    }
    if (numRows % 611 == 0) {
      dense = !dense;
    }
  }

  int32_t count = 0;
  int32_t counted = 0;
  auto nullsBelow = [&](int32_t i) {
    count += bits::countBits(nulls, counted, i);
    counted = i;
    return i - count;
  };

  auto result = indicesBuffer->as<int32_t>();
  auto rawTemp = temp->as<int32_t>();
  BlockTestStream stream;
  stream.nonNullIndex(
      reinterpret_cast<char*>(nulls), rows, numRows, result, rawTemp);
  stream.wait();
  for (auto i = 0; i < numRows; ++i) {
    if (result[i] == -1) {
      ASSERT_FALSE(bits::isBitSet(nulls, rows[i]))
          << "i=" << i << " rows[i]=" << rows[i];
    } else {
      ASSERT_EQ(result[i], rows[i] - nullsBelow(rows[i]))
          << "i=" << i << " rows[i]=" << rows[i];
      ;
    }
  }
}

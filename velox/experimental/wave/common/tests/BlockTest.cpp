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
  Device* device_;
  GpuAllocator* allocator_;
  std::unique_ptr<GpuArena> arena_;
};

TEST_F(BlockTest, boolToIndices) {
  /// We make a set of 256 flags and corresponding 256 indices of true flags.
  constexpr int32_t kNumBlocks = 20480;
  constexpr int32_t kBlockSize = 256;
  constexpr int32_t kNumFlags = kBlockSize * kNumBlocks;
  auto flagsBuffer = arena_->allocate<uint8_t>(kNumFlags);
  auto indicesBuffer = arena_->allocate<int32_t>(kNumFlags);
  auto sizesBuffer = arena_->allocate<int32_t>(kNumBlocks);
  auto timesBuffer = arena_->allocate<int64_t>(kNumBlocks);
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

  auto indicesPointers = arena_->allocate<void*>(kNumBlocks);
  auto flagsPointers = arena_->allocate<void*>(kNumBlocks);
  for (auto i = 0; i < kNumBlocks; ++i) {
    flagsPointers->as<uint8_t*>()[i] = flags + (i * kBlockSize);
    indicesPointers->as<int32_t*>()[i] =
        indicesBuffer->as<int32_t>() + (i * kBlockSize);
  }

  auto startMicros = getCurrentTimeMicro();
  stream.testBoolToIndices(
      kNumBlocks,
      flagsPointers->as<uint8_t*>(),
      indicesPointers->as<int32_t*>(),
      sizesBuffer->as<int32_t>(),
      timesBuffer->as<int64_t>());
  stream.wait();
  auto elapsed = getCurrentTimeMicro() - startMicros;
  for (auto b = 0; b < kNumBlocks; ++b) {
    ASSERT_EQ(
        0,
        ::memcmp(
            referenceIndices.data() + b * kBlockSize,
            indicesBuffer->as<int32_t>() + b * kBlockSize,
            referenceSizes[b] * sizeof(int32_t)));
    ASSERT_EQ(referenceSizes[b], sizesBuffer->as<int32_t>()[b]);
  }
  std::cout << "Flags to indices: " << elapsed << "us, "
            << kNumFlags / static_cast<float>(elapsed) << " Mrows/s"
            << std::endl;

  auto temp =
      arena_->allocate<char>(BlockTestStream::boolToIndicesSize() * kNumBlocks);
  startMicros = getCurrentTimeMicro();
  stream.testBoolToIndicesNoShared(
      kNumBlocks,
      flagsPointers->as<uint8_t*>(),
      indicesPointers->as<int32_t*>(),
      sizesBuffer->as<int32_t>(),
      timesBuffer->as<int64_t>(),
      temp->as<char>());
  stream.wait();
  elapsed = getCurrentTimeMicro() - startMicros;
  std::cout << "Flags to indices no smem: " << elapsed << "us, "
            << kNumFlags / static_cast<float>(elapsed) << " Mrows/s"
            << std::endl;
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
  auto timesBuffer = arena_->allocate<int64_t>(kNumBlocks);
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

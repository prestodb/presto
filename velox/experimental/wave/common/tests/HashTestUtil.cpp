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

#include "velox/experimental/wave/common/tests/HashTestUtil.h"
#include <fmt/format.h>
#include "velox/common/base/BitUtil.h"
#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/common/HashTable.h"

namespace facebook::velox::wave {

constexpr uint32_t kPrime32 = 1815531889;
inline uint32_t scale32(uint32_t n, uint32_t scale) {
  return (static_cast<uint64_t>(static_cast<uint32_t>(n)) * scale) >> 32;
}

// Returns the byte size for a GpuProbe with numRows as first, rounded row count
// as second.
std::pair<int64_t, int32_t> probeSize(HashRun& run) {
  int32_t roundedRows =
      bits::roundUp(run.numRows, run.blockSize * run.numRowsPerThread);
  return {
      sizeof(HashProbe) +
          // Column data and hash number array.
          (1 + run.numColumns) * roundedRows * sizeof(int64_t)
          // Pointers to column starts
          + sizeof(int64_t*) * run.numColumns
          // retry lists
          + 3 * sizeof(int32_t) * roundedRows +
          // numRows for each block.
          sizeof(int32_t) * roundedRows /
              (run.blockSize * run.numRowsPerThread) +
          // Temp space for partitioning.
          roundedRows * sizeof(int32_t) +
          // alignment padding
          256,
      roundedRows};
}

void fillHashTestInput(
    int32_t numRows,
    int32_t keyRange,
    int32_t powerOfTwo,
    int64_t counter,
    uint8_t numColumns,
    int64_t** columns,
    int32_t numHot,
    int32_t hotPct) {
  int32_t delta = counter & (powerOfTwo - 1);
  for (auto i = 0; i < numRows; ++i) {
    auto previous = columns[0][i];
    auto seed = (previous + delta + i) * kPrime32;
    if (hotPct && scale32(seed >> 32, 100) <= hotPct) {
      int32_t nth = scale32(seed, numHot);
      nth = std::min<int64_t>(
          keyRange - 1, nth * (static_cast<float>(keyRange) / nth));
      columns[0][i] = nth;
    } else {
      columns[0][i] = scale32(seed, keyRange);
    }
  }
  counter += numRows;
  for (auto c = 1; c < numColumns; ++c) {
    for (auto r = 0; r < numRows; ++r) {
      columns[c][r] = 1; // c + (r & 7);
    }
  }
}

void initializeHashTestInput(HashRun& run, GpuArena* arena) {
  auto [bytes, roundedRows] = probeSize(run);
  if (!arena) {
    run.isCpu = true;
    run.cpuData = std::make_unique<char[]>(bytes);
    run.input = run.cpuData.get();
  } else {
    run.isCpu = false;
    run.gpuData = arena->allocate<char>(bytes);
    run.input = run.gpuData->as<char>();
  }
  auto data = run.input;
  auto dataBegin = data;
  HashProbe* probe = new (data) HashProbe();
  run.probe = probe;
  data += sizeof(HashProbe);
  probe->numRows = reinterpret_cast<int32_t*>(data);
  data += bits::roundUp(
      sizeof(int32_t) * roundedRows / (run.numRowsPerThread * run.blockSize),
      8);
  if (!arena) {
    probe->numRows[0] = run.numRows;
  } else {
    run.numBlocks = roundedRows / (run.blockSize * run.numRowsPerThread);
    for (auto i = 0; i < run.numBlocks; ++i) {
      if (i == run.numBlocks - 1) {
        probe->numRows[i] =
            run.numRows - (i * run.blockSize * run.numRowsPerThread);
        break;
      }
      probe->numRows[i] = run.blockSize * run.numRowsPerThread;
      ;
    }
  }
  probe->numRowsPerThread = run.numRowsPerThread;
  probe->hashes = reinterpret_cast<uint64_t*>(data);
  data += sizeof(uint64_t) * roundedRows;
  probe->keys = data;
  data += sizeof(void*) * run.numColumns;
  probe->kernelRetries1 = reinterpret_cast<int32_t*>(data);
  data += sizeof(int32_t) * roundedRows;
  probe->kernelRetries2 = reinterpret_cast<int32_t*>(data);
  data += sizeof(int32_t) * roundedRows;
  probe->hostRetries = reinterpret_cast<int32_t*>(data);
  data += sizeof(int32_t) * roundedRows;
  for (auto i = 0; i < run.numColumns; ++i) {
    reinterpret_cast<int64_t**>(probe->keys)[i] =
        reinterpret_cast<int64_t*>(data);
    data += sizeof(int64_t) * roundedRows;
  }
  run.partitionTemp = reinterpret_cast<int32_t*>(data);
  data += bits::roundUp(sizeof(int32_t) * roundedRows, 8);
  VELOX_CHECK_LE(data - dataBegin, bytes);
}

void setupGpuTable(
    int32_t numSlots,
    int32_t maxRows,
    int64_t rowSize,
    GpuArena* arena,
    GpuHashTableBase*& table,
    WaveBufferPtr& buffer) {
  using FreeSetType = FreeSetBase<void*, 1024>;
  // GPU cache lines are 128 bytes divided in 4 separately loadable 32 byte
  // sectors.
  constexpr int32_t kAlignment = 128;
  int32_t numBuckets = bits::nextPowerOfTwo(numSlots / 4);
  int64_t bytes = sizeof(GpuHashTableBase) + sizeof(HashPartitionAllocator) +
      sizeof(FreeSetType) + sizeof(GpuBucketMembers) * numBuckets +
      maxRows * rowSize;
  buffer = arena->allocate<char>(bytes + kAlignment);
  table = buffer->as<GpuHashTableBase>();
  new (table) GpuHashTableBase();
  table->sizeMask = numBuckets - 1;
  char* data = reinterpret_cast<char*>(table + 1);
  table->allocators = reinterpret_cast<RowAllocator*>(data);
  auto allocatorBase =
      reinterpret_cast<HashPartitionAllocator*>(table->allocators);
  data += sizeof(HashPartitionAllocator);
  auto freeSet = reinterpret_cast<FreeSetType*>(data);
  new (freeSet) FreeSetType();
  data += sizeof(FreeSetType);
  // The buckets start at aligned address.
  data = reinterpret_cast<char*>(
      bits::roundUp(reinterpret_cast<uint64_t>(data), kAlignment));
  table->buckets = reinterpret_cast<GpuBucket*>(data);
  data += sizeof(GpuBucketMembers) * numBuckets;
  auto allocator = reinterpret_cast<HashPartitionAllocator*>(table->allocators);
  new (allocator)
      HashPartitionAllocator(data, maxRows * rowSize, rowSize, freeSet);
  table->partitionMask = 0;
  table->partitionShift = 0;
  memset(table->buckets, 0, sizeof(GpuBucketMembers) * (table->sizeMask + 1));
}

std::string HashRun::toString() const {
  std::stringstream out;
  std::string opLabel = testCase == HashTestCase::kUpdateSum1 ? "update sum1"
      : testCase == HashTestCase::kGroupSum1                  ? "groupSum1"
                                             : "update array_agg1";
  out << "===" << label << ":" << opLabel << " distinct=" << numDistinct
      << " rows=" << numRows << " (" << numBlocks << "x" << blockSize << "x"
      << numRowsPerThread << ") ";
  if (hotPct) {
    out << " skew " << hotPct << "% in " << numHot << " ";
  }
  auto sorted = scores;
  std::sort(sorted.begin(), sorted.end(), [](auto& left, auto& right) {
    return left.second < right.second;
  });
  float gb =
      numRows * sizeof(int64_t) * numColumns / static_cast<float>(1 << 30);
  for (auto& score : sorted) {
    out << std::endl
        << "  * "
        << fmt::format(
               " {}={:.2f} rps {:.2f} GB/s {} us {:.2f}x",
               score.first,
               numRows / (score.second / 1e6),
               gb / (score.second / 1e6),
               score.second,
               score.second / sorted[0].second);
  }
  return out.str();
}

void HashRun::addScore(const char* label, uint64_t micros) {
  scores.push_back(std::make_pair<std::string, float>(label, micros));
}
} // namespace facebook::velox::wave

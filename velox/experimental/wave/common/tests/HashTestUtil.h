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

#pragma once

#include <memory>
#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/HashTable.h"

namespace facebook::velox::wave {

/// Identifies operation being tested. A collection of representative hash table
/// ops like aggregates probes and builds with different functions and layouts.
enum class HashTestCase {
  // bigint sum.  Update only, no hash table.
  kUpdateSum1,
  // group by with bigint sum.
  kGroupSum1,
  // array_agg of bigint. Update only, no hash table.
  kUpdateArrayAgg1
};

/// Describes a hashtable benchmark case.
struct HashRun {
  // Label of test case. Describes what is done. The labels for different
  // implementations come from 'scores'.
  std::string label;
  // the operation being measured.
  HashTestCase testCase;
  // CPU/GPU measurement.
  bool isCpu;

  // Number of slots in table.
  int32_t numSlots{0};

  // Number of probe rows.
  int32_t numRows;

  // Number of distinct keys.
  int32_t numDistinct;

  // Number of distinct hot keys.
  int32_t numHot{0};

  // Percentage of hot keys over total keys. e.g. with 1000 distinct and 10 hot
  // and hotPct of 50, every second key will be one of 10 and the rest are
  // evenly spread over the 1000.
  int32_t hotPct{0};

  // Number of keys processed by each thread of each block.
  int32_t numRowsPerThread;

  int32_t blockSize{256};

  // Number of blocks of 'blockSize' threads.
  int32_t numBlocks;

  // Number of columns. Key is column 0.
  uint8_t numColumns{1};

  // Number of independent hash tables.
  int32_t numTables{1};

  // Result, labeled by implementation alternative.
  std::vector<std::pair<std::string, float>> scores;

  std::unique_ptr<char[]> cpuData;
  WaveBufferPtr gpuData;

  // Input data, either cpuData or gpuData.
  char* input;

  // Initialized probe params, contained in 'input'.
  HashProbe* probe;
  // One int per row, use for partitioning intermediates. Uninitialized.
  int32_t* partitionTemp;

  int32_t* partitionArgs;
  std::string toString() const;
  void addScore(const char* label, uint64_t micros);
  void clearScore() {
    scores.clear();
  }
};

void fillHashTestInput(
    int32_t numRows,
    int32_t keyRange,
    int32_t powerOfTwo,
    int64_t counter,
    uint8_t numColumns,
    int64_t** columns,
    int32_t numHot = 0,
    int32_t hotPct = 0);

void initializeHashTestInput(HashRun& run, GpuArena* arena);

void setupGpuTable(
    int32_t numSlots,
    int32_t maxRows,
    int64_t rowSize,
    GpuArena* arena,
    GpuHashTableBase*& table,
    WaveBufferPtr& buffer);

} // namespace facebook::velox::wave

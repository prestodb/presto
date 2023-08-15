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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>

#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_string(spiller_benchmark_path);
DECLARE_uint64(spiller_benchmark_max_spill_file_size);
DECLARE_uint64(spiller_benchmark_min_spill_run_size);
DECLARE_uint32(spiller_benchmark_num_input_vectors);
DECLARE_uint32(spiller_benchmark_input_vector_size);
DECLARE_string(spiller_benchmark_compression_kind);
DECLARE_uint32(spiller_benchmark_spill_executor_size);

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::memory;
using namespace facebook::velox::exec;

namespace facebook::velox::exec::test {
// This test measures the spill input overhead in spill join & probe.
class JoinSpillInputTest {
 public:
  JoinSpillInputTest() = default;

  /// Sets up the test.
  void setUp();

  /// Runs the test.
  void run();

  /// Prints out the measured test stats.
  void printStats();

  /// Cleans up the test.
  void cleanup();

 private:
  std::shared_ptr<MemoryPool> rootPool_;
  std::shared_ptr<MemoryPool> pool_;
  RowTypePtr rowType_;
  uint32_t numInputVectors_;
  uint32_t inputVectorSize_;
  std::unique_ptr<VectorFuzzer> vectorFuzzer_;
  std::vector<RowVectorPtr> rowVectors_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
  std::string spillDir_;
  std::shared_ptr<filesystems::FileSystem> fs_;
  std::unique_ptr<Spiller> spiller_;
  // Stats.
  uint64_t executionTimeUs_{0};
};
} // namespace facebook::velox::exec::test

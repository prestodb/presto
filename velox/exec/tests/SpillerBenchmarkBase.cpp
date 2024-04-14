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
#include <filesystem>
#include <fstream>
#include <iostream>

#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/tests/SpillerBenchmarkBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_string(
    spiller_benchmark_name,
    "SpillerBenchmarkTest",
    "The name of this benchmark");
DEFINE_string(
    spiller_benchmark_path,
    "",
    "The directory path for spilling. e.g. with '/path/to/dir' provided, spill "
    "file like '/path/to/dir/SpillerBenchmarkTest-spill-0-0-0' will be "
    "created.");
DEFINE_string(
    spiller_benchmark_compression_kind,
    "none",
    "The compression kind to compress spill rows before write to disk");
DEFINE_string(
    spiller_benchmark_spiller_type,
    "AGGREGATE_INPUT",
    "The spiller type name.");
DEFINE_uint32(
    spiller_benchmark_num_spill_vectors,
    10'000,
    "The number of vectors for spilling");
DEFINE_uint32(
    spiller_benchmark_num_key_columns,
    2,
    "The number of key columns");
DEFINE_uint32(
    spiller_benchmark_spill_executor_size,
    std::thread::hardware_concurrency(),
    "The spiller executor size in number of threads");
DEFINE_uint32(
    spiller_benchmark_spill_vector_size,
    100,
    "The number of rows per each spill vector");
DEFINE_uint64(
    spiller_benchmark_max_spill_file_size,
    2 << 30,
    "The max spill file size");
DEFINE_uint64(
    spiller_benchmark_min_spill_run_size,
    1 << 30,
    "The minimum spiller run size");
DEFINE_uint64(
    spiller_benchmark_write_buffer_size,
    1 << 20,
    "The spill write buffer size");

using namespace facebook::velox::memory;

namespace facebook::velox::exec::test {

void SpillerBenchmarkBase::setUp() {
  rootPool_ =
      memory::memoryManager()->addRootPool(FLAGS_spiller_benchmark_name);
  pool_ = rootPool_->addLeafChild(FLAGS_spiller_benchmark_name);

  rowType_ =
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {INTEGER(), BIGINT(), VARCHAR(), VARBINARY(), DOUBLE()});

  numInputVectors_ = FLAGS_spiller_benchmark_num_spill_vectors;
  inputVectorSize_ = FLAGS_spiller_benchmark_spill_vector_size;
  {
    VectorFuzzer::Options options;
    options.vectorSize = inputVectorSize_;
    vectorFuzzer_ = std::make_unique<VectorFuzzer>(options, pool_.get());
  }
  rowVectors_.reserve(numInputVectors_);
  for (int i = 0; i < numInputVectors_; ++i) {
    rowVectors_.push_back(vectorFuzzer_->fuzzRow(rowType_));
  }

  if (FLAGS_spiller_benchmark_spill_executor_size != 0) {
    executor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        FLAGS_spiller_benchmark_spill_executor_size,
        std::make_shared<folly::NamedThreadFactory>(
            FLAGS_spiller_benchmark_name));
  }

  if (FLAGS_spiller_benchmark_path.empty()) {
    tempDir_ = exec::test::TempDirectoryPath::create();
    spillDir_ = tempDir_->getPath();
  } else {
    spillDir_ = FLAGS_spiller_benchmark_path;
  }

  fs_ = filesystems::getFileSystem(spillDir_, {});
  fs_->mkdir(spillDir_);
}

void SpillerBenchmarkBase::printStats() const {
  LOG(INFO) << "total execution time: " << succinctMicros(executionTimeUs_);
  LOG(INFO) << numInputVectors_ << " vectors each with " << inputVectorSize_
            << " rows have been processed";
  const auto memStats = pool_->stats();
  LOG(INFO) << "peak memory usage[" << succinctBytes(memStats.peakBytes)
            << "] cumulative memory usage["
            << succinctBytes(memStats.cumulativeBytes) << "]";
  LOG(INFO) << spiller_->stats().toString();
  // List files under file path.
  SpillPartitionSet partitionSet;
  spiller_->finishSpill(partitionSet);
  VELOX_CHECK_EQ(partitionSet.size(), 1);
  const auto files = fs_->list(spillDir_);
  for (const auto& file : files) {
    auto rfile = fs_->openFileForRead(file);
    LOG(INFO) << "spilled file " << file << " size "
              << succinctBytes(rfile->size());
  }
}

void SpillerBenchmarkBase::cleanup() {
  LOG(INFO) << "Remove spill dir: " << spillDir_;
  fs_->rmdir(spillDir_);
}

} // namespace facebook::velox::exec::test

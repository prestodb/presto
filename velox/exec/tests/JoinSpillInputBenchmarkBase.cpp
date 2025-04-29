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

#include <deque>
#include "velox/serializers/PrestoSerializer.h"

#include "velox/exec/tests/JoinSpillInputBenchmarkBase.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::memory;
using namespace facebook::velox::exec;

namespace facebook::velox::exec::test {
namespace {
const int numSampleVectors = 100;
} // namespace

void JoinSpillInputBenchmarkBase::setUp() {
  SpillerBenchmarkBase::setUp();
  common::SpillConfig spillConfig;
  spillConfig.getSpillDirPathCb = [&]() -> std::string_view {
    return spillDir_;
  };
  spillConfig.updateAndCheckSpillLimitCb = [&](uint64_t) {};
  spillConfig.fileNamePrefix = FLAGS_spiller_benchmark_name;
  spillConfig.writeBufferSize = FLAGS_spiller_benchmark_write_buffer_size;
  spillConfig.executor = executor_.get();
  spillConfig.compressionKind =
      stringToCompressionKind(FLAGS_spiller_benchmark_compression_kind);
  spillConfig.maxFileSize = FLAGS_spiller_benchmark_max_spill_file_size;
  spillConfig.maxSpillRunRows = 0;
  spillConfig.fileCreateConfig = {};

  spiller_ = std::make_unique<NoRowContainerSpiller>(
      rowType_, std::nullopt, HashBitRange{29, 29}, &spillConfig, &spillStats_);
  dynamic_cast<NoRowContainerSpiller*>(spiller_.get())
      ->setPartitionsSpilled({SpillPartitionId(0)});
}

void JoinSpillInputBenchmarkBase::run() {
  MicrosecondTimer timer(&executionTimeUs_);
  auto noRowContainerSpiller =
      dynamic_cast<NoRowContainerSpiller*>(spiller_.get());
  VELOX_CHECK_NOT_NULL(noRowContainerSpiller);
  for (auto i = 0; i < numInputVectors_; ++i) {
    noRowContainerSpiller->spill(
        SpillPartitionId(0), rowVectors_[i % numSampleVectors]);
  }
}

} // namespace facebook::velox::exec::test

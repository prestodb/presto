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

  spiller_ = std::make_unique<Spiller>(
      exec::Spiller::Type::kHashJoinProbe,
      rowType_,
      HashBitRange{29, 29},
      [&]() -> const std::string& { return spillDir_; },
      [&](uint64_t) {},
      FLAGS_spiller_benchmark_name,
      FLAGS_spiller_benchmark_max_spill_file_size,
      FLAGS_spiller_benchmark_write_buffer_size,
      stringToCompressionKind(FLAGS_spiller_benchmark_compression_kind),
      memory::spillMemoryPool(),
      executor_.get());
  spiller_->setPartitionsSpilled({0});
}

void JoinSpillInputBenchmarkBase::run() {
  MicrosecondTimer timer(&executionTimeUs_);
  for (auto i = 0; i < numInputVectors_; ++i) {
    spiller_->spill(0, rowVectors_[i % numSampleVectors]);
  }
}

} // namespace facebook::velox::exec::test

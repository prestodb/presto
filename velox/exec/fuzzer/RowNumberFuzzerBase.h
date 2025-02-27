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

#include <utility>

#include "velox/common/fuzzer/Utils.h"
#include "velox/exec/Split.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_int32(steps);

DECLARE_int32(duration_sec);

DECLARE_int32(batch_size);

DECLARE_int32(num_batches);

DECLARE_double(null_ratio);

DECLARE_bool(enable_spill);

DECLARE_int32(max_spill_level);

DECLARE_bool(enable_oom_injection);

namespace facebook::velox::exec {

class RowNumberFuzzerBase {
 public:
  explicit RowNumberFuzzerBase(
      size_t initialSeed,
      std::unique_ptr<test::ReferenceQueryRunner>);

  void run();

  virtual ~RowNumberFuzzerBase() = default;

 protected:
  bool isTableScanSupported(const TypePtr& type);

  // Runs one test iteration from query plans generations, executions and result
  // verifications.
  virtual void runSingleIteration() = 0;

  // Sets up the Dwrf reader/writer, serializers and Hive connector for the
  // fuzzers.
  void setupReadWrite();

  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = FLAGS_null_ratio;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  int32_t randInt(int32_t min, int32_t max) {
    return fuzzer::rand(rng_, min, max);
  }

  // Validates the plan with input and result with the reference query runner.
  void validateExpectedResults(
      const core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& input,
      const RowVectorPtr& result);

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    std::vector<Split> splits;

    explicit PlanWithSplits(
        core::PlanNodePtr _plan,
        const std::vector<Split>& _splits = {})
        : plan(std::move(_plan)), splits(_splits) {}
  };

  // Executes a plan with spilling and oom injection possibly.
  RowVectorPtr execute(
      const PlanWithSplits& plan,
      const std::shared_ptr<memory::MemoryPool>& pool,
      bool injectSpill,
      bool injectOOM,
      const std::optional<std::string>& spillConfig = std::nullopt,
      int maxSpillLevel = -1);

  // Tests a plan by executing it with and without spilling. OOM injection
  // also might be done based on FLAG_enable_oom_injection.
  void testPlan(
      const PlanWithSplits& plan,
      int32_t testNumber,
      const RowVectorPtr& expected,
      const std::optional<std::string>& spillConfig);

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool(
          "rowNumberFuzzer",
          memory::kMaxMemory,
          memory::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild(
      "rowNumberFuzzerLeaf",
      true,
      memory::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> writerPool_{rootPool_->addAggregateChild(
      "rowNumberFuzzerWriter",
      memory::MemoryReclaimer::create())};
  VectorFuzzer vectorFuzzer_;
  std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner_;
};

} // namespace facebook::velox::exec

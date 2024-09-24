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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "velox/common/file/FileSystems.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/fuzzer/AggregationFuzzer.h"
#include "velox/exec/fuzzer/AggregationFuzzerOptions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

/// AggregationFuzzerRunner leverages AggregationFuzzer and VectorFuzzer to
/// automatically generate and execute aggregation tests. It works by:
///
///  1. Taking an initial set of available function signatures.
///  2. Generating a random query plan based on the available function
///     signatures.
///  3. Generating a random set of input data (vector), with a variety of
///     encodings and data layouts.
///  4. Executing a variety of logically equivalent query plans and
///     asserting results are the same.
///  5. Rinse and repeat.
///
/// The common usage pattern is as following:
///
///  $ ./velox_aggregate_fuzzer_test --steps 10000
///
/// The important flags that control AggregateFuzzer's behavior are:
///
///  --steps: how many iterations to run.
///  --duration_sec: alternatively, for how many seconds it should run (takes
///          precedence over --steps).
///  --seed: pass a deterministic seed to reproduce the behavior (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --only: restrict the functions to fuzz.
///  --batch_size: size of input vector batches generated.
///
/// e.g:
///
///  $ ./velox_aggregation_fuzzer_test \
///         --steps 10000 \
///         --seed 123 \
///         --v=1 \
///         --only "min,max"
class AggregationFuzzerRunner {
 public:
  static int run(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const AggregationFuzzerOptions& options) {
    return runFuzzer(
        seed, std::nullopt, std::move(referenceQueryRunner), options);
  }

  static int runRepro(
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
    return runFuzzer(0, planPath, std::move(referenceQueryRunner), {});
  }

 protected:
  static int runFuzzer(
      size_t seed,
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const AggregationFuzzerOptions& options) {
    auto signatures = facebook::velox::exec::getAggregateFunctionSignatures();
    if (signatures.empty()) {
      LOG(ERROR) << "No aggregate functions registered.";
      exit(1);
    }

    auto filteredSignatures = velox::fuzzer::filterSignatures(
        signatures, options.onlyFunctions, options.skipFunctions);
    if (filteredSignatures.empty()) {
      LOG(ERROR)
          << "No aggregate functions left after filtering using 'only' and 'skip' lists.";
      exit(1);
    }

    facebook::velox::parse::registerTypeResolver();
    facebook::velox::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();
    facebook::velox::filesystems::registerLocalFileSystem();

    facebook::velox::exec::test::aggregateFuzzer(
        filteredSignatures,
        seed,
        options.customVerificationFunctions,
        options.customInputGenerators,
        options.functionDataSpec,
        options.timestampPrecision,
        options.queryConfigs,
        options.hiveConfigs,
        options.orderableGroupKeys,
        planPath,
        std::move(referenceQueryRunner));
    // Calling gtest here so that it can be recognized as tests in CI systems.
    return RUN_ALL_TESTS();
  }
};

} // namespace facebook::velox::exec::test

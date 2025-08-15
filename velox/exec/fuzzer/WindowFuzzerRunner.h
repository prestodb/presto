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
#include "velox/exec/fuzzer/AggregationFuzzerOptions.h"
#include "velox/exec/fuzzer/WindowFuzzer.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

class WindowFuzzerRunner {
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
    auto aggregationSignatures =
        facebook::velox::exec::getAggregateFunctionSignatures();
    auto windowSignatures = facebook::velox::exec::windowFunctions();
    if (aggregationSignatures.empty() && windowSignatures.empty()) {
      LOG(ERROR) << "No function registered.";
      exit(1);
    }

    auto filteredAggregationSignatures = velox::fuzzer::filterSignatures(
        aggregationSignatures, options.onlyFunctions, options.skipFunctions);
    auto filteredWindowSignatures = velox::fuzzer::filterSignatures(
        windowSignatures, options.onlyFunctions, options.skipFunctions);
    if (filteredAggregationSignatures.empty() &&
        filteredWindowSignatures.empty()) {
      LOG(ERROR)
          << "No function left after filtering using 'only' and 'skip' lists.";
      return 1;
    }

    facebook::velox::parse::registerTypeResolver();
    facebook::velox::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();
    facebook::velox::filesystems::registerLocalFileSystem();

    auto& aggregationFunctionDataSpecs =
        referenceQueryRunner->aggregationFunctionDataSpecs();

    facebook::velox::exec::test::windowFuzzer(
        filteredAggregationSignatures,
        filteredWindowSignatures,
        seed,
        options.customVerificationFunctions,
        options.customInputGenerators,
        options.orderDependentFunctions,
        aggregationFunctionDataSpecs,
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

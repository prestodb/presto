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
#include "velox/exec/tests/utils/AggregationFuzzer.h"
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
  struct Options {
    /// Comma-separated list of functions to test. By default, all functions
    /// are tested.
    std::string onlyFunctions;

    /// Set of functions to not test.
    std::unordered_set<std::string> skipFunctions;

    /// Set of functions whose results are non-deterministic. These can be
    /// order-dependent functions whose results depend on the order of input
    /// rows, or functions that return complex-typed results containing
    /// floating-point fields.
    ///
    /// For some functions, the result can be transformed to a deterministic
    /// value. If such transformation exists, it can be specified to be used for
    /// results verification. If no transformation is specified, results are not
    /// verified.
    ///
    /// Keys are function names. Values are optional transformations. "{}"
    /// should be used to indicate the original value, i.e. "f({})"
    /// transformation applies function 'f' to aggregation result.
    std::unordered_map<std::string, std::string> customVerificationFunctions;

    std::unordered_map<std::string, std::shared_ptr<InputGenerator>>
        customInputGenerators;

    /// Timestamp precision to use when generating inputs of type TIMESTAMP.
    VectorFuzzer::Options::TimestampPrecision timestampPrecision{
        VectorFuzzer::Options::TimestampPrecision::kNanoSeconds};

    /// A set of configuration properties to use when running query plans.
    /// Could be used to specify timezone or enable/disable settings that
    /// affect semantics of individual aggregate functions.
    std::unordered_map<std::string, std::string> queryConfigs;
  };

  static int run(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const Options& options) {
    return runFuzzer(
        seed, std::nullopt, std::move(referenceQueryRunner), options);
  }

  static int runRepro(
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
    return runFuzzer(0, planPath, std::move(referenceQueryRunner), {});
  }

 private:
  static int runFuzzer(
      size_t seed,
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const Options& options) {
    auto signatures = facebook::velox::exec::getAggregateFunctionSignatures();
    if (signatures.empty()) {
      LOG(ERROR) << "No aggregate functions registered.";
      exit(1);
    }

    auto filteredSignatures = filterSignatures(
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
        options.timestampPrecision,
        options.queryConfigs,
        planPath,
        std::move(referenceQueryRunner));
    // Calling gtest here so that it can be recognized as tests in CI systems.
    return RUN_ALL_TESTS();
  }

  static std::unordered_set<std::string> splitNames(const std::string& names) {
    // Parse, lower case and trim it.
    std::vector<folly::StringPiece> nameList;
    folly::split(',', names, nameList);
    std::unordered_set<std::string> nameSet;

    for (const auto& it : nameList) {
      auto str = folly::trimWhitespace(it).toString();
      folly::toLowerAscii(str);
      nameSet.insert(str);
    }
    return nameSet;
  }

  // Parse the comma separated list of function names, and use it to filter the
  // input signatures.
  static facebook::velox::exec::AggregateFunctionSignatureMap filterSignatures(
      const facebook::velox::exec::AggregateFunctionSignatureMap& input,
      const std::string& onlyFunctions,
      const std::unordered_set<std::string>& skipFunctions) {
    if (onlyFunctions.empty() && skipFunctions.empty()) {
      return input;
    }

    facebook::velox::exec::AggregateFunctionSignatureMap output;
    if (!onlyFunctions.empty()) {
      // Parse, lower case and trim it.
      auto nameSet = splitNames(onlyFunctions);
      for (const auto& it : input) {
        if (nameSet.count(it.first) > 0) {
          output.insert(it);
        }
      }
    } else {
      output = input;
    }

    for (auto s : skipFunctions) {
      auto str = s;
      folly::toLowerAscii(str);
      output.erase(str);
    }
    return output;
  }
};

} // namespace facebook::velox::exec::test

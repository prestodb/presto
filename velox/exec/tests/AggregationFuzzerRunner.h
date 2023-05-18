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
#include "velox/exec/tests/AggregationFuzzer.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"

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
  // TODO: List of the functions that at some point crash or fail and need to
  // be fixed before we can enable.
  static inline const std::unordered_set<std::string> skipFunctions_ = {
      "stddev_pop", // https://github.com/facebookincubator/velox/issues/3493
  };

  // Functions whose results verification should be skipped. These can be
  // order-dependent functions whose results depend on the order of input rows,
  // or functions that return complex-typed results containing floating-point
  // fields. For some functions, the result can be transformed to a value that
  // can be verified. If such transformation exists, it can be specified to be
  // used for results verification. If no transformation is specified, results
  // are not verified.
  static inline const std::unordered_map<std::string, std::string>
      customVerificationFunctions_ = {
          // Order-dependent functions.
          {"approx_distinct", ""},
          {"approx_distinct_partial", ""},
          {"approx_distinct_merge", ""},
          {"approx_set", ""},
          {"approx_set_partial", ""},
          {"approx_set_merge", ""},
          {"approx_percentile_partial", ""},
          {"approx_percentile_merge", ""},
          {"arbitrary", ""},
          {"array_agg", "array_sort({})"},
          {"map_agg", "array_sort(map_keys({}))"},
          {"map_union", "array_sort(map_keys({}))"},
          {"map_union_sum", "array_sort(map_keys({}))"},
          {"max_by", ""},
          {"min_by", ""},
          // TODO: Skip result verification of companion functions that return
          // complex types that contain floating-point fields for now, until we
          // fix
          // test utilities in QueryAssertions to tolerate floating-point
          // imprecision in complex types.
          // https://github.com/facebookincubator/velox/issues/4481
          {"avg_partial", ""},
          {"avg_merge", ""},
  };

  static int run(const std::string& planPath) {
    return runFuzzer("", 0, {planPath});
  }

  static int run(const std::string& onlyFunctions, size_t seed) {
    return runFuzzer(onlyFunctions, seed, std::nullopt);
  }

  static int runFuzzer(
      const std::string& onlyFunctions,
      size_t seed,
      const std::optional<std::string>& planPath,
      const std::unordered_set<std::string>& skipFunctions = skipFunctions_,
      const std::unordered_map<std::string, std::string>&
          customVerificationFunctions = customVerificationFunctions_) {
    auto signatures = facebook::velox::exec::getAggregateFunctionSignatures();
    if (signatures.empty()) {
      LOG(ERROR) << "No aggregate functions registered.";
      exit(1);
    }

    auto filteredSignatures =
        filterSignatures(signatures, onlyFunctions, skipFunctions);
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
        filteredSignatures, seed, customVerificationFunctions, planPath);
    // Calling gtest here so that it can be recognized as tests in CI systems.
    return RUN_ALL_TESTS();
  }

 private:
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

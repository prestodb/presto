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

#include "velox/exec/Aggregate.h"
#include "velox/exec/tests/AggregationFuzzer.h"
#include "velox/parse/TypeResolver.h"

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
      const std::string& onlyFunctions,
      size_t seed,
      const std::unordered_set<std::string>& skipFunctions,
      const std::unordered_map<std::string, std::string>&
          orderDependentFunctions) {
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

    facebook::velox::exec::test::aggregateFuzzer(
        filteredSignatures, seed, orderDependentFunctions);
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

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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "velox/expression/tests/ExpressionFuzzer.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

/// ExpressionFuzzerTest is an test/executable that leverages ExpressionFuzzer
/// and VectorFuzzer to automatically generate and execute expression tests. It
/// works by:
///
///  1. Taking an initial set of available function signatures.
///  2. Generating a random expression tree based on the available function
///     signatures.
///  3. Generating a random set of input data (vector), with a variety of
///     encodings and data layouts.
///  4. Executing the expression using the common and simplified eval paths, and
///     asserting results are the exact same.
///  5. Rinse and repeat.
///
/// The common usage pattern is as following:
///
///  $ ./velox_expression_fuzzer_test --steps 10000
///
/// The important flags that control Fuzzer's behavior are:
///
///  --steps: how many iterations to run
///  --seed: pass a deterministic seed to reproduce the behavior (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --only: restrict the functions to fuzz.
///  --batch_size: size of input vector batches generated.
///
/// e.g:
///
///  $ ./velox_expression_fuzzer_test \
///         --steps 10000 \
///         --seed 123 \
///         --v=1 \
///         --only "substr,trim"

DEFINE_int64(
    seed,
    123456,
    "Initial seed for random number generator "
    "(use it to reproduce previous results).");

DEFINE_string(
    only,
    "",
    "If specified, Fuzzer will only choose functions from "
    "this comma separated list of function names "
    "(e.g: --only \"split\" or --only \"substr,ltrim\").");

DEFINE_int32(steps, 1, "Number of expressions to generate.");

using namespace facebook::velox;

// Parse the comma separated list of funciton names, and use it to filter the
// input signatures.
FunctionSignatureMap filterSignatures(
    const FunctionSignatureMap& input,
    const std::string& onlyFunctions) {
  if (onlyFunctions.empty()) {
    return input;
  }

  // Parse, lower case and trim it.
  std::vector<folly::StringPiece> nameList;
  folly::split(',', onlyFunctions, nameList);
  std::unordered_set<std::string> nameSet;

  for (const auto& it : nameList) {
    auto str = folly::trimWhitespace(it).toString();
    folly::toLowerAscii(str);
    nameSet.insert(str);
  }

  // Use the generated set to filter the input signatures.
  FunctionSignatureMap output;
  for (const auto& it : input) {
    if (nameSet.count(it.first) > 0) {
      output.insert(it);
    }
  }
  return output;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::init(&argc, &argv);

  functions::prestosql::registerAllScalarFunctions();

  test::expressionFuzzer(
      filterSignatures(getFunctionSignatures(), FLAGS_only),
      FLAGS_steps,
      FLAGS_seed);
  return RUN_ALL_TESTS();
}

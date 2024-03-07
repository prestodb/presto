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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <unordered_set>

#include "velox/expression/tests/FuzzerRunner.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

using facebook::velox::test::FuzzerRunner;

int main(int argc, char** argv) {
  facebook::velox::functions::prestosql::registerAllScalarFunctions();

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  // TODO: List of the functions that at some point crash or fail and need to
  // be fixed before we can enable.
  // This list can include a mix of function names and function signatures.
  // Use function name to exclude all signatures of a given function from
  // testing. Use function signature to exclude only a specific signature.
  std::unordered_set<std::string> skipFunctions = {
      // Fuzzer and the underlying engine are confused about cardinality(HLL)
      // (since HLL is a user defined type), and end up trying to use
      // cardinality passing a VARBINARY (since HLL's implementation uses an
      // alias to VARBINARY).
      "cardinality",
      "element_at",
      "width_bucket",
      // Fuzzer cannot generate valid 'comparator' lambda.
      "array_sort(array(T),constant function(T,T,bigint)) -> array(T)",
      // https://github.com/facebookincubator/velox/issues/8919
      "plus(date,interval year to month) -> date",
      "minus(date,interval year to month) -> date",
      "plus(timestamp,interval year to month) -> timestamp",
      "plus(interval year to month,timestamp) -> timestamp",
      "minus(timestamp,interval year to month) -> timestamp",
      // https://github.com/facebookincubator/velox/issues/8438#issuecomment-1907234044
      "regexp_extract",
      "regexp_extract_all",
      "regexp_like",
  };
  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  return FuzzerRunner::run(initialSeed, skipFunctions, {{}});
}

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
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <unordered_set>

#include "velox/exec/tests/utils/AggregationFuzzerRunner.h"
#include "velox/exec/tests/utils/DuckQueryRunner.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_string(
    only,
    "",
    "If specified, Fuzzer will only choose functions from "
    "this comma separated list of function names "
    "(e.g: --only \"min\" or --only \"sum,avg\").");

int main(int argc, char** argv) {
  facebook::velox::aggregate::prestosql::registerAllAggregateFunctions();
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::functions::prestosql::registerInternalFunctions();

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::init(&argc, &argv);

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  auto duckQueryRunner =
      std::make_unique<facebook::velox::exec::test::DuckQueryRunner>();

  // List of functions that have known bugs that cause crashes or failures.
  static const std::unordered_set<std::string> skipFunctions = {
      // https://github.com/facebookincubator/velox/issues/3493
      "stddev_pop",
      // Lambda functions are not supported yet.
      "reduce_agg",
  };

  // Functions whose results verification should be skipped. These can be
  // order-dependent functions whose results depend on the order of input rows,
  // or functions that return complex-typed results containing floating-point
  // fields. For some functions, the result can be transformed to a value that
  // can be verified. If such transformation exists, it can be specified to be
  // used for results verification. If no transformation is specified, results
  // are not verified.
  static const std::unordered_map<std::string, std::string>
      customVerificationFunctions = {
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
          {"array_agg", "\"$internal$canonicalize\"({})"},
          {"array_agg_partial", "\"$internal$canonicalize\"({})"},
          {"array_agg_merge", "\"$internal$canonicalize\"({})"},
          {"array_agg_merge_extract", "\"$internal$canonicalize\"({})"},
          {"set_agg", "\"$internal$canonicalize\"({})"},
          {"set_union", "\"$internal$canonicalize\"({})"},
          {"map_agg", "\"$internal$canonicalize\"(map_keys({}))"},
          {"map_union", "\"$internal$canonicalize\"(map_keys({}))"},
          {"map_union_sum", "\"$internal$canonicalize\"(map_keys({}))"},
          {"max_by", ""},
          {"min_by", ""},
          {"map_union_sum", "\"$internal$canonicalize\"(map_keys({}))"},
          {"multimap_agg",
           "transform_values({}, (k, v) -> \"$internal$canonicalize\"(v))"},
          // TODO: Skip result verification of companion functions that return
          // complex types that contain floating-point fields for now, until we
          // fix
          // test utilities in QueryAssertions to tolerate floating-point
          // imprecision in complex types.
          // https://github.com/facebookincubator/velox/issues/4481
          {"avg_partial", ""},
          {"avg_merge", ""},
          // Semantically inconsistent functions
          {"skewness", ""},
          {"kurtosis", ""},
          {"entropy", ""},
          // https://github.com/facebookincubator/velox/issues/6330
          {"max_data_size_for_stats", ""},
          {"sum_data_size_for_stats", ""},
      };

  using Runner = facebook::velox::exec::test::AggregationFuzzerRunner;

  Runner::Options options;
  options.onlyFunctions = FLAGS_only;
  options.skipFunctions = skipFunctions;
  options.customVerificationFunctions = customVerificationFunctions;
  options.timestampPrecision =
      facebook::velox::VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
  return Runner::run(initialSeed, std::move(duckQueryRunner), options);
}

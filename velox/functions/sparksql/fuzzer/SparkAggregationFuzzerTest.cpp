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

#include "velox/exec/fuzzer/AggregationFuzzerOptions.h"
#include "velox/exec/fuzzer/AggregationFuzzerRunner.h"
#include "velox/exec/fuzzer/DuckQueryRunner.h"
#include "velox/exec/fuzzer/TransformResultVerifier.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/sparksql/aggregates/Register.h"

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
  facebook::velox::functions::aggregate::sparksql::registerAggregateFunctions(
      "", false);

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  facebook::velox::functions::prestosql::registerInternalFunctions();
  facebook::velox::memory::MemoryManager::initialize({});

  // TODO: List of the functions that at some point crash or fail and need to
  // be fixed before we can enable. Constant argument of bloom_filter_agg cause
  // fuzzer test fail.
  std::unordered_set<std::string> skipFunctions = {"bloom_filter_agg"};

  using facebook::velox::exec::test::TransformResultVerifier;

  auto makeArrayVerifier = []() {
    return TransformResultVerifier::create("\"$internal$canonicalize\"({})");
  };

  // The results of the following functions depend on the order of input
  // rows. For some functions, the result can be transformed to a value that
  // doesn't depend on the order of inputs. If such transformation exists, it
  // can be specified to be used for results verification. If no transformation
  // is specified, results are not verified.
  std::unordered_map<
      std::string,
      std::shared_ptr<facebook::velox::exec::test::ResultVerifier>>
      customVerificationFunctions = {
          {"last", nullptr},
          {"last_ignore_null", nullptr},
          {"first", nullptr},
          {"first_ignore_null", nullptr},
          {"max_by", nullptr},
          {"min_by", nullptr},
          // If multiple values have the same greatest frequency, the return
          // value is indeterminate.
          {"mode", nullptr},
          {"skewness", nullptr},
          {"kurtosis", nullptr},
          {"collect_list", makeArrayVerifier()},
          {"collect_set", makeArrayVerifier()},
          // Nested nulls are handled as values in Spark. But nested nulls
          // comparison always generates null in DuckDB.
          {"min", nullptr},
          {"max", nullptr},
      };

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  auto duckQueryRunner =
      std::make_unique<facebook::velox::exec::test::DuckQueryRunner>(
          rootPool.get());
  duckQueryRunner->disableAggregateFunctions(
      {// https://github.com/facebookincubator/velox/issues/7677
       "max_by",
       "min_by",
       // The skewness functions of Velox and DuckDB use different
       // algorithms.
       // https://github.com/facebookincubator/velox/issues/4845
       "skewness",
       // Spark's kurtosis uses Pearson's formula for calculating the kurtosis
       // coefficient. Meanwhile, DuckDB employs the sample kurtosis calculation
       // formula. The results from the two methods are completely different.
       "kurtosis"});

  using facebook::velox::DataSpec;
  // For some functions, velox supports NaN, Infinity better than presto query
  // runner, which makes the comparison impossible.
  // Add data spec in vector fuzzer to enforce to not generate such data
  // for those functions before they are fixed in presto query runner
  static const std::unordered_map<std::string, DataSpec> functionDataSpec = {
      {"regr_avgx", DataSpec{false, false}},
      {"regr_avgy", DataSpec{false, false}},
      {"regr_r2", DataSpec{false, false}},
      {"regr_sxx", DataSpec{false, false}},
      {"regr_syy", DataSpec{false, false}},
      {"regr_sxy", DataSpec{false, false}},
      {"regr_slope", DataSpec{false, false}},
      {"regr_replacement", DataSpec{false, false}},
      {"covar_pop", DataSpec{true, false}},
      {"covar_samp", DataSpec{true, false}},
  };

  using Runner = facebook::velox::exec::test::AggregationFuzzerRunner;
  using Options = facebook::velox::exec::test::AggregationFuzzerOptions;

  Options options;
  options.onlyFunctions = FLAGS_only;
  options.skipFunctions = skipFunctions;
  options.customVerificationFunctions = customVerificationFunctions;
  options.orderableGroupKeys = true;
  options.functionDataSpec = functionDataSpec;
  return Runner::run(initialSeed, std::move(duckQueryRunner), options);
}

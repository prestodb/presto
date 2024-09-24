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

#include <boost/random/uniform_int_distribution.hpp>
#include "velox/exec/fuzzer/InputGenerator.h"
#include "velox/exec/fuzzer/ResultVerifier.h"

namespace facebook::velox::exec::test {

struct AggregationFuzzerOptions {
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
  std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>
      customVerificationFunctions;

  std::unordered_map<std::string, std::shared_ptr<InputGenerator>>
      customInputGenerators;

  std::unordered_set<std::string> orderDependentFunctions;

  std::unordered_map<std::string, DataSpec> functionDataSpec;

  /// Timestamp precision to use when generating inputs of type TIMESTAMP.
  VectorFuzzer::Options::TimestampPrecision timestampPrecision{
      VectorFuzzer::Options::TimestampPrecision::kMilliSeconds};

  /// A set of configuration properties to use when running query plans.
  /// Could be used to specify timezone or enable/disable settings that
  /// affect semantics of individual aggregate functions.
  std::unordered_map<std::string, std::string> queryConfigs;

  /// A set of hive configuration properties to use when running query plans.
  /// Could be used to specify different timestamp units for Presto and Spark
  /// fuzzer test.
  std::unordered_map<std::string, std::string> hiveConfigs;

  // Whether group keys must be orderable or be just comparable.
  bool orderableGroupKeys = false;
};

} // namespace facebook::velox::exec::test

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

#include <string>

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox {

class ExpressionBenchmarkBuilder;

// This class represents a set of expressions to benchmark those expressions
// uses the same input vector type, and are expected to have the same result
// if testing is not disabled for the set.
// Users can pass the inputVector explicitly, if not passed a flat vector will
// be fuzzed with some default fuzzer options. Users can also pass fuzzer
// config to be used.
class ExpressionBenchmarkSet {
 public:
  ExpressionBenchmarkSet& addExpression(
      const std::string& name,
      const std::string& expression);

  ExpressionBenchmarkSet& addExpressions(
      const std::vector<std::pair<std::string, std::string>>& expressions);

  ExpressionBenchmarkSet& disableTesting() {
    disableTesting_ = true;
    return *this;
  }

  ExpressionBenchmarkSet& withFuzzerOptions(
      const VectorFuzzer::Options& options) {
    VELOX_CHECK(
        !inputRowVector_,
        "input row vector is already passed, fuzzer wont be used");
    fuzzerOptions_ = options;
    return *this;
  }

  ExpressionBenchmarkSet& withIterations(int iterations) {
    iterations_ = iterations;
    return *this;
  }

 private:
  ExpressionBenchmarkSet(
      ExpressionBenchmarkBuilder& builder,
      const RowVectorPtr& inputRowVector)
      : inputRowVector_(inputRowVector),
        inputType_(inputRowVector_->type()),
        builder_(builder) {}

  ExpressionBenchmarkSet(
      ExpressionBenchmarkBuilder& builder,
      const TypePtr& inputType)
      : inputType_(inputType), builder_(builder) {}

  // All the expressions that belongs to this set.
  std::vector<std::pair<std::string, exec::ExprSet>> expressions_;

  // The input that will be used for benchmarking expressions. If not set,
  // a flat input vector is fuzzed using fuzzerOptions_.
  RowVectorPtr inputRowVector_;

  // The type of the input that will be used for all the expressions
  // benchmarked.
  TypePtr inputType_;

  // User can provide fuzzer options for the input row vector used for this
  // benchmark. Note that the fuzzer will be used to generate a flat input row
  // vector if inputRowVector_ is nullptr.
  VectorFuzzer::Options fuzzerOptions_{.vectorSize = 10000, .nullRatio = 0};

  // Number of times to run each benchmark.
  int iterations_ = 1000;

  bool disableTesting_ = false;

  // The builder that this expression set belongs to.
  ExpressionBenchmarkBuilder& builder_;
  friend class ExpressionBenchmarkBuilder;
};

// A utility class to simplify creating expression's benchmarks.
class ExpressionBenchmarkBuilder
    : public functions::test::FunctionBenchmarkBase {
 public:
  explicit ExpressionBenchmarkBuilder() : FunctionBenchmarkBase() {}

  // Register all the benchmarks, so that they would run when
  // folly::runBenchmarks() is called.
  void registerBenchmarks();

  // All benchmarks within one group set are expected to have the same results.
  // If disableTesting=true for a group set, testing is skipped.
  void testBenchmarks();

  test::VectorMaker& vectorMaker() {
    return vectorMaker_;
  }

  ExpressionBenchmarkSet& addBenchmarkSet(
      const std::string& name,
      const RowVectorPtr& inputRowVector) {
    VELOX_CHECK(!benchmarkSets_.count(name));
    benchmarkSets_.emplace(name, ExpressionBenchmarkSet(*this, inputRowVector));
    return benchmarkSets_.at(name);
  }

  ExpressionBenchmarkSet& addBenchmarkSet(
      const std::string& name,
      const TypePtr& inputType) {
    VELOX_CHECK(!benchmarkSets_.count(name));
    benchmarkSets_.emplace(name, ExpressionBenchmarkSet(*this, inputType));
    return benchmarkSets_.at(name);
  }

 private:
  void ensureInputVectors();

  std::map<std::string, ExpressionBenchmarkSet> benchmarkSets_;
};
} // namespace facebook::velox

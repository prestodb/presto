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

#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"
#include <folly/Benchmark.h>
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox {

ExpressionBenchmarkSet& ExpressionBenchmarkSet::addExpression(
    const std::string& name,
    const std::string& expression) {
  expressions_.push_back(
      std::make_pair(name, builder_.compileExpression(expression, inputType_)));
  return *this;
}

ExpressionBenchmarkSet& ExpressionBenchmarkSet::addExpressions(
    const std::vector<std::pair<std::string, std::string>>& expressions) {
  for (auto& [name, expression] : expressions) {
    addExpression(name, expression);
  }
  return *this;
}

// Make sure all input vectors are generated.
void ExpressionBenchmarkBuilder::ensureInputVectors() {
  for (auto& [_, benchmarkSet] : benchmarkSets_) {
    if (!benchmarkSet.inputRowVector_) {
      VectorFuzzer fuzzer(benchmarkSet.fuzzerOptions_, pool());
      benchmarkSet.inputRowVector_ = std::dynamic_pointer_cast<RowVector>(
          fuzzer.fuzzFlat(benchmarkSet.inputType_));
    }
  }
}

void ExpressionBenchmarkBuilder::testBenchmarks() {
  ensureInputVectors();

  auto evalExpression = [&](auto& exprSet, auto& inputVector) {
    exec::EvalCtx evalCtx(&this->execCtx_, &exprSet, inputVector.get());
    SelectivityVector rows(inputVector->size());
    std::vector<VectorPtr> results(1);
    exprSet.eval(rows, evalCtx, results);
    return results[0];
  };

  auto testBenchmarkSet = [&](auto& benchmarkSet) {
    if (benchmarkSet.expressions_.size() == 0) {
      return;
    }
    // Evaluate the first expression.
    auto it = benchmarkSet.expressions_.begin();
    auto refResult = evalExpression(it->second, benchmarkSet.inputRowVector_);
    it++;
    while (it != benchmarkSet.expressions_.end()) {
      auto result = evalExpression(it->second, benchmarkSet.inputRowVector_);
      test::assertEqualVectors(refResult, result);
      it++;
    }
  };

  for (auto& [_, benchmarkSet] : benchmarkSets_) {
    if (!benchmarkSet.disableTesting_) {
      testBenchmarkSet(benchmarkSet);
    }
  }
}

void ExpressionBenchmarkBuilder::registerBenchmarks() {
  ensureInputVectors();
  // Generate input vectors if needed.
  for (auto& [setName, benchmarkSet] : benchmarkSets_) {
    for (auto& [exprName, exprSet] : benchmarkSet.expressions_) {
      auto name = fmt::format("{}##{}", setName, exprName);
      auto& inputVector = benchmarkSet.inputRowVector_;
      auto times = benchmarkSet.iterations_;
      // The compiler does not allow capturing exprSet int the lambda.
      auto& exprSetLocal = exprSet;
      folly::addBenchmark(
          __FILE__, name, [this, &inputVector, &exprSetLocal, times]() {
            int cnt = 0;
            folly::BenchmarkSuspender suspender;
            // TODO: shall we cache those.
            exec::EvalCtx evalCtx(
                &this->execCtx_, &exprSetLocal, inputVector.get());
            SelectivityVector rows(inputVector->size());
            suspender.dismiss();

            std::vector<VectorPtr> results(1);
            for (auto i = 0; i < times; i++) {
              exprSetLocal.eval(rows, evalCtx, results);

              // TODO: add flag to enable/disable flattening.
              BaseVector::flattenVector(results[0]);

              // TODO: add flag to enable/disable reuse.
              results[0]->prepareForReuse();

              cnt += results[0]->size();
            }
            folly::doNotOptimizeAway(cnt);
            return 1;
          });
    }

    folly::addBenchmark(__FILE__, "-", []() -> unsigned { return 0; });
  }
}
} // namespace facebook::velox

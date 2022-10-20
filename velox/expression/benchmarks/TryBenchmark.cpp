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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// This runs two sets of benchmarks:
// 1) Benchmark executing expressions wrapped in try compared to executing the
// same expressions without.
// It sums 50 integer columns to create a deep tree of expressions, and runs
// it with no Try expression, a single Try expression around the entire sum,
// and a Try around each individual addition. No exceptions are thrown or
// caught in these benchmarks.
// 2) Benchmark the performance impact of throwing exceptions and catching them
// using Try.
// It divides two integers, using division by 0 to trigger an exception getting
// thrown.
// These benchmarks show that meerly adding a Try expression does not
// significantly impact performance, and the performance cost of handling
// exceptions scales linearly with the number of rows that saw exceptions.

using namespace facebook::velox;

namespace {
class TryBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  TryBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerAllScalarFunctions();
  }

  RowVectorPtr makeData(vector_size_t numArgs) {
    const vector_size_t size = 1'000;

    std::vector<VectorPtr> args;

    for (vector_size_t i = 0; i < numArgs; ++i) {
      args.emplace_back(vectorMaker_.flatVector<int32_t>(
          // Add 1 so we don't accidentally end up with any 0s for the division
          // test.
          size,
          [i](vector_size_t row) { return (row * i) + 1; }));
    }

    return vectorMaker_.rowVector(args);
  }

  // Baseline does not use any try.
  size_t runBaseline(vector_size_t numArgs) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeData(numArgs);

    std::string expression = "c0 + c1";
    for (vector_size_t i = 2; i < numArgs; ++i) {
      expression += fmt::format(" + c{}", i);
    }

    auto exprSet = compileExpression(expression, rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t runWithOuterTry(vector_size_t numArgs) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeData(numArgs);

    std::string expression = "c0 + c1";
    for (vector_size_t i = 2; i < numArgs; ++i) {
      expression += fmt::format(" + c{}", i);
    }
    expression = fmt::format("try({})", expression);

    auto exprSet = compileExpression(expression, rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t runWithNestedTries(vector_size_t numArgs) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeData(numArgs);

    std::string expression = "try(c0 + c1)";
    for (vector_size_t i = 2; i < numArgs; ++i) {
      expression = fmt::format("try({} + c{})", expression, i);
    }

    auto exprSet = compileExpression(expression, rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t runDivisionWithNoExceptions() {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeData(2);

    auto exprSet = compileExpression("TRY(c0 / c1)", rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t runDivisionWithOneException() {
    folly::BenchmarkSuspender suspender;
    auto numerators = makeData(1)->childAt(0);
    auto denominatorValues = std::vector<int32_t>(numerators->size(), 1);
    denominatorValues[0] = 0; // NOLINT
    auto denominators = vectorMaker_.flatVector(denominatorValues);
    auto rowVector = vectorMaker_.rowVector({numerators, denominators});

    auto exprSet = compileExpression("TRY(c0 / c1)", rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t runDivisionWithAllExceptions() {
    folly::BenchmarkSuspender suspender;
    auto numerators = makeData(1)->childAt(0);
    auto denominatorValues = std::vector<int32_t>(numerators->size(), 0);
    auto denominators = vectorMaker_.flatVector(denominatorValues);
    auto rowVector = vectorMaker_.rowVector({numerators, denominators});

    auto exprSet = compileExpression("TRY(c0 / c1)", rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }
};

BENCHMARK_MULTI(baseline) {
  TryBenchmark benchmark;
  return benchmark.runBaseline(50);
}

BENCHMARK_MULTI(singleOuterTry) {
  TryBenchmark benchmark;
  return benchmark.runWithOuterTry(50);
}

BENCHMARK_MULTI(nestedTries) {
  TryBenchmark benchmark;
  return benchmark.runWithNestedTries(50);
}

BENCHMARK_MULTI(divideNoExceptions) {
  TryBenchmark benchmark;
  return benchmark.runDivisionWithNoExceptions();
}

BENCHMARK_MULTI(divideOneException) {
  TryBenchmark benchmark;
  return benchmark.runDivisionWithOneException();
}

BENCHMARK_MULTI(divideAllExceptions) {
  TryBenchmark benchmark;
  return benchmark.runDivisionWithAllExceptions();
}
} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}

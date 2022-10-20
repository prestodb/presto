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
#include "folly/Random.h"
#include "velox/functions/Macros.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions::test {
namespace {
class CompareBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  CompareBenchmark() : FunctionBenchmarkBase() {
    prestosql::registerComparisonFunctions();
  }

  VectorPtr makeData() {
    constexpr vector_size_t size = 1000;

    return vectorMaker_.flatVector<int64_t>(
        size,
        [](auto row) { return row % 2 ? row : folly::Random::rand32() % size; },
        facebook::velox::test::VectorMaker::nullEvery(5));
  }

  size_t run(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto inputs = vectorMaker_.rowVector({makeData(), makeData()});

    auto exprSet = compileExpression(
        fmt::format("c0 {} c1", functionName), inputs->type());
    suspender.dismiss();

    return doRun(exprSet, inputs);
  }

  size_t doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
    return cnt;
  }

  size_t runFastVeloxInVeloxOut() {
    folly::BenchmarkSuspender suspender;
    auto input1 = makeData();
    auto input1Flat = input1->asFlatVector<int64_t>();
    auto input2 = makeData();
    auto input2Flat = input2->asFlatVector<int64_t>();
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      auto result = vectorMaker_.flatVector<bool>(1000);
      for (int j = 0; j < input1Flat->size(); j++) {
        if (input1Flat->isNullAt(j) || input2Flat->isNullAt(j)) {
          result->setNull(j, true);
        } else {
          result->set(j, input1Flat->valueAt(j) == input2Flat->valueAt(j));
        }
      }
      cnt += result->size();
      folly::doNotOptimizeAway(result);
    }
    folly::doNotOptimizeAway(cnt);
    return cnt;
  }

  size_t runFastVeloxInStdOut() {
    folly::BenchmarkSuspender suspender;
    auto input1 = makeData();
    auto input1Flat = input1->asFlatVector<int64_t>();
    auto input2 = makeData();
    auto input2Flat = input2->asFlatVector<int64_t>();
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      std::vector<std::optional<bool>> result;
      for (int j = 0; j < input1Flat->size(); j++) {
        if (input1Flat->isNullAt(j) || input2Flat->isNullAt(j)) {
          result.push_back(std::nullopt);
        } else {
          result.push_back(input1Flat->valueAt(j) == input2Flat->valueAt(j));
        }
      }
      cnt += result.size();
      folly::doNotOptimizeAway(result);
    }
    folly::doNotOptimizeAway(cnt);
    return cnt;
  }

  size_t runFastStdInStdOut() {
    auto buildInput = []() {
      std::vector<std::optional<int64_t>> result;
      for (auto i = 0; i < 1000; i++) {
        if (i % 5) {
          result.push_back(std::nullopt);
        } else {
          if (i % 2) {
            result.push_back(i);
          } else {
            result.push_back(folly::Random::rand32() % 1000);
          }
        }
      }
      return result;
    };

    folly::BenchmarkSuspender suspender;
    auto input1 = buildInput();
    auto input2 = buildInput();
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      std::vector<std::optional<bool>> result;
      for (int j = 0; j < input1.size(); j++) {
        if (!input1[j].has_value() || !input2[j].has_value()) {
          result.push_back(std::nullopt);
        } else {
          result.push_back(input1[j] == input2[j]);
        }
      }
      cnt += result.size();
      folly::doNotOptimizeAway(result);
    }
    folly::doNotOptimizeAway(cnt);
    return cnt;
  }
};

BENCHMARK_MULTI(Eq_Velox) {
  CompareBenchmark benchmark;
  return benchmark.run("==");
}

BENCHMARK_MULTI(Eq_FastVeloxInVeloxOut) {
  CompareBenchmark benchmark;
  return benchmark.runFastVeloxInVeloxOut();
}

BENCHMARK_MULTI(Eq_FastVeloxInStdOut) {
  CompareBenchmark benchmark;
  return benchmark.runFastVeloxInStdOut();
}

BENCHMARK_MULTI(Eq_FastStdInStdOut) {
  CompareBenchmark benchmark;
  return benchmark.runFastStdInStdOut();
}

BENCHMARK_MULTI(Neq_Velox) {
  CompareBenchmark benchmark;
  return benchmark.run("=");
}

BENCHMARK_MULTI(Gt_Velox) {
  CompareBenchmark benchmark;
  return benchmark.run(">");
}
} // namespace
} // namespace facebook::velox::functions::test

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}

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

#include <gflags/gflags.h>

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/CheckedArithmeticImpl.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
// Variations of the simple multiply function regarding output values.
template <typename T>
struct MultiplyVoidOutputFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::multiply(a, b);
  }
};

template <typename T>
struct MultiplyNullableOutputFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::multiply(a, b);
    return true;
  }
};

template <typename T>
struct MultiplyNullOutputFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::multiply(a, b);
    return false; // always returns null as a toy example.
  }
};

// Checked vs. Unchecked Arithmetic.
template <typename T>
struct PlusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::plus(a, b);
  }
};

template <typename T>
struct CheckedPlusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::checkedPlus(a, b);
  }
};

class SimpleArithmeticBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  explicit SimpleArithmeticBenchmark() : FunctionBenchmarkBase() {
    registerFunction<MultiplyVoidOutputFunction, double, double, double>(
        {"multiply"});
    registerFunction<MultiplyNullableOutputFunction, double, double, double>(
        {"multiply_nullable_output"});
    registerFunction<MultiplyNullOutputFunction, double, double, double>(
        {"multiply_null_output"});

    registerFunction<PlusFunction, int64_t, int64_t, int64_t>({"plus"});
    registerFunction<CheckedPlusFunction, int64_t, int64_t, int64_t>(
        {"checked_plus"});

    // Set input schema.
    inputType_ = ROW({
        {"a", DOUBLE()},
        {"b", DOUBLE()},
        {"c", INTEGER()},
        {"d", INTEGER()},
        {"constant", DOUBLE()},
        {"half_null", DOUBLE()},
    });

    // Generate input data.
    smallRowVector_ = makeRowVector(100);
    mediumRowVector_ = makeRowVector(1'000);
    largeRowVector_ = makeRowVector(10'000);
  }

  RowVectorPtr makeRowVector(vector_size_t size) {
    VectorFuzzer::Options opts;
    opts.vectorSize = size;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<VectorPtr> children;
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // A
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // B
    children.emplace_back(fuzzer.fuzzFlat(INTEGER())); // C
    children.emplace_back(fuzzer.fuzzFlat(INTEGER())); // D
    children.emplace_back(fuzzer.fuzzConstant(DOUBLE())); // Constant

    opts.nullRatio = 0.5; // 50%
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // HalfNull

    return std::make_shared<RowVector>(
        pool(), inputType_, nullptr, size, std::move(children));
  }

  static constexpr auto kIterationsSmall = 10'000;
  static constexpr auto kIterationsMeduim = 1000;
  static constexpr auto kIterationsLarge = 100;

  void runSmall(const std::string& expression) {
    run(expression, kIterationsSmall, smallRowVector_);
  }

  void runMedium(const std::string& expression) {
    run(expression, kIterationsMeduim, mediumRowVector_);
  }

  void runLarge(const std::string& expression) {
    run(expression, kIterationsLarge, largeRowVector_);
  }

  // Runs `expression` `times` thousand times.
  size_t
  run(const std::string& expression, size_t times, const RowVectorPtr& input) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < times; i++) {
      count += evaluate(exprSet, input)->size();
    }
    return count;
  }

 private:
  TypePtr inputType_;
  RowVectorPtr smallRowVector_;
  RowVectorPtr mediumRowVector_;
  RowVectorPtr largeRowVector_;
};

std::unique_ptr<SimpleArithmeticBenchmark> benchmark;

BENCHMARK(multiplySmall) {
  benchmark->runSmall("multiply(a, b)");
}

BENCHMARK(multiplySameColumnSmall) {
  benchmark->runSmall("multiply(a, a)");
}

BENCHMARK(multiplyHalfNullSmall) {
  benchmark->runSmall("multiply(a, half_null)");
}

BENCHMARK(multiplyConstantSmall) {
  benchmark->runSmall("multiply(a, constant)");
}

BENCHMARK(multiplyLiteralSmall) {
  benchmark->runSmall("multiply(a, cast(2 as double))");
}

BENCHMARK(multiplyNestedSmall) {
  benchmark->runSmall("multiply(multiply(a, b), b)");
}

BENCHMARK(multiplyNestedDeepSmall) {
  benchmark->runSmall(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyOutputVoidSmall) {
  benchmark->runSmall("multiply(a, b)");
}

BENCHMARK(multiplyOutputNullableSmall) {
  benchmark->runSmall("multiply_nullable_output(a, b)");
}

BENCHMARK(multiplyOutputAlwaysNullSmall) {
  benchmark->runSmall("multiply_null_output(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(plusUncheckedSmall) {
  benchmark->runSmall("plus(c, d)");
}

BENCHMARK(plusCheckedSmall) {
  benchmark->runSmall("checked_plus(c, d)");
}

BENCHMARK_DRAW_LINE();
BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyMedium) {
  benchmark->runMedium("multiply(a, b)");
}

BENCHMARK(multiplySameColumnMedium) {
  benchmark->runMedium("multiply(a, a)");
}

BENCHMARK(multiplyHalfNullMedium) {
  benchmark->runMedium("multiply(a, half_null)");
}

BENCHMARK(multiplyConstantMedium) {
  benchmark->runMedium("multiply(a, constant)");
}

BENCHMARK(multiplyLiteralMedium) {
  benchmark->runMedium("multiply(a, cast(2 as double))");
}

BENCHMARK(multiplyNestedMedium) {
  benchmark->runMedium("multiply(multiply(a, b), b)");
}

BENCHMARK(multiplyNestedDeepMedium) {
  benchmark->runMedium(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyOutputVoidMedium) {
  benchmark->runMedium("multiply(a, b)");
}

BENCHMARK(multiplyOutputNullableMedium) {
  benchmark->runMedium("multiply_nullable_output(a, b)");
}

BENCHMARK(multiplyOutputAlwaysNullMedium) {
  benchmark->runMedium("multiply_null_output(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(plusUncheckedMedium) {
  benchmark->runMedium("plus(c, d)");
}

BENCHMARK(plusCheckedMedium) {
  benchmark->runMedium("checked_plus(c, d)");
}

BENCHMARK_DRAW_LINE();
BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyLarge) {
  benchmark->runLarge("multiply(a, b)");
}

BENCHMARK(multiplySameColumnLarge) {
  benchmark->runLarge("multiply(a, a)");
}

BENCHMARK(multiplyHalfNullLarge) {
  benchmark->runLarge("multiply(a, half_null)");
}

BENCHMARK(multiplyConstantLarge) {
  benchmark->runLarge("multiply(a, constant)");
}

BENCHMARK(multiplyLiteralLarge) {
  benchmark->runLarge("multiply(a, cast(2 as double))");
}

BENCHMARK(multiplyNestedLarge) {
  benchmark->runLarge("multiply(multiply(a, b), b)");
}

BENCHMARK(multiplyNestedDeepLarge) {
  benchmark->runLarge(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(multiplyOutputVoidLarge) {
  benchmark->runLarge("multiply(a, b)");
}

BENCHMARK(multiplyOutputNullableLarge) {
  benchmark->runLarge("multiply_nullable_output(a, b)");
}

BENCHMARK(multiplyOutputAlwaysNullLarge) {
  benchmark->runLarge("multiply_null_output(a, b)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(plusUncheckedLarge) {
  benchmark->runLarge("plus(c, d)");
}

BENCHMARK(plusCheckedLarge) {
  benchmark->runLarge("checked_plus(c, d)");
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize({});
  benchmark = std::make_unique<SimpleArithmeticBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}

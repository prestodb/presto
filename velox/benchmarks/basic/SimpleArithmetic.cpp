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
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/functions/prestosql/CheckedArithmeticImpl.h"
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
        {"c", BIGINT()},
        {"d", BIGINT()},
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
    children.emplace_back(fuzzer.fuzzFlat(BIGINT())); // C
    children.emplace_back(fuzzer.fuzzFlat(BIGINT())); // D
    children.emplace_back(fuzzer.fuzzConstant(DOUBLE())); // Constant

    opts.nullRatio = 0.5; // 50%
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // HalfNull

    return std::make_shared<RowVector>(
        pool(), inputType_, nullptr, size, std::move(children));
  }

  size_t runSmall(const std::string& expression, size_t times) {
    return run(expression, times, smallRowVector_);
  }

  size_t runMedium(const std::string& expression, size_t times) {
    return run(expression, times, mediumRowVector_);
  }

  size_t runLarge(const std::string& expression, size_t times) {
    return run(expression, times, largeRowVector_);
  }

  // Runs `expression` `times` thousand times.
  size_t
  run(const std::string& expression, size_t times, const RowVectorPtr& input) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < times * 1'000; i++) {
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

BENCHMARK_MULTI(multiplySmall, n) {
  return benchmark->runSmall("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplySameColumnSmall, n) {
  return benchmark->runSmall("multiply(a, a)", n);
}

BENCHMARK_MULTI(multiplyHalfNullSmall, n) {
  return benchmark->runSmall("multiply(a, half_null)", n);
}

BENCHMARK_MULTI(multiplyConstantSmall, n) {
  return benchmark->runSmall("multiply(a, constant)", n);
}

BENCHMARK_MULTI(multiplyNestedSmall, n) {
  return benchmark->runSmall("multiply(multiply(a, b), b)", n);
}

BENCHMARK_MULTI(multiplyNestedDeepSmall, n) {
  return benchmark->runSmall(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))",
      n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(multiplyOutputVoidSmall, n) {
  return benchmark->runSmall("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputNullableSmall, n) {
  return benchmark->runSmall("multiply_nullable_output(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputAlwaysNullSmall, n) {
  return benchmark->runSmall("multiply_null_output(a, b)", n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(plusUncheckedSmall, n) {
  return benchmark->runSmall("plus(c, d)", n);
}

BENCHMARK_MULTI(plusCheckedSmall, n) {
  return benchmark->runSmall("checked_plus(c, d)", n);
}

BENCHMARK_DRAW_LINE();
BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(multiplyMedium, n) {
  return benchmark->runMedium("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplySameColumnMedium, n) {
  return benchmark->runMedium("multiply(a, a)", n);
}

BENCHMARK_MULTI(multiplyHalfNullMedium, n) {
  return benchmark->runMedium("multiply(a, half_null)", n);
}

BENCHMARK_MULTI(multiplyConstantMedium, n) {
  return benchmark->runMedium("multiply(a, constant)", n);
}

BENCHMARK_MULTI(multiplyNestedMedium, n) {
  return benchmark->runMedium("multiply(multiply(a, b), b)", n);
}

BENCHMARK_MULTI(multiplyNestedDeepMedium, n) {
  return benchmark->runMedium(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))",
      n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(multiplyOutputVoidMedium, n) {
  return benchmark->runMedium("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputNullableMedium, n) {
  return benchmark->runMedium("multiply_nullable_output(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputAlwaysNullMedium, n) {
  return benchmark->runMedium("multiply_null_output(a, b)", n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(plusUncheckedMedium, n) {
  return benchmark->runMedium("plus(c, d)", n);
}

BENCHMARK_MULTI(plusCheckedMedium, n) {
  return benchmark->runMedium("checked_plus(c, d)", n);
}

BENCHMARK_DRAW_LINE();
BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(multiplyLarge, n) {
  return benchmark->runLarge("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplySameColumnLarge, n) {
  return benchmark->runLarge("multiply(a, a)", n);
}

BENCHMARK_MULTI(multiplyHalfNullLarge, n) {
  return benchmark->runLarge("multiply(a, half_null)", n);
}

BENCHMARK_MULTI(multiplyConstantLarge, n) {
  return benchmark->runLarge("multiply(a, constant)", n);
}

BENCHMARK_MULTI(multiplyNestedLarge, n) {
  return benchmark->runLarge("multiply(multiply(a, b), b)", n);
}

BENCHMARK_MULTI(multiplyNestedDeepLarge, n) {
  return benchmark->runLarge(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))",
      n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(multiplyOutputVoidLarge, n) {
  return benchmark->runLarge("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputNullableLarge, n) {
  return benchmark->runLarge("multiply_nullable_output(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputAlwaysNullLarge, n) {
  return benchmark->runLarge("multiply_null_output(a, b)", n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(plusUncheckedLarge, n) {
  return benchmark->runLarge("plus(c, d)", n);
}

BENCHMARK_MULTI(plusCheckedLarge, n) {
  return benchmark->runLarge("checked_plus(c, d)", n);
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<SimpleArithmeticBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}

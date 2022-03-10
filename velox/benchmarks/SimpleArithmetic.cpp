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
#include "velox/parse/ExpressionsParser.h"
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
  SimpleArithmeticBenchmark() : FunctionBenchmarkBase() {
    registerFunction<MultiplyVoidOutputFunction, double, double, double>(
        {"multiply"});
    registerFunction<MultiplyNullableOutputFunction, double, double, double>(
        {"multiply_nullable_output"});
    registerFunction<MultiplyNullOutputFunction, double, double, double>(
        {"multiply_null_output"});

    registerFunction<PlusFunction, int64_t, int64_t, int64_t>({"plus"});
    registerFunction<CheckedPlusFunction, int64_t, int64_t, int64_t>(
        {"checked_plus"});
  }

  void setInput(const TypePtr& inputType, const RowVectorPtr& rowVector) {
    inputType_ = inputType;
    rowVector_ = rowVector;
  }

  // Runs `expression` `times` times.
  size_t run(const std::string& expression, size_t times) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < times; i++) {
      count += evaluate(exprSet, rowVector_)->size();
    }
    return count;
  }

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

SimpleArithmeticBenchmark benchmark;

BENCHMARK_MULTI(multiply, n) {
  return benchmark.run("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplySameColumn, n) {
  return benchmark.run("multiply(a, a)", n);
}

BENCHMARK_MULTI(multiplyHalfNull, n) {
  return benchmark.run("multiply(a, half_null)", n);
}

BENCHMARK_MULTI(multiplyConstant, n) {
  return benchmark.run("multiply(a, constant)", n);
}

BENCHMARK_MULTI(multiplyNested, n) {
  return benchmark.run("multiply(multiply(a, b), b)", n);
}

BENCHMARK_MULTI(multiplyNestedDeep, n) {
  return benchmark.run(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))",
      n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(multiplyOutputVoid, n) {
  return benchmark.run("multiply(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputNullable, n) {
  return benchmark.run("multiply_nullable_output(a, b)", n);
}

BENCHMARK_MULTI(multiplyOutputAlwaysNull, n) {
  return benchmark.run("multiply_null_output(a, b)", n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(plusUnchecked, n) {
  return benchmark.run("plus(c, d)", n);
}

BENCHMARK_MULTI(plusChecked, n) {
  return benchmark.run("checked_plus(c, d)", n);
}

} // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Set input schema.
  auto inputType = ROW({
      {"a", DOUBLE()},
      {"b", DOUBLE()},
      {"c", BIGINT()},
      {"d", BIGINT()},
      {"constant", DOUBLE()},
      {"half_null", DOUBLE()},
  });
  const size_t size = 100'000;

  // Generate input data.
  auto* pool = benchmark.pool();
  VectorFuzzer::Options opts;
  opts.vectorSize = size;
  opts.nullChance = 0;

  std::vector<VectorPtr> children;
  children.emplace_back(
      VectorFuzzer(opts, pool, FLAGS_fuzzer_seed).fuzzFlat(DOUBLE())); // A
  children.emplace_back(
      VectorFuzzer(opts, pool, FLAGS_fuzzer_seed).fuzzFlat(DOUBLE())); // B
  children.emplace_back(
      VectorFuzzer(opts, pool, FLAGS_fuzzer_seed).fuzzFlat(BIGINT())); // C
  children.emplace_back(
      VectorFuzzer(opts, pool, FLAGS_fuzzer_seed).fuzzFlat(BIGINT())); // D
  children.emplace_back(VectorFuzzer(opts, pool, FLAGS_fuzzer_seed)
                            .fuzzConstant(DOUBLE())); // Constant
  opts.nullChance = 2; // 50%
  children.emplace_back(VectorFuzzer(opts, pool, FLAGS_fuzzer_seed)
                            .fuzzFlat(DOUBLE())); // HalfNull

  auto rowVector = std::make_shared<RowVector>(
      pool, inputType, nullptr, size, std::move(children));

  // Set them into the benchmark object and start the tests.
  benchmark.setInput(inputType, rowVector);
  folly::runBenchmarks();
  return 0;
}

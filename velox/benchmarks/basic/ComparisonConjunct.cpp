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
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::functions;

namespace {

template <typename T>
struct PlusFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TInput& result, const TInput& a, const TInput& b) {
    result = functions::plus(a, b);
  }
};

class ComparisonBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit ComparisonBenchmark(size_t vectorSize) : FunctionBenchmarkBase() {
    registerBinaryScalar<EqFunction, bool>({"eq"});
    registerBinaryScalar<NeqFunction, bool>({"neq"});
    registerBinaryScalar<LtFunction, bool>({"lt"});
    registerBinaryScalar<GtFunction, bool>({"gt"});
    registerBinaryScalar<LteFunction, bool>({"lte"});
    registerBinaryScalar<GteFunction, bool>({"gte"});
    registerFunction<BetweenFunction, bool, double, double, double>({"btw"});

    // Use it as a baseline.
    registerFunction<PlusFunction, double, double, double>({"plus"});

    // Set input schema.
    inputType_ = ROW({
        {"a", DOUBLE()},
        {"b", DOUBLE()},
        {"c", DOUBLE()},
        {"d", BOOLEAN()},
        {"e", BOOLEAN()},
        {"constant", DOUBLE()},
        {"half_null", DOUBLE()},
        {"bool_half_null", BOOLEAN()},
    });

    // Generate input data.
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<VectorPtr> children;
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // A
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // B
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // C
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN())); // D
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN())); // E
    children.emplace_back(fuzzer.fuzzConstant(DOUBLE())); // Constant

    opts.nullRatio = 0.5; // 50%
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE())); // HalfNull
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN())); // BoolHalfNull

    rowVector_ = std::make_shared<RowVector>(
        pool(), inputType_, nullptr, vectorSize, std::move(children));
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

std::unique_ptr<ComparisonBenchmark> benchmark;

BENCHMARK_MULTI(plus, n) {
  return benchmark->run("plus(a, b)", n);
}

BENCHMARK_MULTI(eq, n) {
  return benchmark->run("eq(a, b)", n);
}

BENCHMARK_MULTI(neq, n) {
  return benchmark->run("neq(a, b)", n);
}

BENCHMARK_MULTI(gt, n) {
  return benchmark->run("gt(a, b)", n);
}

BENCHMARK_MULTI(lt, n) {
  return benchmark->run("lt(a, b)", n);
}

BENCHMARK_MULTI(between, n) {
  return benchmark->run("btw(a, b, c)", n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(eqToConstant, n) {
  return benchmark->run("eq(a, constant)", n);
}

BENCHMARK_RELATIVE_MULTI(eqHalfNull, n) {
  return benchmark->run("eq(a, half_null)", n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(eqBools, n) {
  return benchmark->run("eq(d, e)", n);
}

BENCHMARK_MULTI(andConjunct, n) {
  return benchmark->run("d AND e", n);
}

BENCHMARK_MULTI(orConjunct, n) {
  return benchmark->run("d OR e", n);
}

BENCHMARK_MULTI(andHalfNull, n) {
  return benchmark->run("d AND bool_half_null", n);
}

BENCHMARK_MULTI(conjunctsNested, n) {
  return benchmark->run(
      "(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))", n);
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<ComparisonBenchmark>(1'000'000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}

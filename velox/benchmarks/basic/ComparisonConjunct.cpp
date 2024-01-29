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
  size_t run(const std::string& expression, size_t times = 100) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();
    // For functions like eq, the construction if the selectivity vector is
    // effect the total runtime, hence we pulled out of the evaluation loop.
    SelectivityVector rows(rowVector_->size());
    size_t count = 0;
    for (auto i = 0; i < times; i++) {
      count += evaluate(exprSet, rowVector_, rows)->size();
    }
    return count;
  }

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

std::unique_ptr<ComparisonBenchmark> benchmark;

BENCHMARK(plus) {
  benchmark->run("plus(a, b)");
}

BENCHMARK(eq) {
  benchmark->run("eq(a, b)");
}

BENCHMARK(neq) {
  benchmark->run("neq(a, b)");
}

BENCHMARK(gt) {
  benchmark->run("gt(a, b)");
}

BENCHMARK(lt) {
  benchmark->run("lt(a, b)");
}

BENCHMARK(between) {
  benchmark->run("btw(a, b, c)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(eqToConstant) {
  benchmark->run("eq(a, constant)");
}

BENCHMARK_RELATIVE(eqHalfNull) {
  benchmark->run("eq(a, half_null)");
}

BENCHMARK_DRAW_LINE();

BENCHMARK(eqBools) {
  benchmark->run("eq(d, e)");
}

BENCHMARK(andConjunct) {
  benchmark->run("d AND e");
}

BENCHMARK(orConjunct) {
  benchmark->run("d OR e");
}

BENCHMARK(andHalfNull) {
  benchmark->run("d AND bool_half_null");
}

BENCHMARK(conjunctsNested) {
  benchmark->run("(d OR e) AND ((d AND (neq(d, (d OR e)))) OR (eq(a, b)))");
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize({});
  benchmark = std::make_unique<ComparisonBenchmark>(1'000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}

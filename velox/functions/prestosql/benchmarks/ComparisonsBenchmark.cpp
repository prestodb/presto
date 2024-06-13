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

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::functions {

void registerVectorFunctions() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_eq, "eq");
  registerBinaryScalar<EqFunction, bool>({"nonsimd_eq"});
  registerFunction<EqFunction, bool, IntervalDayTime, IntervalDayTime>(
      {"nonsimd_eq"});
  registerFunction<EqFunction, bool, IntervalYearMonth, IntervalYearMonth>(
      {"nonsimd_eq"});
}

} // namespace facebook::velox::functions

namespace {
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

class ComparisonsBechmark
    : public facebook::velox::functions::test::FunctionBenchmarkBase {
 public:
  ComparisonsBechmark() {
    registerVectorFunctions();
  }

  RowVectorPtr createRowData(const TypePtr& type) {
    VectorFuzzer::Options opts;
    opts.nullRatio = 0;
    opts.vectorSize = 10'000;
    VectorFuzzer fuzzer(opts, execCtx_.pool());

    return fuzzer.fuzzInputFlatRow(ROW({"c0", "c1"}, {type, type}));
  }

  void run(const std::string& name, const TypePtr& type) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = createRowData(type);
    auto exprSet =
        compileExpression(fmt::format("{}(c0, c1)", name), rowVector->type());
    suspender.dismiss();

    uint32_t cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

std::unique_ptr<ComparisonsBechmark> benchmark;

BENCHMARK(non_simd_bigint_eq) {
  benchmark->run("nonsimd_eq", BIGINT());
}

BENCHMARK_RELATIVE(simd_bigint_eq) {
  benchmark->run("eq", BIGINT());
}

BENCHMARK(non_simd_integer_eq) {
  benchmark->run("nonsimd_eq", INTEGER());
}

BENCHMARK_RELATIVE(simd_integer_eq) {
  benchmark->run("eq", INTEGER());
}

BENCHMARK(non_simd_smallint_eq) {
  benchmark->run("nonsimd_eq", SMALLINT());
}

BENCHMARK_RELATIVE(simd_smallint_eq) {
  benchmark->run("eq", SMALLINT());
}

BENCHMARK(non_simd_tinyint_eq) {
  benchmark->run("nonsimd_eq", TINYINT());
}

BENCHMARK_RELATIVE(simd_tinyint_eq) {
  benchmark->run("eq", TINYINT());
}

BENCHMARK(non_simd_double_eq) {
  benchmark->run("nonsimd_eq", DOUBLE());
}

BENCHMARK_RELATIVE(simd_double_eq) {
  benchmark->run("eq", DOUBLE());
}

BENCHMARK(non_simd_real_eq) {
  benchmark->run("nonsimd_eq", REAL());
}

BENCHMARK_RELATIVE(simd_real_eq) {
  benchmark->run("eq", REAL());
}

BENCHMARK(non_simd_date_eq) {
  benchmark->run("nonsimd_eq", DATE());
}

BENCHMARK_RELATIVE(simd_date_eq) {
  benchmark->run("eq", DATE());
}

BENCHMARK(non_simd_interval_day_time_eq) {
  benchmark->run("nonsimd_eq", INTERVAL_DAY_TIME());
}

BENCHMARK_RELATIVE(simd_interval_day_time_eq) {
  benchmark->run("eq", INTERVAL_DAY_TIME());
}

BENCHMARK(non_simd_interval_year_month_eq) {
  benchmark->run("nonsimd_eq", INTERVAL_YEAR_MONTH());
}

BENCHMARK_RELATIVE(simd_interval_year_month_eq) {
  benchmark->run("eq", INTERVAL_YEAR_MONTH());
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  facebook::velox::memory::MemoryManager::initialize({});

  benchmark = std::make_unique<ComparisonsBechmark>();
  folly::runBenchmarks();

  benchmark.reset();
  return 0;
}

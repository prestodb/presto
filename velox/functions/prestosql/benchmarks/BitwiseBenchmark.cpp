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
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace {
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

VELOX_UDF_BEGIN(bitwise_arithmetic_shift_right_nocheck)
FOLLY_ALWAYS_INLINE bool call(int64_t& result, int64_t number, int64_t shift) {
  result = number >> shift;
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(bitwise_logical_shift_right_nocheck)
FOLLY_ALWAYS_INLINE bool
call(int64_t& result, int64_t number, int64_t shift, int64_t bits) {
  if (bits == 64) {
    result = number >> shift;
    return true;
  }

  result = (number & ((1LL << bits) - 1)) >> shift;
  return true;
}
VELOX_UDF_END();

class BitwiseBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  BitwiseBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerBitwiseFunctions();
    registerFunction<
        udf_bitwise_arithmetic_shift_right_nocheck,
        int64_t,
        int64_t,
        int64_t>({"bitwise_arithmetic_shift_right_nocheck"});
    registerFunction<
        udf_bitwise_logical_shift_right_nocheck,
        int64_t,
        int64_t,
        int64_t,
        int64_t>({"bitwise_logical_shift_right_nocheck"});
  }

  void runBitwise(const std::string& fnName, bool logicalShift = false) {
    folly::BenchmarkSuspender suspender;

    VectorFuzzer::Options opts;
    opts.vectorSize = 100'000;
    VectorFuzzer fuzzer(opts, execCtx_.pool());
    auto vectorLeft = fuzzer.fuzzFlat(BIGINT());
    auto vectorRight = vectorMaker_.flatVector<int32_t>(
        opts.vectorSize,
        [](auto row) {
          return row % 62 + 2; // Ensure value between [2, 64]
        },
        nullptr);

    auto rowVector = logicalShift
        ? vectorMaker_.rowVector({vectorLeft, vectorRight, vectorRight})
        : vectorMaker_.rowVector({vectorLeft, vectorRight});
    auto exprSet = logicalShift
        ? compileExpression(
              fmt::format("{}(c0, c1, c2)", fnName), rowVector->type())
        : compileExpression(
              fmt::format("{}(c0, c1)", fnName), rowVector->type());

    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    uint32_t cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(bitwise_arithmetic_shift_right) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_arithmetic_shift_right");
}

BENCHMARK_RELATIVE(bitwise_arithmetic_shift_right_nocheck) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_arithmetic_shift_right_nocheck");
}

BENCHMARK_RELATIVE(bitwise_right_shift_arithmetic) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_right_shift_arithmetic");
}

BENCHMARK_RELATIVE(bitwise_right_shift) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_right_shift");
}

BENCHMARK_RELATIVE(bitwise_left_shift) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_left_shift");
}

BENCHMARK_RELATIVE(bitwise_xor) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_xor");
}

BENCHMARK_RELATIVE(bitwise_or) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_or");
}

BENCHMARK(bitwise_logical_shift_right) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_logical_shift_right", true);
}

BENCHMARK_RELATIVE(bitwise_logical_shift_right_nocheck) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_logical_shift_right_nocheck", true);
}

BENCHMARK_RELATIVE(bitwise_shift_left) {
  BitwiseBenchmark benchmark;
  benchmark.runBitwise("bitwise_shift_left", true);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}

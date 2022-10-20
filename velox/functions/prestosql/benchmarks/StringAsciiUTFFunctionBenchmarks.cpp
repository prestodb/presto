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
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

namespace {

class StringAsciiUTFFunctionBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  StringAsciiUTFFunctionBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerStringFunctions();
  }

  void runUpperLower(const std::string& fnName, bool utf) {
    folly::BenchmarkSuspender suspender;

    VectorFuzzer::Options opts;
    if (utf) {
      opts.charEncodings.clear();
      opts.charEncodings = {UTF8CharList::UNICODE_CASE_SENSITIVE};
    }

    opts.stringLength = 100;
    opts.vectorSize = 100'000;
    VectorFuzzer fuzzer(opts, execCtx_.pool());
    auto vector = fuzzer.fuzzFlat(VARCHAR());

    auto rowVector = vectorMaker_.rowVector({vector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", fnName), rowVector->type());

    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void runSubStr(bool utf) {
    folly::BenchmarkSuspender suspender;

    VectorFuzzer::Options opts;
    if (utf) {
      opts.charEncodings.clear();
      opts.charEncodings = {
          UTF8CharList::UNICODE_CASE_SENSITIVE,
          UTF8CharList::EXTENDED_UNICODE,
          UTF8CharList::MATHEMATICAL_SYMBOLS};
    }

    opts.stringLength = 100;
    opts.vectorSize = 10'000;
    VectorFuzzer fuzzer(opts, execCtx_.pool());
    auto vector = fuzzer.fuzzFlat(VARCHAR());

    auto positionVector =
        BaseVector::createConstant(25, opts.vectorSize, execCtx_.pool());

    auto rowVector = vectorMaker_.rowVector({vector, positionVector});

    auto exprSet = compileExpression("substr(c0, c1)", rowVector->type());

    suspender.dismiss();
    doRun(exprSet, rowVector);
  }

  void runLPadRPad(const std::string& fnName, bool utf) {
    folly::BenchmarkSuspender suspender;

    VectorFuzzer::Options opts;
    if (utf) {
      opts.charEncodings.clear();
      opts.charEncodings = {
          UTF8CharList::UNICODE_CASE_SENSITIVE,
          UTF8CharList::EXTENDED_UNICODE,
          UTF8CharList::MATHEMATICAL_SYMBOLS};
    }

    opts.stringLength = 10;
    opts.vectorSize = 10'000;
    VectorFuzzer fuzzer(opts, execCtx_.pool());
    auto stringVector = fuzzer.fuzzFlat(VARCHAR());
    auto padStringVector = fuzzer.fuzzFlat(VARCHAR());

    auto sizeVector =
        BaseVector::createConstant(55, opts.vectorSize, execCtx_.pool());

    auto rowVector =
        vectorMaker_.rowVector({stringVector, sizeVector, padStringVector});

    auto exprSet = compileExpression(
        fmt::format("{}(c0, c1, c2)", fnName), rowVector->type());

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

BENCHMARK(utfLower) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runUpperLower("lower", true);
}

BENCHMARK_RELATIVE(asciiLower) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runUpperLower("lower", false);
}

BENCHMARK(utfUpper) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runUpperLower("upper", true);
}

BENCHMARK_RELATIVE(asciiUpper) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runUpperLower("upper", false);
}

BENCHMARK(utfSubStr) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runSubStr(true);
}

BENCHMARK_RELATIVE(asciiSubStr) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runSubStr(false);
}

BENCHMARK(utfLPad) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runLPadRPad("lpad", true);
}

BENCHMARK_RELATIVE(aciiLPad) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runLPadRPad("lpad", false);
}

BENCHMARK(utfRPad) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runLPadRPad("rpad", true);
}

BENCHMARK_RELATIVE(aciiRPad) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runLPadRPad("rpad", false);
}
} // namespace

// Preliminary release run, before ascii optimization.
//============================================================================
//../../velox/functions/prestosql/benchmarks/StringAsciiUTFFunctionBenchmarks.cpprelative
// time/iter  iters/s
//============================================================================
// utfLower                                                    67.71ms    14.77
// asciiLower                                        99.84%    67.82ms    14.75
// utfUpper                                                    67.75ms    14.76
// asciiUpper                                        98.22%    68.98ms    14.50
//============================================================================
int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}

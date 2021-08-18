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
#include "velox/buffer/StringViewBufferHolder.h"
#include "velox/functions/common/VectorFunctions.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

namespace {

/// Generate an ascii random string of size length
std::string generateRandomString(size_t length, bool makeUtf8) {
  const std::string chars = makeUtf8
      ? u8"0123456789\u0041\u0042\u0043\u0044\u0045\u0046\u0047\u0048"
        "\u0049\u0050\u0051\u0052\u0053\u0054\u0056\u0057"
      : "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::string randomString;
  randomString.reserve(length);
  randomString.resize(length);
  for (std::size_t i = 0; i < length; ++i) {
    randomString[i] = chars[folly::Random::rand32() % chars.size()];
  }
  return randomString;
}

class StringAsciiUTFFunctionBenchmark
    : public functions::test::FunctionBenchmarkBase {
  /// Creates a base vector of size size and populates it with strFunc
  VectorPtr createAndPopulateVector(
      const vector_size_t size,
      const size_t stringSize,
      const bool makeUtf) {
    auto stringViewBufferHolder = StringViewBufferHolder(execCtx_.pool());
    auto vector = std::dynamic_pointer_cast<FlatVector<StringView>>(
        FlatVector<StringView>::create(
            CppToType<StringView>::create(), size, execCtx_.pool()));

    for (int i = 0; i < size; i++) {
      vector->set(
          i,
          stringViewBufferHolder.getOwnedValue(
              generateRandomString(stringSize, makeUtf)));
    }
    vector->setStringBuffers(stringViewBufferHolder.moveBuffers());
    return vector;
  }

 public:
  StringAsciiUTFFunctionBenchmark() : FunctionBenchmarkBase() {
    functions::registerVectorFunctions();
  }

  void runStringFunction(const std::string& fnName, bool utf) {
    folly::BenchmarkSuspender suspender;

    auto vector = createAndPopulateVector(10'000, 50, utf);
    auto rowVector = vectorMaker_.rowVector({vector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", fnName), rowVector->type());

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
  benchmark.runStringFunction("lower", true);
}

BENCHMARK_RELATIVE(asciiLower) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runStringFunction("lower", false);
}

BENCHMARK(utfUpper) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runStringFunction("upper", true);
}

BENCHMARK_RELATIVE(asciiUpper) {
  StringAsciiUTFFunctionBenchmark benchmark;
  benchmark.runStringFunction("upper", false);
}

} // namespace

// Preliminary release run, before ascii optimization.
//============================================================================
//../../velox/functions/common/benchmarks/StringAsciiUTFFunctionBenchmarks.cpprelative
// time/iter  iters/s
//============================================================================
// utfLower                                                    67.71ms    14.77
// asciiLower                                        99.84%    67.82ms    14.75
// utfUpper                                                    67.75ms    14.76
// asciiUpper                                        98.22%    68.98ms    14.50
//============================================================================
int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}

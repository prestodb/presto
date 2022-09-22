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

#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::tpch;
using namespace facebook::velox::functions;
using namespace facebook::velox::functions::test;

DEFINE_int32(vector_size, 10000, "Vector size");
DEFINE_int32(num_runs, 10000, "Number of runs");
DEFINE_int32(num_rows, 10000, "Number of rows");

namespace {

enum class TpchBenchmarkCase {
  TpchQuery2,
  TpchQuery9,
  TpchQuery13,
  TpchQuery14,
  TpchQuery16Part,
  TpchQuery16Supplier,
  TpchQuery20,
};

class LikeFunctionsBenchmark : public FunctionBaseTest,
                               public FunctionBenchmarkBase {
 public:
  explicit LikeFunctionsBenchmark() {
    exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);

    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_vector_size;
    VectorFuzzer fuzzer(opts, FunctionBenchmarkBase::pool());
    inputFuzzer_ = fuzzer.fuzzFlat(VARCHAR());
  }

  // Generate random string using characters from characterSet.
  std::string generateRandomString(const char* characterSet) {
    auto characterSetLength = strlen(characterSet);
    auto minimumLength = 1;
    auto maximumLength = 10;
    auto outputStringLength = rand() % maximumLength + minimumLength;
    std::string output;

    for (int i = 0; i < outputStringLength; i++) {
      output += characterSet[rand() % characterSetLength];
    }
    return output;
  }

  std::string generatePattern(
      PatternKind patternKind,
      const std::string& inputString) {
    switch (patternKind) {
      case PatternKind::kExactlyN:
        return std::string(inputString.size(), '_');
      case PatternKind::kAtLeastN:
        return generateRandomString(kWildcardCharacterSet);
      case PatternKind::kPrefix: {
        auto fixedPatternLength =
            std::min(vector_size_t(inputString.size()), 10);
        auto fixedPatternString = inputString.substr(0, fixedPatternLength);
        return fixedPatternString + generateRandomString(kAnyWildcardCharacter);
      }
      case PatternKind::kSuffix: {
        auto fixedPatternStartIdx =
            std::max(vector_size_t(inputString.size() - 10), 0);
        auto fixedPatternString = inputString.substr(fixedPatternStartIdx, 10);
        return generateRandomString(kAnyWildcardCharacter) + fixedPatternString;
      }
      default:
        return inputString;
    }
  }

  const VectorPtr getTpchData(const TpchBenchmarkCase tpchCase) {
    switch (tpchCase) {
      case TpchBenchmarkCase::TpchQuery2:
      case TpchBenchmarkCase::TpchQuery14:
      case TpchBenchmarkCase::TpchQuery16Part: {
        auto tpchPart = genTpchPart(FLAGS_num_rows);
        return tpchPart->childAt(4);
      }
      case TpchBenchmarkCase::TpchQuery9:
      case TpchBenchmarkCase::TpchQuery20: {
        auto tpchPart = genTpchPart(FLAGS_num_rows);
        return tpchPart->childAt(1);
      }
      case TpchBenchmarkCase::TpchQuery13: {
        auto tpchOrders = genTpchOrders(FLAGS_num_rows);
        return tpchOrders->childAt(8);
      }
      case TpchBenchmarkCase::TpchQuery16Supplier: {
        auto tpchSupplier = genTpchSupplier(FLAGS_num_rows);
        return tpchSupplier->childAt(6);
      }
      default:
        VELOX_FAIL(fmt::format(
            "Tpch data generation for case {} is not supported", tpchCase));
    }
  }

  size_t run(const TpchBenchmarkCase tpchCase, const StringView patternString) {
    folly::BenchmarkSuspender kSuspender;
    const auto input = getTpchData(tpchCase);
    const auto data = makeRowVector({input});
    auto likeExpression = fmt::format("like(c0, '{}')", patternString);
    auto rowType = std::dynamic_pointer_cast<const RowType>(data->type());
    exec::ExprSet exprSet =
        FunctionBenchmarkBase::compileExpression(likeExpression, rowType);
    kSuspender.dismiss();

    size_t cnt = 0;
    for (auto i = 0; i < FLAGS_num_runs; i++) {
      auto result = FunctionBenchmarkBase::evaluate(exprSet, data);
      cnt += result->size();
    }
    folly::doNotOptimizeAway(cnt);

    return cnt;
  }

  size_t run(PatternKind patternKind) {
    folly::BenchmarkSuspender kSuspender;
    const auto input = inputFuzzer_->values()->as<StringView>();
    auto patternString = generatePattern(patternKind, input[0].str());
    std::vector<std::string> patternVector(FLAGS_vector_size, patternString);
    const auto data = makeRowVector({inputFuzzer_});
    auto likeExpression = fmt::format("like(c0, '{}')", patternString);
    auto rowType = std::dynamic_pointer_cast<const RowType>(data->type());
    exec::ExprSet exprSet =
        FunctionBenchmarkBase::compileExpression(likeExpression, rowType);
    kSuspender.dismiss();

    size_t cnt = 0;
    for (auto i = 0; i < FLAGS_num_runs; i++) {
      auto result = FunctionBenchmarkBase::evaluate(exprSet, data);
      cnt += result->size();
    }
    folly::doNotOptimizeAway(cnt);

    return cnt;
  }

  // We inherit from FunctionBaseTest so that we can get access to the helpers
  // it defines, but since it is supposed to be a test fixture TestBody() is
  // declared pure virtual.  We must provide an implementation here.
  void TestBody() override {}

 private:
  static constexpr const char* kWildcardCharacterSet = "%_";
  static constexpr const char* kAnyWildcardCharacter = "%";
  VectorPtr inputFuzzer_;
};

std::unique_ptr<LikeFunctionsBenchmark> benchmark;

BENCHMARK_MULTI(wildcardExactlyN) {
  return benchmark->run(PatternKind::kExactlyN);
}

BENCHMARK_MULTI(wildcardAtLeastN) {
  return benchmark->run(PatternKind::kAtLeastN);
}

BENCHMARK_MULTI(fixedPattern) {
  return benchmark->run(PatternKind::kFixed);
}

BENCHMARK_MULTI(prefixPattern) {
  return benchmark->run(PatternKind::kPrefix);
}

BENCHMARK_MULTI(suffixPattern) {
  return benchmark->run(PatternKind::kSuffix);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(tpchQuery2) {
  return benchmark->run(TpchBenchmarkCase::TpchQuery2, "%BRASS");
}

BENCHMARK_MULTI(tpchQuery9) {
  return benchmark->run(TpchBenchmarkCase::TpchQuery9, "%green%");
}

BENCHMARK_MULTI(tpchQuery13) {
  return benchmark->run(TpchBenchmarkCase::TpchQuery13, "%special%requests%");
}

BENCHMARK_MULTI(tpchQuery14) {
  return benchmark->run(TpchBenchmarkCase::TpchQuery14, "PROMO%");
}

BENCHMARK_MULTI(tpchQuery16Part) {
  return benchmark->run(TpchBenchmarkCase::TpchQuery16Part, "MEDIUM POLISHED%");
}

BENCHMARK_MULTI(tpchQuery16Supplier) {
  return benchmark->run(
      TpchBenchmarkCase::TpchQuery16Supplier, "%Customer%Complaints%");
}

BENCHMARK_MULTI(tpchQuery20) {
  return benchmark->run(TpchBenchmarkCase::TpchQuery20, "forest%");
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv, true);
  benchmark = std::make_unique<LikeFunctionsBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();

  return 0;
}

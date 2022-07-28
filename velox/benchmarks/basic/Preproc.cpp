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
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

std::vector<float> kBenchmarkData{
    2.1476,  -1.3686, 0.7764,  -1.1965, 0.3452,  0.9735,  -1.5781, -1.4886,
    -0.3442, -0.2254, 0.8078,  -2.0639, -0.3671, -0.1386, 1.6947,  0.4243,
    0.5946,  -0.5595, 1.8950,  -0.8058, -0.7195, 1.9570,  0.4971,  -1.0579,
    0.1462,  -0.9993, 1.3345,  0.4643,  -0.3859, 2.2679,  1.2498,  -1.2327,
    0.1197,  1.8612,  -0.1551, 0.7999,  1.0617,  0.4072,  -0.1108, -0.6997,
    0.9412,  1.2738,  0.9107,  -0.2011, -1.0226, -1.1124, -0.5597, -1.8072,
    -0.1432, 0.4668,  0.9091,  0.0897,  -1.1366, 0.8039,  -1.1387, -0.8561,
    -1.7241, 1.0493,  -0.1520, -0.1694, -0.2112, -0.1595, 1.5220,  -0.1118,
    -1.7017, 1.3155,  0.8544,  1.2109,  -0.7560, -0.5676, 0.3196,  -1.0065,
    0.5026,  -0.2619, -0.8735, 0.4402,  -0.3373, 0.5434,  0.3734,  -1.0337,
    1.4022,  1.3895,  0.0928,  -1.4310, 0.4486,  -0.7638, 1.3842,  -0.0864,
    0.3958,  1.2399,  -0.6086, -0.4308, 0.1599,  0.6519,  1.3139,  1.3494,
    -1.0165, 0.1431,  -0.7882, -0.1895};

template <typename T>
struct OneHotFunction {
  FOLLY_ALWAYS_INLINE void call(float& result, float a, float b) {
    result = (std::floor(a) == b) ? 1.0f : 0.0f;
  }
};

/// Measures performance of a typical ML preprocessing workload.
class PreprocBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  PreprocBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerAllScalarFunctions();

    registerFunction<OneHotFunction, float, float, float>({"one_hot"});
  }

  exec::ExprSet compile(const std::vector<std::string>& texts) {
    std::vector<core::TypedExprPtr> typedExprs;
    parse::ParseOptions options;
    for (const auto& text : texts) {
      auto untyped = parse::parseExpr(text, options);
      auto typed = core::Expressions::inferTypes(
          untyped, ROW({"c0"}, {REAL()}), execCtx_.pool());
      typedExprs.push_back(typed);
    }
    return exec::ExprSet(std::move(typedExprs), &execCtx_);
  }

  std::string makeExpression(int n, bool useOneHot) {
    if (useOneHot) {
      return fmt::format(
          "clamp(0.05::REAL * (20.5::REAL + one_hot(c0, {}::REAL)), (-10.0)::REAL, 10.0::REAL)",
          n);
    } else {
      return fmt::format(
          "clamp(0.05::REAL * (20.5::REAL + if(floor(c0) = {}::REAL, 1::REAL, 0::REAL)), (-10.0)::REAL, 10.0::REAL)",
          n);
    }
  }

  std::vector<VectorPtr> evaluateOnce(bool useOneHot) {
    auto data = vectorMaker_.rowVector(
        {vectorMaker_.flatVector<float>(kBenchmarkData)});
    auto exprSet = compile({
        makeExpression(1, useOneHot),
        makeExpression(2, useOneHot),
        makeExpression(3, useOneHot),
        makeExpression(4, useOneHot),
        makeExpression(5, useOneHot),
    });

    SelectivityVector rows(data->size());
    std::vector<VectorPtr> results(exprSet.exprs().size());
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    exprSet.eval(rows, &evalCtx, &results);
    return results;
  }

  // Verify that results of the calculation using one_hot function match the
  // results when using if+floor.
  void test() {
    auto onehot = evaluateOnce(true);
    auto original = evaluateOnce(false);

    VELOX_CHECK_EQ(onehot.size(), original.size());
    for (auto i = 0; i < original.size(); ++i) {
      test::assertEqualVectors(onehot[i], original[i]);
    }
  }

  void run(bool useOneHot) {
    folly::BenchmarkSuspender suspender;

    auto data = vectorMaker_.rowVector(
        {vectorMaker_.flatVector<float>(kBenchmarkData)});
    auto exprSet = compile({
        makeExpression(1, useOneHot),
        makeExpression(2, useOneHot),
        makeExpression(3, useOneHot),
        makeExpression(4, useOneHot),
        makeExpression(5, useOneHot),
    });

    SelectivityVector rows(data->size());
    std::vector<VectorPtr> results(exprSet.exprs().size());
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 1'000; i++) {
      exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
      exprSet.eval(rows, &evalCtx, &results);
      cnt += results[0]->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(original) {
  PreprocBenchmark benchmark;
  benchmark.run(false /* useOneHot */);
}

BENCHMARK(onehot) {
  PreprocBenchmark benchmark;
  benchmark.run(true /* useOneHot */);
}

} // namespace

int main(int /*argc*/, char** /*argv*/) {
  // Verify that benchmark calculations are correct.
  {
    PreprocBenchmark benchmark;
    benchmark.test();
  }

  folly::runBenchmarks();
  return 0;
}

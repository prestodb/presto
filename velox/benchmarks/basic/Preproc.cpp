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
#include <velox/common/base/Exceptions.h>
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {
template <typename T, typename Operation>
class BaseFunction : public exec::VectorFunction {
 public:
  virtual bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    if (args[0]->isConstantEncoding() &&
        args[1]->encoding() == VectorEncoding::Simple::FLAT) {
      // Fast path for (flat, const).
      auto constant = args[0]->asUnchecked<SimpleVector<T>>()->valueAt(0);
      auto flatValues = args[1]->asUnchecked<FlatVector<T>>();
      auto rawValues = flatValues->mutableRawValues();

      auto rawResults =
          getRawResults(rows, args[1], rawValues, resultType, context, result);

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        rawResults[row] = Operation::apply(constant, rawValues[row]);
      });
    } else if (
        args[0]->encoding() == VectorEncoding::Simple::FLAT &&
        args[1]->isConstantEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[1]->asUnchecked<SimpleVector<T>>()->valueAt(0);
      auto flatValues = args[0]->asUnchecked<FlatVector<T>>();
      auto rawValues = flatValues->mutableRawValues();

      auto rawResults =
          getRawResults(rows, args[0], rawValues, resultType, context, result);

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        rawResults[row] = Operation::apply(rawValues[row], constant);
      });
    } else if (
        args[0]->encoding() == VectorEncoding::Simple::FLAT &&
        args[1]->encoding() == VectorEncoding::Simple::FLAT) {
      // Fast path for (flat, flat).
      auto flatA = args[0]->asUnchecked<FlatVector<T>>();
      auto rawA = flatA->mutableRawValues();
      auto flatB = args[1]->asUnchecked<FlatVector<T>>();
      auto rawB = flatB->mutableRawValues();

      T* rawResults = nullptr;
      if (!result) {
        if (BaseVector::isVectorWritable(args[0])) {
          rawResults = rawA;
          result = std::move(args[0]);
        } else if (BaseVector::isVectorWritable(args[1])) {
          rawResults = rawB;
          result = std::move(args[1]);
        }
      }
      if (!rawResults) {
        rawResults = prepareResults(rows, resultType, context, result);
      }

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        rawResults[row] = Operation::apply(rawA[row], rawB[row]);
      });
    } else {
      exec::DecodedArgs decodedArgs(rows, args, context);

      auto a = decodedArgs.at(0);
      auto b = decodedArgs.at(1);

      BaseVector::ensureWritable(rows, resultType, context.pool(), result);
      auto rawResults =
          result->asUnchecked<FlatVector<T>>()->mutableRawValues();

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        rawResults[row] =
            Operation::apply(a->valueAt<T>(row), b->valueAt<T>(row));
      });
    }
  }

 private:
  T* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    BaseVector::ensureWritable(rows, resultType, context.pool(), result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<T>>()->mutableRawValues();
  }

  T* getRawResults(
      const SelectivityVector& rows,
      VectorPtr& flat,
      T* rawValues,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Check if input can be reused for results.
    T* rawResults;
    if (!result && BaseVector::isVectorWritable(flat)) {
      rawResults = rawValues;
      result = std::move(flat);
    } else {
      rawResults = prepareResults(rows, resultType, context, result);
    }

    return rawResults;
  }
};

class Multiplication {
 public:
  template <typename T>
  static T apply(T left, T right) {
    return left * right;
  }
};

class Addition {
 public:
  template <typename T>
  static T apply(T left, T right) {
    return left + right;
  }
};

template <typename Operation>
std::shared_ptr<exec::VectorFunction> make(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto typeKind = inputArgs[0].type->kind();
  switch (typeKind) {
    case TypeKind::TINYINT:
      return std::make_shared<BaseFunction<int8_t, Operation>>();
    case TypeKind::SMALLINT:
      return std::make_shared<BaseFunction<int16_t, Operation>>();
    case TypeKind::INTEGER:
      return std::make_shared<BaseFunction<int32_t, Operation>>();
    case TypeKind::BIGINT:
      return std::make_shared<BaseFunction<int64_t, Operation>>();
    case TypeKind::REAL:
      return std::make_shared<BaseFunction<float, Operation>>();
    case TypeKind::DOUBLE:
      return std::make_shared<BaseFunction<double, Operation>>();
    default:
      VELOX_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  for (auto type :
       {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
    signatures.push_back(exec::FunctionSignatureBuilder()
                             .returnType(type)
                             .argumentType(type)
                             .argumentType(type)
                             .build());
  }
  return signatures;
}

void registerPlus(const std::string& name) {
  exec::registerStatefulVectorFunction(name, signatures(), make<Addition>);
}

void registerMultiply(const std::string& name) {
  exec::registerStatefulVectorFunction(
      name, signatures(), make<Multiplication>);
}

enum class RunConfig { Basic, OneHot, VectorAndOneHot, Simple };

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
    functions::registerBinaryScalar<functions::EqFunction, bool>({"simple_eq"});
    registerPlus("add_v");
    registerMultiply("mult_v");
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

  std::string makeExpression(int n, RunConfig config) {
    switch (config) {
      case RunConfig::Basic:
        return fmt::format(
            "clamp(0.05::REAL * (20.5::REAL + if(floor(c0) = {}::REAL, 1::REAL, 0::REAL)), (-10.0)::REAL, 10.0::REAL)",
            n);
      case RunConfig::Simple:
        return fmt::format(
            "clamp(0.05::REAL * (20.5::REAL + if(simple_eq(floor(c0) ,{}::REAL), 1::REAL, 0::REAL)), (-10.0)::REAL, 10.0::REAL)",
            n);
      case RunConfig::OneHot:
        return fmt::format(
            "clamp(0.05::REAL * (20.5::REAL + one_hot(c0, {}::REAL)), (-10.0)::REAL, 10.0::REAL)",
            n);
      case RunConfig::VectorAndOneHot:
        return fmt::format(
            "clamp(mult_v(0.05::REAL, add_v(20.5::REAL, one_hot(c0, {}::REAL))), (-10.0)::REAL, 10.0::REAL)",
            n);
    }
    return "";
  }

  std::vector<VectorPtr> evaluateOnce(RunConfig config) {
    auto data = vectorMaker_.rowVector(
        {vectorMaker_.flatVector<float>(kBenchmarkData)});
    auto exprSet = compile({
        makeExpression(1, config),
        makeExpression(2, config),
        makeExpression(3, config),
        makeExpression(4, config),
        makeExpression(5, config),
    });

    SelectivityVector rows(data->size());
    std::vector<VectorPtr> results(exprSet.exprs().size());
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    exprSet.eval(rows, evalCtx, results);
    return results;
  }

  // Verify that results of the calculation using one_hot function match the
  // results when using if+floor.
  void test() {
    auto onehot = evaluateOnce(RunConfig::OneHot);
    auto original = evaluateOnce(RunConfig::Basic);
    auto vcectorAndOneHot = evaluateOnce(RunConfig::VectorAndOneHot);

    VELOX_CHECK_EQ(onehot.size(), original.size());
    for (auto i = 0; i < original.size(); ++i) {
      test::assertEqualVectors(onehot[i], original[i]);
    }

    VELOX_CHECK_EQ(vcectorAndOneHot.size(), original.size());
    for (auto i = 0; i < original.size(); ++i) {
      test::assertEqualVectors(vcectorAndOneHot[i], original[i]);
    }
  }

  size_t run(RunConfig config, size_t times) {
    folly::BenchmarkSuspender suspender;

    auto scaledData = std::vector<float>();
    scaledData.reserve(kBenchmarkData.size() * scaleFactor_);

    for (int i = 0; i < scaleFactor_; i++) {
      scaledData.insert(
          scaledData.end(), kBenchmarkData.begin(), kBenchmarkData.end());
    }

    auto data =
        vectorMaker_.rowVector({vectorMaker_.flatVector<float>(scaledData)});
    auto exprSet = compile({
        makeExpression(1, config),
        makeExpression(2, config),
        makeExpression(3, config),
        makeExpression(4, config),
        makeExpression(5, config),
    });

    SelectivityVector rows(data->size());
    std::vector<VectorPtr> results(exprSet.exprs().size());
    suspender.dismiss();

    size_t cnt = 0;
    for (auto i = 0; i < times * 10'000; i++) {
      exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
      exprSet.eval(rows, evalCtx, results);
      cnt += results[0]->size();
    }
    return cnt;
  }

  /// Scale factor for the data.
  static auto const scaleFactor_ = 1;
};

BENCHMARK_MULTI(ifFloor, n) {
  PreprocBenchmark benchmark;
  return benchmark.run(RunConfig::Basic, n);
}

// Same as ifFloor, but uses non-SIMD version of the equality operation.
BENCHMARK_MULTI(ifFloorWithSimpleEq, n) {
  PreprocBenchmark benchmark;
  return benchmark.run(RunConfig::Simple, n);
}

// Replaces if + floor expression with a one_hot function call.
BENCHMARK_MULTI(oneHot, n) {
  PreprocBenchmark benchmark;
  return benchmark.run(RunConfig::OneHot, n);
}

// Same as oneHot, but uses vector functions for plus and multiply.
BENCHMARK_MULTI(oneHotWithVectorArithmetic, n) {
  PreprocBenchmark benchmark;
  return benchmark.run(RunConfig::VectorAndOneHot, n);
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

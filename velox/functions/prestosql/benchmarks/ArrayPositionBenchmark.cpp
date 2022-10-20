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
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/benchmarks/ArrayPositionBasic.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

/// A manually specialized array_position implementation with assumptions that
/// the input consists of a flat array vector of a primitive type (except
/// boolean) that has no null value and a constant search vector of the same
/// type.
template <typename T>
VectorPtr fastPosition(const RowVectorPtr& inputs) {
  VectorPtr arrays = inputs->childAt(0);
  VectorPtr searches = inputs->childAt(1);

  const auto numRows = arrays->size();
  auto result =
      std::static_pointer_cast<FlatVector<int64_t>>(BaseVector::create(
          CppToType<int64_t>::create(), numRows, arrays->pool()));

  auto arrayVector = arrays->as<ArrayVector>();
  auto rawOffsets = arrayVector->rawOffsets();
  auto rawSizes = arrayVector->rawSizes();
  auto rawElements = arrayVector->elements()->as<FlatVector<T>>()->rawValues();

  auto search = searches->as<ConstantVector<T>>()->valueAt(0);

  for (auto row = 0; row < numRows; ++row) {
    const auto start = rawOffsets[row];
    const auto end = start + rawSizes[row];
    if (start == end) {
      result->set(row, 0);
    } else {
      int i;
      for (i = start; i < end; ++i) {
        if (rawElements[i] == search) {
          result->set(row, i - start + 1);
          break;
        }
      }
      if (i == end) {
        result->set(row, 0);
      }
    }
  }

  return result;
}

/// A manually specialized array_position implementation with assumptions that
/// the input consists of a flat array vector of a primitive type (except
/// boolean) that has no null value, a constant search vector of the same type,
/// and a constant instance vector.
template <typename T>
VectorPtr fastPositionWithInstance(const RowVectorPtr& inputs) {
  VectorPtr arrays = inputs->childAt(0);
  VectorPtr searches = inputs->childAt(1);
  VectorPtr instances = inputs->childAt(2);

  const auto numRows = arrays->size();
  auto result =
      std::static_pointer_cast<FlatVector<int64_t>>(BaseVector::create(
          CppToType<int64_t>::create(), numRows, arrays->pool()));

  auto arrayVector = arrays->as<ArrayVector>();
  auto rawOffsets = arrayVector->rawOffsets();
  auto rawSizes = arrayVector->rawSizes();
  auto rawElements = arrayVector->elements()->as<FlatVector<T>>()->rawValues();

  auto search = searches->as<ConstantVector<T>>()->valueAt(0);

  auto instance = instances->as<ConstantVector<int64_t>>()->valueAt(0);
  VELOX_USER_CHECK_NE(
      instance, 0, "array_position cannot take a 0-valued instance argument.");

  for (auto row = 0; row < numRows; ++row) {
    const auto start = rawOffsets[row];
    const auto end = start + rawSizes[row];

    if (start == end) {
      result->set(row, 0);
    } else {
      int startIndex = instance > 0 ? start : end - 1;
      int endIndex = instance > 0 ? end : start - 1;
      int step = instance > 0 ? 1 : -1;
      auto instanceCounter = std::abs(instance);

      int i;
      for (i = startIndex; i != endIndex; i += step) {
        if (rawElements[i] == search) {
          --instanceCounter;
          if (instanceCounter == 0) {
            result->set(row, i - start + 1);
            break;
          }
        }
      }
      if (i == endIndex) {
        result->set(row, 0);
      }
    }
  }

  return result;
}

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_position_basic,
    functions::ArrayPositionFunctionBasic::signatures(),
    std::make_unique<functions::ArrayPositionFunctionBasic>());

class ArrayPositionBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArrayPositionBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerArrayFunctions();
    VELOX_REGISTER_VECTOR_FUNCTION(
        udf_array_position_basic, "array_position_basic");
  }

  RowVectorPtr makeData(int vectorCount) {
    const vector_size_t size = 1'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; });

    auto searchVector =
        BaseVector::createConstant(int32_t{7}, size, execCtx_.pool());

    if (vectorCount == 2) {
      return vectorMaker_.rowVector({arrayVector, searchVector});
    } else {
      auto instanceVector =
          BaseVector::createConstant(int64_t{5}, size, execCtx_.pool());
      return vectorMaker_.rowVector(
          {arrayVector, searchVector, instanceVector});
    }
  }

  void runInteger(const std::string& expression, const int parameterCount) {
    folly::BenchmarkSuspender suspender;

    auto rowVector = makeData(parameterCount);
    auto exprSet = compileExpression(expression, rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }

  template <typename F>
  void runFastInteger(F function, const int parameterCount) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(parameterCount);
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += function(data)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(fastInteger) {
  ArrayPositionBenchmark benchmark;
  benchmark.runFastInteger(fastPosition<int32_t>, 2);
}

BENCHMARK_RELATIVE(vectorFastPathInteger) {
  ArrayPositionBenchmark benchmark;
  benchmark.runInteger("array_position(c0, c1)", 2);
}

BENCHMARK_RELATIVE(vectorBasicInteger) {
  ArrayPositionBenchmark benchmark;
  benchmark.runInteger("array_position_basic(c0, c1)", 2);
}

BENCHMARK(fastIntegerWithInstance) {
  ArrayPositionBenchmark benchmark;
  benchmark.runFastInteger(fastPositionWithInstance<int32_t>, 3);
}

BENCHMARK_RELATIVE(vectorFastPathIntegerWithInstance) {
  ArrayPositionBenchmark benchmark;
  benchmark.runInteger("array_position(c0, c1, c2)", 3);
}

BENCHMARK_RELATIVE(vectorBasicIntegerWithInstance) {
  ArrayPositionBenchmark benchmark;
  benchmark.runInteger("array_position_basic(c0, c1, c2)", 3);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}

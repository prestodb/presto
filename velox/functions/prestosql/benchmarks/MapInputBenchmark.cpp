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
#include "velox/expression/EvalCtx.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class MapSumValuesAndKeysVector : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto arg = args.at(0);

    exec::LocalDecodedVector mapHolder(context, *arg, rows);
    auto mapVector = mapHolder->base()->as<MapVector>();
    auto mapIndices = mapHolder->indices();

    auto mapKeys = mapVector->mapKeys();
    exec::LocalSelectivityVector mapKeysRows(context, mapKeys->size());
    exec::LocalDecodedVector mapKeysHolder(context, *mapKeys, *mapKeysRows);
    auto decodedMapKeys = mapKeysHolder.get();

    auto mapValues = mapVector->mapValues();
    exec::LocalSelectivityVector mapValuesRows(context, mapKeys->size());
    exec::LocalDecodedVector mapValuesHolder(
        context, *mapValues, *mapValuesRows);
    auto decodedMapValues = mapValuesHolder.get();

    auto rawOffsets = mapVector->rawOffsets();
    auto rawSizes = mapVector->rawSizes();

    BaseVector::ensureWritable(rows, BIGINT(), context.pool(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    rows.applyToSelected([&](vector_size_t row) {
      size_t mapIndex = mapIndices[row];
      size_t offsetStart = rawOffsets[mapIndex];
      size_t offsetEnd = offsetStart + rawSizes[mapIndex];

      auto sum = 0;
      for (size_t offset = offsetStart; offset < offsetEnd; ++offset) {
        sum += decodedMapKeys->valueAt<int64_t>(offset);
        sum += decodedMapValues->valueAt<int64_t>(offset);
      }
      flatResult->set(row, sum);
    });
  }
};

class NestedMapSumValuesAndKeysVector : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto arg = args.at(0);

    exec::LocalDecodedVector mapHolder(context, *arg, rows);
    auto mapVector = mapHolder->base()->as<MapVector>();
    auto mapIndices = mapHolder->indices();

    auto mapKeys = mapVector->mapKeys();
    exec::LocalSelectivityVector rowsKeys(context, mapKeys->size());
    exec::LocalDecodedVector mapKeysHolder(context, *mapKeys, *rowsKeys);
    auto decodedMapKeys = mapKeysHolder.get();

    auto mapValues = mapVector->mapValues();
    exec::LocalSelectivityVector rowsValues(context, mapValues->size());
    exec::LocalDecodedVector mapValuesHolder(context, *mapValues, *rowsValues);
    auto decodedMapValues = mapValuesHolder.get();

    auto rawOffsets = mapVector->rawOffsets();
    auto rawSizes = mapVector->rawSizes();

    // Read inner map
    auto innerMapVector = decodedMapValues->base()->as<MapVector>();
    exec::LocalSelectivityVector innerMapVectorRows(
        context, innerMapVector->size());
    exec::LocalDecodedVector innerMapHolder(
        context, *innerMapVector, *innerMapVectorRows);
    auto innerMapIndices = innerMapHolder->indices();

    auto innerMapKeys = innerMapVector->mapKeys();
    exec::LocalSelectivityVector innerMapKeysRows(
        context, innerMapKeys->size());
    exec::LocalDecodedVector innerMapKeysHolder(
        context, *innerMapKeys, *innerMapKeysRows);
    auto decodedInnerMapKeys = innerMapKeysHolder.get();

    auto innerMapValues = innerMapVector->mapValues();
    exec::LocalSelectivityVector innerMapValuesRows(
        context, innerMapKeys->size());
    exec::LocalDecodedVector innerMapValuesHolder(
        context, *innerMapValues, *innerMapValuesRows);
    auto decodedInnerMapValues = innerMapValuesHolder.get();

    auto innerRawOffsets = innerMapVector->rawOffsets();
    auto innerRawSizes = innerMapVector->rawSizes();

    // Prepare results
    BaseVector::ensureWritable(rows, BIGINT(), context.pool(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    rows.applyToSelected([&](vector_size_t row) {
      size_t mapIndex = mapIndices[row];
      size_t offsetStart = rawOffsets[mapIndex];
      size_t offsetEnd = offsetStart + rawSizes[mapIndex];

      auto sum = 0;
      for (size_t offset = offsetStart; offset < offsetEnd; ++offset) {
        sum += decodedMapKeys->valueAt<int64_t>(offset);

        size_t innerMapIndex = innerMapIndices[offset];
        size_t innerOffsetStart = innerRawOffsets[innerMapIndex];
        size_t innerOffsetEnd = innerOffsetStart + innerRawSizes[innerMapIndex];
        for (size_t innerOffset = innerOffsetStart;
             innerOffset < innerOffsetEnd;
             ++innerOffset) {
          sum += decodedInnerMapKeys->valueAt<int64_t>(innerOffset);
          sum += decodedInnerMapValues->valueAt<int64_t>(innerOffset);
        }
      }

      flatResult->set(row, sum);
    });
  }
};

// This is an intresting point of comparison, here we use the vector reader
// inside of the vector function. This allows to better isolate hidden factors
// and to time different components.
class NestedMapWithMapView : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    LocalDecodedVector decoded_(context, *args[0], rows);
    using exec_in_t = typename VectorExec::template resolver<
        Map<int64_t, Map<int64_t, int64_t>>>::in_type;
    VectorReader<Map<int64_t, Map<int64_t, int64_t>>> reader{decoded_.get()};

    // Prepare results
    BaseVector::ensureWritable(rows, BIGINT(), context.pool(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    rows.applyToSelected([&](vector_size_t row) {
      auto sum = 0;
      for (const auto& entry : reader[row]) {
        sum += entry.first;
        for (const auto& entryInner : *entry.second) {
          sum += entryInner.first;
          sum += *entryInner.second;
        }
      }

      flatResult->set(row, sum);
    });
  }
};

// Simple function implementations
template <typename T>
struct MapSumValuesAndKeysSimple {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& out,
      const arg_type<Map<int64_t, int64_t>>& map) {
    out = 0;
    for (const auto& entry : map) {
      out += entry.first;
      out += *entry.second;
    }
    return true;
  }
};

template <typename T>
struct NestedMapSumValuesAndKeysSimple {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& out,
      const arg_type<Map<int64_t, Map<int64_t, int64_t>>>& map) {
    out = 0;
    for (const auto& innerMap : map) {
      out += innerMap.first;
      for (const auto& entry : *innerMap.second) {
        out += entry.first;
        out += *entry.second;
      }
    }
    return true;
  }
};

template <typename T>
struct NestedMapSumStructBind {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& out,
      const arg_type<Map<int64_t, Map<int64_t, int64_t>>>& map) {
    out = 0;
    for (const auto& [key, value] : map) {
      out += key;
      for (const auto& [keyInner, valueInner] : *value) {
        out += keyInner;
        out += *valueInner;
      }
    }
    return true;
  }
};

class MapInputBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  MapInputBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerMapFunctions();
    functions::prestosql::registerArrayFunctions();

    registerFunction<MapSumValuesAndKeysSimple, int64_t, Map<int64_t, int64_t>>(
        {"map_sum_simple"});
    facebook::velox::exec::registerVectorFunction(
        "map_sum_vector",
        {exec::FunctionSignatureBuilder()
             .returnType("bigint")
             .argumentType("map(bigint,bigint)")
             .build()},
        std::make_unique<MapSumValuesAndKeysVector>());

    registerFunction<
        NestedMapSumValuesAndKeysSimple,
        int64_t,
        Map<int64_t, Map<int64_t, int64_t>>>({"nested_map_sum_simple"});

    registerFunction<
        NestedMapSumStructBind,
        int64_t,
        Map<int64_t, Map<int64_t, int64_t>>>(
        {"nested_map_sum_simple_struct_bind"});

    facebook::velox::exec::registerVectorFunction(
        "nested_map_sum_vector",
        {exec::FunctionSignatureBuilder()
             .typeVariable("K")
             .typeVariable("V")
             .returnType("bigint")
             .argumentType("map(K,V)")
             .build()},
        std::make_unique<NestedMapSumValuesAndKeysVector>());

    facebook::velox::exec::registerVectorFunction(
        "nested_map_sum_vector_mapview",
        {exec::FunctionSignatureBuilder()
             .typeVariable("K")
             .typeVariable("V")
             .returnType("bigint")
             .argumentType("map(K,V)")
             .build()},
        std::make_unique<NestedMapWithMapView>());
  }

  RowVectorPtr makeMapDataMap() {
    const vector_size_t size = 1'000;
    auto mapVector = vectorMaker_.mapVector<int64_t, int64_t>(
        size,
        [](auto row) { return row % 100; },
        [](auto row) { return row % 100; },
        [](auto row) { return row % 100; });

    return vectorMaker_.rowVector({mapVector});
  }

  RowVectorPtr makeDataNestedMap() {
    const vector_size_t size = 1000;
    auto keysOuter = vectorMaker_.arrayVector<int64_t>(
        size, [](auto) { return 3; }, [](auto row) { return row % 100; });

    auto keysInner = vectorMaker_.arrayVector<int64_t>(
        size,
        [](auto row) { return row % 100; },
        [](auto row) { return row % 100; });

    auto valuesInner = vectorMaker_.arrayVector<int64_t>(
        size,
        [](auto row) { return row % 100; },
        [](auto row) { return row % 100; });

    auto rowVector =
        vectorMaker_.rowVector({keysOuter, keysInner, valuesInner});

    auto expression = compileExpression(
        "map(c0, array_constructor ( map(c1, c2), map(c1, c2), map(c1, c2)))",
        rowVector->type());

    return vectorMaker_.rowVector({evaluate(expression, rowVector)});
  }

  void run(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeMapDataMap();
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void runNested(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeDataNestedMap();
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), rowVector->type());
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

  bool hasSameResults(
      ExprSet& expr1,
      ExprSet& expr2,
      const RowVectorPtr& rowVector) {
    auto result1 = evaluate(expr1, rowVector);
    auto result2 = evaluate(expr2, rowVector);
    auto flatResults1 = result1->asFlatVector<int64_t>();
    auto flatResults2 = result2->asFlatVector<int64_t>();

    if (flatResults1->size() != flatResults2->size()) {
      return false;
    }

    for (auto i = 0; i < flatResults1->size(); i++) {
      if (flatResults1->valueAt(i) != flatResults2->valueAt(i)) {
        return false;
      }
    }

    return true;
  }

  bool testMapSum() {
    auto rowVector = makeMapDataMap();
    auto exprSet1 = compileExpression("map_sum_vector(c0)", rowVector->type());
    auto exprSet2 = compileExpression("map_sum_simple(c0)", rowVector->type());
    return hasSameResults(exprSet1, exprSet2, rowVector);
  }

  bool testNestedMapSum() {
    auto rowVector = makeDataNestedMap();
    auto exprSet1 =
        compileExpression("nested_map_sum_vector(c0)", rowVector->type());
    auto exprSet2 =
        compileExpression("nested_map_sum_simple(c0)", rowVector->type());
    auto exprSet3 = compileExpression(
        "nested_map_sum_vector_mapview(c0)", rowVector->type());
    auto exprSet4 = compileExpression(
        "nested_map_sum_simple_struct_bind(c0)", rowVector->type());

    return hasSameResults(exprSet1, exprSet2, rowVector) &&
        hasSameResults(exprSet3, exprSet2, rowVector) &&
        hasSameResults(exprSet3, exprSet4, rowVector);
  }
};

BENCHMARK(mapSumVectorFunction) {
  MapInputBenchmark benchmark;
  benchmark.run("map_sum_vector");
}

BENCHMARK_RELATIVE(mapSumSimpleFunction) {
  MapInputBenchmark benchmark;
  benchmark.run("map_sum_simple");
}

BENCHMARK(nestedMapSumVectorFunction) {
  MapInputBenchmark benchmark;
  benchmark.runNested("nested_map_sum_vector");
}

BENCHMARK_RELATIVE(nestedMapSumSimpleFunction) {
  MapInputBenchmark benchmark;
  benchmark.runNested("nested_map_sum_simple");
}

BENCHMARK_RELATIVE(nestedMapSumSimpleFunctionStructBind) {
  MapInputBenchmark benchmark;
  benchmark.runNested("nested_map_sum_simple_struct_bind");
}

BENCHMARK_RELATIVE(nestedMapSumVectorFunctionMapView) {
  MapInputBenchmark benchmark;
  benchmark.runNested("nested_map_sum_vector_mapview");
}
} // namespace

int main(int /*argc*/, char** /*argv*/) {
  MapInputBenchmark benchmark;
  if (benchmark.testMapSum() && benchmark.testNestedMapSum()) {
    folly::runBenchmarks();
  } else {
    VELOX_UNREACHABLE("tests failed");
  }
  return 0;
}

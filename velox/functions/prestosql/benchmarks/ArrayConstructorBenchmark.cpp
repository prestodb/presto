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

#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  functions::prestosql::registerArrayFunctions();
  memory::MemoryManager::initialize({});
  auto anotherPool = memory::memoryManager()->addLeafPool("bm");

  ExpressionBenchmarkBuilder benchmarkBuilder;

  auto* pool = benchmarkBuilder.pool();
  auto& vm = benchmarkBuilder.vectorMaker();

  auto createSet = [&](const TypePtr& type,
                       bool withNulls,
                       const VectorPtr& constantInput,
                       const std::string& suffix = "") {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;
    options.nullRatio = withNulls ? 0.2 : 0.0;

    VectorFuzzer fuzzer(options, pool);
    std::vector<VectorPtr> columns;
    columns.push_back(fuzzer.fuzzFlat(type));
    columns.push_back(fuzzer.fuzzFlat(type));
    columns.push_back(fuzzer.fuzzFlat(type));
    columns.push_back(
        BaseVector::createNullConstant(type, options.vectorSize, pool));
    columns.push_back(
        BaseVector::wrapInConstant(options.vectorSize, 0, constantInput));

    auto input = vm.rowVector({"c0", "c1", "c2", "n", "c"}, columns);

    benchmarkBuilder
        .addBenchmarkSet(
            fmt::format(
                "array_constructor_{}_{}{}",
                mapTypeKindToName(type->kind()),
                withNulls ? "nulls" : "nullfree",
                suffix),
            input)
        .addExpression("1", "array_constructor(c0)")
        .addExpression("2", "array_constructor(c0, c1)")
        .addExpression("3", "array_constructor(c0, c1, c2)")
        .addExpression("2_null", "array_constructor(c0, c1, n)")
        .addExpression("2_const", "array_constructor(c0, c1, c)");
  };

  auto constantInteger = BaseVector::createConstant(INTEGER(), 11, 1, pool);
  createSet(INTEGER(), true, constantInteger);
  createSet(INTEGER(), false, constantInteger);

  {
    auto constantString =
        BaseVector::createConstant(VARCHAR(), std::string(20, 'x'), 1, pool);
    createSet(VARCHAR(), true, constantString);
    createSet(VARCHAR(), false, constantString);
  }

  pool = anotherPool.get();
  {
    auto constantString =
        BaseVector::createConstant(VARCHAR(), std::string(20, 'x'), 1, pool);
    createSet(VARCHAR(), true, constantString, "_dp");
    createSet(VARCHAR(), false, constantString, "_dp");
  }

  pool = benchmarkBuilder.pool();

  auto constantRow = vm.rowVector({
      BaseVector::createConstant(INTEGER(), 11, 1, pool),
      BaseVector::createConstant(DOUBLE(), 1.23, 1, pool),
  });
  createSet(ROW({INTEGER(), DOUBLE()}), true, constantRow);
  createSet(ROW({INTEGER(), DOUBLE()}), false, constantRow);

  auto constantArray = vm.arrayVector<int32_t>({{1, 2, 3, 4, 5}});
  createSet(ARRAY(INTEGER()), true, constantArray);
  createSet(ARRAY(INTEGER()), false, constantArray);

  auto constantMap = vm.mapVector<int32_t, float>({{{1, 1.23}, {2, 2.34}}});
  createSet(MAP(INTEGER(), REAL()), true, constantMap);
  createSet(MAP(INTEGER(), REAL()), false, constantMap);

  benchmarkBuilder.registerBenchmarks();

  folly::runBenchmarks();
  return 0;
}

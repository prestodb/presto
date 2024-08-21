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
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

namespace facebook::velox::functions {
extern void registerSubscriptFunction(
    const std::string& name,
    bool enableCaching);
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::testingSetInstance({});

  ExpressionBenchmarkBuilder benchmarkBuilder;
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::functions::registerSubscriptFunction(
      "subscriptNocaching", false);

  auto* pool = benchmarkBuilder.pool();
  auto& vm = benchmarkBuilder.vectorMaker();

  auto createSet = [&](const TypePtr& mapType,
                       size_t mapLength,
                       size_t baseVectorSize,
                       size_t numberOfBatches = 1000) {
    VectorFuzzer::Options options;
    options.vectorSize = 1000;
    options.containerLength = mapLength;
    // Make sure it's big enough for nested complex types.
    options.complexElementsMaxSize = baseVectorSize * mapLength * mapLength;
    options.containerVariableLength = false;

    VectorFuzzer fuzzer(options, pool);
    std::vector<VectorPtr> columns;

    // Fuzz input map vector.
    auto flatBase = fuzzer.fuzzFlat(mapType, baseVectorSize);
    auto dictionary = fuzzer.fuzzDictionary(flatBase, options.vectorSize);

    columns.push_back(dictionary);

    // Fuzz indices vector.
    DecodedVector decoded(*dictionary);
    auto* map = flatBase->as<MapVector>();
    auto indices = allocateIndices(options.vectorSize, pool);
    auto* mutableIndices = indices->asMutable<vector_size_t>();

    for (int i = 0; i < options.vectorSize; i++) {
      auto mapIndex = decoded.index(i);
      // Select a random exisiting key except when map is empty.
      if (map->sizeAt(mapIndex) == 0) {
        mutableIndices[i] = 0;
      }
      mutableIndices[i] = folly::Random::rand32() % map->sizeAt(mapIndex) +
          map->offsetAt(mapIndex);
    }

    auto indicesVector = BaseVector::wrapInDictionary(
        nullptr, indices, options.vectorSize, map->mapKeys());

    columns.push_back(indicesVector);

    auto name = fmt::format(
        "{}_{}_{}", mapType->childAt(0)->toString(), mapLength, baseVectorSize);

    benchmarkBuilder.addBenchmarkSet(name, vm.rowVector(columns))
        .addExpression("subscript", "element_at(c0, c1)")
        .addExpression("subscriptNocaching", "subscriptNocaching(c0, c1)")
        .withIterations(numberOfBatches);
  };

  auto createSetsForType = [&](const auto& keyType) {
    createSet(MAP(keyType, INTEGER()), 10, 1);
    createSet(MAP(keyType, INTEGER()), 10, 1000);

    createSet(MAP(keyType, INTEGER()), 100, 1);
    createSet(MAP(keyType, INTEGER()), 100, 1000);

    createSet(MAP(keyType, INTEGER()), 1000, 1);
    createSet(MAP(keyType, INTEGER()), 1000, 1000);

    createSet(MAP(keyType, INTEGER()), 10000, 1);
    createSet(MAP(keyType, INTEGER()), 10000, 1000);
  };

  createSetsForType(INTEGER());
  createSetsForType(VARCHAR());

  // For complex types, caching only applies if the Vector has a single constant
  // value, so we only run with a baseVectorSize of 1.  Also, due to the
  // cost of the cardinality explosion from having nested complex types, we
  // limit the number of iterations to 100.
  auto createSetsForComplexType = [&](const auto& keyType) {
    createSet(MAP(keyType, INTEGER()), 10, 1, 100);

    createSet(MAP(keyType, INTEGER()), 100, 1, 100);

    createSet(MAP(keyType, INTEGER()), 1000, 1, 100);

    createSet(MAP(keyType, INTEGER()), 10000, 1, 100);
  };

  createSetsForComplexType(ARRAY(BIGINT()));
  createSetsForComplexType(MAP(INTEGER(), VARCHAR()));

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();

  folly::runBenchmarks();
  return 0;
}

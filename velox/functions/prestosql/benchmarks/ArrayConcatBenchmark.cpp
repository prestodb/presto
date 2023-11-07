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

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  ExpressionBenchmarkBuilder benchmarkBuilder;
  facebook::velox::functions::prestosql::registerAllScalarFunctions();

  auto* pool = benchmarkBuilder.pool();
  auto& vm = benchmarkBuilder.vectorMaker();

  auto createSet =
      [&](const TypePtr& elementType,
          const std::vector<int>& containerLengths = {5, 10, 20, 40}) {
        VectorFuzzer::Options options;
        options.vectorSize = 1000;
        options.complexElementsMaxSize = 1'000'000;
        options.containerVariableLength = false;

        for (auto length : containerLengths) {
          options.containerLength = length;
          options.stringLength = length;
          VectorFuzzer fuzzer(options, pool);

          std::vector<VectorPtr> columns;
          for (int i = 0; i < 4; i++) {
            columns.push_back(fuzzer.fuzz(ARRAY(elementType)));
          }

          benchmarkBuilder
              .addBenchmarkSet(
                  fmt::format(
                      "array_concat_{}_{}",
                      elementType->toString(),
                      options.containerLength),
                  vm.rowVector(columns))
              .addExpression("2arg", "concat(c0, c1)")
              .addExpression("3arg", "concat(c0, c1, c2)")
              .addExpression("4arg", "concat(c0, c1, c2, c3)");
        }
      };

  createSet(INTEGER());
  createSet(VARCHAR());
  createSet(BOOLEAN());

  benchmarkBuilder.registerBenchmarks();

  folly::runBenchmarks();
  return 0;
}

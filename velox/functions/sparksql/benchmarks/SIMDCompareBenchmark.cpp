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
#include "velox/functions/sparksql/Register.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook;

using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});
  functions::sparksql::registerFunctions("");
  std::vector<TypePtr> inputTypes = {
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      VARCHAR(),
  };
  ExpressionBenchmarkBuilder benchmarkBuilder;
  for (auto nullRatio : {0.0, 0.1, 0.9}) {
    for (auto& inputType : inputTypes) {
      VectorFuzzer::Options opts;
      opts.vectorSize = 10000;
      opts.nullRatio = nullRatio;
      VectorFuzzer fuzzer(opts, benchmarkBuilder.pool());
      std::vector<VectorPtr> childrenVectors = {
          fuzzer.fuzzDictionary(fuzzer.fuzzFlat(inputType)),
          fuzzer.fuzzFlat(inputType)};
      benchmarkBuilder
          .addBenchmarkSet(
              fmt::format(
                  "Dict#{}#{}\%null", inputType->toString(), nullRatio * 100),
              fuzzer.fuzzRow(
                  std::move(childrenVectors), {"c0", "c1"}, opts.vectorSize))
          .addExpression("equalto", "equalto(c0, c1)")
          .addExpression("greaterthan", "greaterthan(c0, c1)")
          .addExpression("greaterthanorequal", "greaterthanorequal(c0, c1)")
          .withIterations(100);
      benchmarkBuilder
          .addBenchmarkSet(
              fmt::format(
                  "Flat#{}#{}\%null", inputType->toString(), nullRatio * 100),
              ROW({"c0", "c1"}, {inputType, inputType}))
          .withFuzzerOptions(opts)
          .addExpression("equalto", "equalto(c0, c1)")
          .addExpression("greaterthan", "greaterthan(c0, c1)")
          .addExpression("greaterthanorequal", "greaterthanorequal(c0, c1)")
          .withIterations(100);
    }
  }

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}

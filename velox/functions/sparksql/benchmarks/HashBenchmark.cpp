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

using namespace facebook;

using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});
  functions::sparksql::registerFunctions("");

  ExpressionBenchmarkBuilder benchmarkBuilder;

  std::vector<TypePtr> inputTypes = {
      TINYINT(),
      SMALLINT(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      VARCHAR(),
      DECIMAL(18, 6),
      DECIMAL(38, 6),
      UNKNOWN(),
      ARRAY(MAP(INTEGER(), VARCHAR())),
      ROW({"f_map", "f_array"}, {MAP(INTEGER(), VARCHAR()), ARRAY(INTEGER())}),
  };
  for (auto nullRatio : {0.0, 0.25}) {
    for (auto& inputType : inputTypes) {
      benchmarkBuilder
          .addBenchmarkSet(
              fmt::format(
                  "hash#{}#{}\%nulls", inputType->toString(), nullRatio * 100),
              ROW({"c0"}, {inputType}))
          .withFuzzerOptions({.vectorSize = 4096, .nullRatio = nullRatio})
          .addExpression("hash", "hash(c0)")
          .addExpression("xxhash64", "xxhash64(c0)")
          .withIterations(100);
    }
  }

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}

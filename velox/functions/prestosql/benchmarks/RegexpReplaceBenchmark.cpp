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
  memory::MemoryManager::initialize({});
  facebook::velox::functions::prestosql::registerAllScalarFunctions();

  ExpressionBenchmarkBuilder benchmarkBuilder;

  VectorFuzzer::Options options;
  options.vectorSize = 1'000;
  options.nullRatio = 0.01;

  auto* pool = benchmarkBuilder.pool();
  auto& vm = benchmarkBuilder.vectorMaker();

  // Compare regexp_replace with fixed and lambda replacement.
  benchmarkBuilder.addBenchmarkSet("lambda_one_group", ROW({"c0"}, {VARCHAR()}))
      .withFuzzerOptions(options)
      .addExpression("fixed", "regexp_replace(c0, '(\\w)', '$1')")
      .addExpression("lambda", "regexp_replace(c0, '(\\w)', x -> x[1])");
  benchmarkBuilder
      .addBenchmarkSet("lambda_two_groups", ROW({"c0"}, {VARCHAR()}))
      .withFuzzerOptions(options)
      .addExpression("fixed", "regexp_replace(c0, '(\\w)(\\w*)', '$1$2')")
      .addExpression(
          "lambda",
          "regexp_replace(c0, '(\\w)(\\w*)', x -> concat(x[1], x[2]))");

  // Compare regexp_replace with fixed-string pattern and replace.
  benchmarkBuilder
      .addBenchmarkSet("fixed_replacement", ROW({"c0"}, {VARCHAR()}))
      .withFuzzerOptions(options)
      .addExpression("replace", "replace(c0, 'a', '_')")
      .addExpression("regexp_replace", "regexp_replace(c0, 'a','_')");

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();

  folly::runBenchmarks();
  return 0;
}

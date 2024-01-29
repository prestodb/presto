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
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::functions;
using namespace facebook::velox::functions::test;
using namespace facebook::velox::memory;
using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});
  exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);
  // Register the scalar functions.
  prestosql::registerAllScalarFunctions("");

  // exec::register
  ExpressionBenchmarkBuilder benchmarkBuilder;
  const vector_size_t vectorSize = 1000;
  auto vectorMaker = benchmarkBuilder.vectorMaker();

  auto makeInput =
      [&](vector_size_t vectorSize, bool padAtHead, bool padAtTail) {
        return vectorMaker.flatVector<std::string>(vectorSize, [&](auto row) {
          // Strings in even rows contain/start with/end with a_b_c depends on
          // value of padAtHead && padAtTail.
          if (row % 2 == 0) {
            auto padding = std::string(row / 2 + 1, 'x');
            if (padAtHead && padAtTail) {
              return fmt::format("{}a_b_c{}", padding, padding);
            } else if (padAtHead) {
              return fmt::format("{}a_b_c", padding);
            } else if (padAtTail) {
              return fmt::format("a_b_c{}", padding);
            } else {
              return std::string("a_b_c");
            }
          } else {
            return std::string(row, 'x');
          }
        });
      };

  auto substringInput = makeInput(vectorSize, true, true);
  auto prefixInput = makeInput(vectorSize, false, true);
  auto suffixInput = makeInput(vectorSize, true, false);

  benchmarkBuilder
      .addBenchmarkSet(
          "like_substring", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("like_substring", R"(like(col0, '%a\_b\_c%', '\'))")
      .addExpression("strpos", R"(strpos(col0, 'a_b_c') > 0)");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_prefix", vectorMaker.rowVector({"col0"}, {prefixInput}))
      .addExpression("like_prefix", R"(like(col0, 'a\_b\_c%', '\'))")
      .addExpression("starts_with", R"(starts_with(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_suffix", vectorMaker.rowVector({"col0"}, {suffixInput}))
      .addExpression("like_suffix", R"(like(col0, '%a\_b\_c', '\'))")
      .addExpression("ends_with", R"(ends_with(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_generic", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("like_generic", R"(like(col0, '%a%b%c'))");

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();
  folly::runBenchmarks();
  return 0;
}

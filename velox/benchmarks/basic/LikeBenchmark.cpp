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

  ExpressionBenchmarkBuilder benchmarkBuilder;
  const vector_size_t vectorSize = 1000;
  auto vectorMaker = benchmarkBuilder.vectorMaker();

  auto makeInput = [&](vector_size_t vectorSize,
                       bool padAtHead,
                       bool padAtTail,
                       std::string content = "a_b_c",
                       std::string paddingStr = "xxx") {
    return vectorMaker.flatVector<std::string>(vectorSize, [&](auto row) {
      // Strings in even rows contain/start with/end with a_b_c depends on
      // value of padAtHead && padAtTail.

      // Calculates the padding.
      std::ostringstream os;
      for (auto i = 0; i < row / 2 + 1; ++i) {
        os << paddingStr;
      }
      auto padding = os.str();

      if (row % 2 == 0) {
        if (padAtHead && padAtTail) {
          return fmt::format("{}{}{}", padding, content, padding);
        } else if (padAtHead) {
          return fmt::format("{}{}", padding, content);
        } else if (padAtTail) {
          return fmt::format("{}{}", content, padding);
        } else {
          return content;
        }
      } else {
        // Yes, two padding concatenated, since we have a '/2' above.
        return padding + padding;
      }
    });
  };

  auto substringInput = makeInput(vectorSize, true, true);
  auto prefixInput = makeInput(vectorSize, false, true);
  auto prefixUnicodeInput = makeInput(vectorSize, false, true, "你_好_啊");
  auto suffixInput = makeInput(vectorSize, true, false);
  auto suffixUnicodeInput = makeInput(vectorSize, true, false, "你_好_啊");

  benchmarkBuilder
      .addBenchmarkSet(
          "substring", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("substring", R"(like(col0, '%a\_b\_c%', '\'))")
      .addExpression("strpos", R"(strpos(col0, 'a_b_c') > 0)");

  benchmarkBuilder
      .addBenchmarkSet(
          "prefix",
          vectorMaker.rowVector(
              {"col0", "col1"}, {prefixInput, prefixUnicodeInput}))
      .addExpression("prefix", R"(like(col0, 'a\_b\_c%', '\'))")
      .addExpression("relaxed_prefix_1", R"(like(col0, 'a\__\_c%', '\'))")
      .addExpression("relaxed_prefix_2", R"(like(col0, '_\__\_c%', '\'))")
      .addExpression(
          "relaxed_prefix_unicode_1", R"(like(col1, '你\__\_啊%', '\'))")
      .addExpression(
          "relaxed_prefix_unicode_2", R"(like(col1, '_\__\_啊%', '\'))")
      .addExpression("starts_with", R"(starts_with(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "suffix",
          vectorMaker.rowVector(
              {"col0", "col1"}, {suffixInput, suffixUnicodeInput}))
      .addExpression("suffix", R"(like(col0, '%a\_b\_c', '\'))")
      .addExpression("relaxed_suffix_1", R"(like(col0, '%a\__\_c', '\'))")
      .addExpression("relaxed_suffix_2", R"(like(col0, '%_\__\_c', '\'))")
      .addExpression(
          "relaxed_suffix_unicode_1", R"(like(col1, '%你\__\_啊', '\'))")
      .addExpression(
          "relaxed_suffix_unicode_2", R"(like(col1, '%_\__\_啊', '\'))")
      .addExpression("ends_with", R"(ends_with(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "generic", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("generic", R"(like(col0, '%a%b%c'))");

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();
  folly::runBenchmarks();
  return 0;
}

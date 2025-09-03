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

#include "velox/core/Expressions.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/sparksql/registration/Register.h"

using namespace facebook::velox;
namespace {
class FromJsonBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  FromJsonBenchmark() : FunctionBenchmarkBase() {
    functions::sparksql::registerFunctions("");
  }

  RowVectorPtr makeData(const StringView& input) {
    const vector_size_t size = 1'000;

    return vectorMaker_.rowVector(
        {"c0"},
        {vectorMaker_.flatVector<StringView>(
            size, [&](vector_size_t /*row*/) { return input; })});
  }

  void run(const StringView& input, const TypePtr& outputType) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeData(input);

    auto typedExpr = std::make_shared<const core::CallTypedExpr>(
        outputType,
        "from_json",
        std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"));
    exec::ExprSet exprSet({typedExpr}, &execCtx_);
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    for (auto i = 0; i < 1000; i++) {
      evaluate(exprSet, rowVector)->size();
    }
  }
};

BENCHMARK(bm_json_depth1_1col) {
  FromJsonBenchmark benchmark;
  benchmark.run(StringView(R"({"a": 1})"), ROW({"a"}, {BIGINT()}));
}

BENCHMARK(bm_json_depth1_4col) {
  FromJsonBenchmark benchmark;
  benchmark.run(
      StringView(R"({"a": 1, "b": 2, "c": 2, "d": 2})"),
      ROW({"a", "b", "c", "d"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()}));
}

BENCHMARK(bm_json_depth1_6col) {
  FromJsonBenchmark benchmark;
  benchmark.run(
      StringView(R"({"a": 1, "b": 2, "c": 2, "d": 2, "e": 1, "f": 2})"),
      ROW({"a", "b", "c", "d", "e", "f"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()}));
}

BENCHMARK(bm_json_depth1_8col) {
  FromJsonBenchmark benchmark;
  benchmark.run(
      StringView(
          R"({"a": 1, "b": 2, "c": 2, "d": 2, "e": 1, "f": 2, "g": 2, "h": 2})"),
      ROW({"a", "b", "c", "d", "e", "f", "g", "h"},
          {BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT(),
           BIGINT()}));
}

BENCHMARK(bm_json_depth2_2col) {
  FromJsonBenchmark benchmark;
  benchmark.run(
      StringView(R"({"a": {"c": 2, "d": 2}, "b": {"e": 2, "f": 2}})"),
      ROW({"a", "b"},
          {ROW({"c", "d"}, {BIGINT(), BIGINT()}),
           ROW({"e", "f"}, {BIGINT(), BIGINT()})}));
}

BENCHMARK(bm_json_depth3_2col) {
  FromJsonBenchmark benchmark;
  benchmark.run(
      StringView(
          R"({"a": {"c": {"e": 2, "f": 2}}, "b": {"d": {"g": 2, "h": 2}}})"),
      ROW({"a", "b"},
          {ROW({"c"}, {ROW({"e", "f"}, {BIGINT(), BIGINT()})}),
           ROW({"d"}, {ROW({"g", "h"}, {BIGINT(), BIGINT()})})}));
}

BENCHMARK(bm_json_depth4_2col) {
  FromJsonBenchmark benchmark;
  benchmark.run(
      StringView(
          R"({"a": {"c": {"e": {"g": 2, "h": 2}}}, "b": {"d": {"f": {"i": 2, "j": 2}}}})"),
      ROW({"a", "b"},
          {ROW({"c"}, {ROW({"e"}, {ROW({"g", "h"}, {BIGINT(), BIGINT()})})}),
           ROW({"b"}, {ROW({"d"}, {ROW({"i", "j"}, {BIGINT(), BIGINT()})})})}));
}
} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}

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

#include <gflags/gflags.h>

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
const std::string_view kColName{"a"};

class SimpleCastBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit SimpleCastBenchmark() : FunctionBenchmarkBase() {}

  RowVectorPtr makeRowVector(const TypePtr& type, vector_size_t size) {
    VectorFuzzer::Options opts;
    opts.vectorSize = size;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(std::move(opts), pool(), FLAGS_fuzzer_seed);
    VectorPtr input = fuzzer.fuzzFlat(type); // Col a
    return vectorMaker_.rowVector(
        std::vector<std::string>{std::string(kColName)},
        std::vector<VectorPtr>{std::move(input)});
  }

  static constexpr auto kNumSmallRuns = 100;
  static constexpr auto kNumMediumRuns = 1000;
  static constexpr auto kNumLargeRuns = 10'000;

  void runSmall(const TypePtr& inputType, const TypePtr& outputType) {
    run(inputType, outputType, kNumSmallRuns);
  }

  void runMedium(const TypePtr& inputType, const TypePtr& outputType) {
    run(inputType, outputType, kNumMediumRuns);
  }

  void runLarge(const TypePtr& inputType, const TypePtr& outputType) {
    run(inputType, outputType, kNumLargeRuns);
  }

  size_t
  run(const TypePtr& inputType, const TypePtr& outputType, size_t batchSize) {
    folly::BenchmarkSuspender suspender;
    auto input = makeRowVector(inputType, batchSize);
    auto castInput =
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            inputType, std::string(kColName));
    std::vector<facebook::velox::core::TypedExprPtr> expr{
        std::make_shared<facebook::velox::core::CastTypedExpr>(
            outputType, castInput, false)};
    exec::ExprSet exprSet(expr, &execCtx_);
    SelectivityVector rows(input->size());
    VectorPtr result;
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < 1000; i++) {
      evaluate(exprSet, input, rows, result);
      count += result->size();
    }
    return count;
  }
};

TypePtr buildStructType(
    std::function<std::string(int)>&& nameGenerator,
    const TypePtr& fieldType,
    size_t numChildren) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < numChildren; i++) {
    names.push_back(nameGenerator(i));
    types.push_back(fieldType);
  }
  return ROW(std::move(names), std::move(types));
}

std::unique_ptr<SimpleCastBenchmark> benchmark;

BENCHMARK(castTimestampDateSmall) {
  benchmark->setAdjustTimestampToTimezone("false");
  benchmark->runSmall(TIMESTAMP(), DATE());
}

BENCHMARK(castTimestampDateMedium) {
  benchmark->setAdjustTimestampToTimezone("false");
  benchmark->runMedium(TIMESTAMP(), DATE());
}

BENCHMARK(castTimestampDateLarge) {
  benchmark->setAdjustTimestampToTimezone("false");
  benchmark->runLarge(TIMESTAMP(), DATE());
}

BENCHMARK(castTimestampDateAdjustTimeZoneSmall) {
  benchmark->setTimezone("America/Los_Angeles");
  benchmark->setAdjustTimestampToTimezone("true");
  benchmark->runSmall(TIMESTAMP(), DATE());
}

BENCHMARK(castTimestampDateAdjustTimeZoneMedium) {
  benchmark->setTimezone("America/Los_Angeles");
  benchmark->setAdjustTimestampToTimezone("true");
  benchmark->runMedium(TIMESTAMP(), DATE());
}

BENCHMARK(castTimestampDateAdjustTimeZoneLarge) {
  benchmark->setTimezone("America/Los_Angeles");
  benchmark->setAdjustTimestampToTimezone("true");
  benchmark->runLarge(TIMESTAMP(), DATE());
}

BENCHMARK(castStructFewFieldsRenameSmall) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 3);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, INTEGER(), 3);
  suspender.dismiss();

  benchmark->runSmall(oldType, newType);
}

BENCHMARK(castStructFewFieldsRenameMedium) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 3);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, INTEGER(), 3);
  suspender.dismiss();

  benchmark->runMedium(oldType, newType);
}

BENCHMARK(castStructFewFieldsRenameLarge) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 3);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, INTEGER(), 3);
  suspender.dismiss();

  benchmark->runLarge(oldType, newType);
}

BENCHMARK(castStructManyFieldsRenameSmall) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 1000);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, INTEGER(), 1000);
  suspender.dismiss();

  benchmark->runSmall(oldType, newType);
}

BENCHMARK(castStructManyFieldsRenameMedium) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 1000);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, INTEGER(), 1000);
  suspender.dismiss();

  benchmark->runMedium(oldType, newType);
}

BENCHMARK(castStructManyFieldsRenameLarge) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 1000);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, INTEGER(), 1000);
  suspender.dismiss();

  benchmark->runLarge(oldType, newType);
}

BENCHMARK(castStructFewFieldsNestedCastSmall) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 3);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, BIGINT(), 3);
  suspender.dismiss();

  benchmark->runSmall(oldType, newType);
}

BENCHMARK(castStructFewFieldsNestedCastMedium) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 3);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, BIGINT(), 3);
  suspender.dismiss();

  benchmark->runMedium(oldType, newType);
}

BENCHMARK(castStructFewFieldsNestedCastLarge) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 3);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, BIGINT(), 3);
  suspender.dismiss();

  benchmark->runLarge(oldType, newType);
}

BENCHMARK(castStructManyFieldsNestedCastSmall) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 1000);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, BIGINT(), 1000);
  suspender.dismiss();

  benchmark->runSmall(oldType, newType);
}

BENCHMARK(castStructManyFieldsNestedCastMedium) {
  folly::BenchmarkSuspender suspender;
  auto oldType = buildStructType([](int) { return ""; }, INTEGER(), 1000);
  auto newType = buildStructType(
      [](int i) { return fmt::format("col{}", i); }, BIGINT(), 1000);
  suspender.dismiss();

  benchmark->runMedium(oldType, newType);
}

// castStructManyFieldsNestedCastLarge is skipped because it takes too long to
// run.

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize({});
  benchmark = std::make_unique<SimpleCastBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}

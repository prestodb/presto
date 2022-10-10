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
#include <memory>
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

VectorPtr fastCardinality(const VectorPtr& vector) {
  auto arrayVector = vector->as<ArrayVector>();
  auto sizes = arrayVector->sizes();
  auto rawSizes = sizes->as<vector_size_t>();

  auto numRows = vector->size();

  BufferPtr values = AlignedBuffer::allocate<int64_t>(numRows, vector->pool());
  auto rawValues = values->asMutable<int64_t>();
  for (auto i = 0; i < numRows; ++i) {
    rawValues[i] = rawSizes[i];
  }

  return std::make_shared<FlatVector<int64_t>>(
      vector->pool(), nullptr, numRows, values, std::vector<BufferPtr>{});
}

class CardinalityVectorFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& arg = args[0];

    context.ensureWritable(rows, BIGINT(), result);
    auto rawValues =
        result->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();
    result->clearNulls(rows);

    if (arg->encoding() == VectorEncoding::Simple::ARRAY) {
      auto arrayVector = arg->asUnchecked<ArrayVector>();
      auto sizes = arrayVector->sizes();
      auto rawSizes = sizes->as<vector_size_t>();
      rows.applyToSelected([&](auto row) { rawValues[row] = rawSizes[row]; });
    } else {
      VELOX_CHECK(arg->isConstantEncoding());
      auto arrayVector = arg->wrappedVector()->asUnchecked<ArrayVector>();
      auto size = arrayVector->sizeAt(arg->wrappedIndex(rows.begin()));
      rows.applyToSelected([&](auto row) { rawValues[row] = size; });
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T) -> bigint
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("bigint")
                .argumentType("array(T)")

                .build()};
  }
};

class CardinalityBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit CardinalityBenchmark(uint32_t seed)
      : FunctionBenchmarkBase(), seed_{seed} {
    functions::prestosql::registerAllScalarFunctions();
    registerVectorFunction(
        "cardinality_v",
        CardinalityVectorFunction::signatures(),
        std::make_unique<CardinalityVectorFunction>());
  }

  RowVectorPtr generateData(bool dictionary) {
    VectorFuzzer::Options options;
    options.vectorSize = 10'024;
    options.containerVariableLength = true;
    options.containerLength = 64;

    VectorFuzzer fuzzer(options, pool(), seed_);

    if (dictionary) {
      return vectorMaker_.rowVector(
          {fuzzer.fuzzDictionary(fuzzer.fuzzFlat(ARRAY(BIGINT()))),
           fuzzer.fuzzFlat(BOOLEAN())});
    } else {
      return vectorMaker_.rowVector(
          {fuzzer.fuzzFlat(ARRAY(BIGINT())), fuzzer.fuzzFlat(BOOLEAN())});
    }
  }

  VectorPtr evaluateOnce(
      const std::string& expression,
      const RowVectorPtr& data) {
    auto exprSet = compileExpression(expression, asRowType(data->type()));
    return evaluate(exprSet, data);
  }

  void test() {
    auto flatData = generateData(false);

    auto basicResult = evaluateOnce("cardinality(c0)", flatData);
    auto vectorResult = evaluateOnce("cardinality_v(c0)", flatData);
    auto fastResult = fastCardinality(flatData->childAt(0));

    test::assertEqualVectors(basicResult, fastResult);
    test::assertEqualVectors(basicResult, vectorResult);

    auto dictData = generateData(true);

    basicResult = evaluateOnce("cardinality(c0)", dictData);
    vectorResult = evaluateOnce("cardinality_v(c0)", dictData);

    test::assertEqualVectors(basicResult, vectorResult);
  }

  void run(size_t times, const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData(false);
    auto exprSet =
        compileExpression(functionName + "(c0)", asRowType(data->type()));
    suspender.dismiss();

    doRun(exprSet, data, times);
  }

  void runDictionary(size_t times, const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData(true);
    auto exprSet =
        compileExpression(functionName + "(c0)", asRowType(data->type()));
    suspender.dismiss();

    doRun(exprSet, data, times);
  }

  void runConditional(size_t times, const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData(false);
    auto exprSet = compileExpression(
        fmt::format("if(c1, 5, {}(c0))", functionName),
        asRowType(data->type()));
    suspender.dismiss();

    doRun(exprSet, data, times);
  }

  void runFast(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData(false);
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < times; i++) {
      cnt += fastCardinality(data->childAt(0))->size();
    }
    folly::doNotOptimizeAway(cnt);
  }

 private:
  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t times) {
    int cnt = 0;
    for (auto i = 0; i < times; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }

  const uint32_t seed_;
};
} // namespace

const uint32_t seed = folly::Random::rand32();
size_t static constexpr kIterationCount = 1000;
BENCHMARK(simple) {
  CardinalityBenchmark benchmark(seed);
  benchmark.run(kIterationCount, "cardinality");
}

BENCHMARK(vector) {
  CardinalityBenchmark benchmark(seed);
  benchmark.run(kIterationCount, "cardinality_v");
}

BENCHMARK(fast) {
  CardinalityBenchmark benchmark(seed);
  benchmark.runFast(kIterationCount);
}

BENCHMARK(simpleDictionary) {
  CardinalityBenchmark benchmark(seed);
  benchmark.runDictionary(kIterationCount, "cardinality");
}

BENCHMARK(vectorDictionary) {
  CardinalityBenchmark benchmark(seed);
  benchmark.runDictionary(kIterationCount, "cardinality_v");
}

BENCHMARK(simpleConditional) {
  CardinalityBenchmark benchmark(seed);
  benchmark.runConditional(kIterationCount, "cardinality");
}

BENCHMARK(vectorConditional) {
  CardinalityBenchmark benchmark(seed);
  benchmark.runConditional(kIterationCount, "cardinality_v");
}

int main(int /*argc*/, char** /*argv*/) {
  LOG(ERROR) << "Seed: " << seed;
  {
    CardinalityBenchmark benchmark(seed);
    benchmark.test();
  }
  folly::runBenchmarks();
  return 0;
}

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

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"

// This macro should be set, defined in command line.
// #define WITH_NULLS false

// Benchmark a function that creates a matrix of size n*n with numbers 1 to
// n^2-1 for every input n, it set nulls in the diagonal when WITH_NULLS is
// true.

namespace facebook::velox::exec {
namespace {
class VectorFunctionImpl : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    LocalDecodedVector decoded_(context, *args[0], rows); // NOLINT

    // Prepare results
    auto elementType =
        ArrayType(std::make_shared<ArrayType>(ArrayType(BIGINT())));
    BaseVector::ensureWritable(
        rows, std::make_shared<ArrayType>(elementType), context.pool(), result);

    auto arrayVectorOuter = result->as<ArrayVector>();
    auto arrayVectorInner = arrayVectorOuter->elements()->as<ArrayVector>();
    auto flatElements = arrayVectorInner->elements()->as<FlatVector<int64_t>>();

    auto offsetOuter = 0;
    auto offsetInner = 0;

    rows.applyToSelected([&](vector_size_t row) {
      auto n = decoded_->valueAt<int64_t>(row);
      arrayVectorOuter->setOffsetAndSize(row, offsetOuter, n);
      auto count = 0;

      arrayVectorInner->resize(arrayVectorInner->size() + n);
      // This is an optimization that we can not do in the simple function
      // interface.
      flatElements->resize(flatElements->size() + n * n, false);
      for (auto i = 0; i < n; i++) {
        // Create n arrays of size n.
        arrayVectorInner->setOffsetAndSize(offsetOuter + i, offsetInner, n);
        for (auto j = 0; j < n; j++) {
          if (WITH_NULLS && i == j) {
            flatElements->setNull(offsetInner + j, true);
          } else {
            flatElements->set(offsetInner + j, count);
          }
          count++;
        }
        offsetInner += n;
      }
      offsetOuter += n;
    });
  }
};

template <typename T>
struct SimpleFunctionImpl {
  template <typename TOut>
  bool call(TOut& out, const int64_t& n) {
    int count = 0;
    for (auto i = 0; i < n; i++) {
      auto& matrixRow = out.add_item();
      matrixRow.resize(n);
      for (auto j = 0; j < n; j++) {
        if (WITH_NULLS && i == j) {
          matrixRow[j] = std::nullopt;
        } else {
          matrixRow[j] = count;
        }
        count++;
      }
    }
    VELOX_DCHECK(count == n * n);
    return true;
  }
};

class NestedArrayWriterBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  NestedArrayWriterBenchmark() : FunctionBenchmarkBase() {
    registerFunction<SimpleFunctionImpl, Array<Array<int64_t>>, int64_t>(
        {"array_proxy"});

    facebook::velox::exec::registerVectorFunction(
        "vector",
        {exec::FunctionSignatureBuilder()
             .returnType("array(array(bigint))")
             .argumentType("bigint")
             .build()},
        std::make_unique<VectorFunctionImpl>());
  }

  vector_size_t size = 200;
  size_t totalItemsCount = size * size - 1;

  auto makeInput() {
    std::vector<int64_t> inputData(size, 0);
    for (auto i = 0; i < size; i++) {
      inputData[i] = i;
    }

    auto input = vectorMaker_.rowVector({vectorMaker_.flatVector(inputData)});
    return input;
  }

  size_t run(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto input = makeInput();
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", functionName), input->type());
    suspender.dismiss();

    doRun(exprSet, input);
    return totalItemsCount;
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }

  bool
  hasSameResults(ExprSet& expr1, ExprSet& expr2, const RowVectorPtr& input) {
    auto result1 = evaluate(expr1, input);
    auto result2 = evaluate(expr2, input);
    if (result1->size() != result2->size()) {
      return false;
    }

    for (auto i = 0; i < result1->size(); i++) {
      if (!result1->equalValueAt(result2.get(), i, i)) {
        return false;
      }
    }
    return true;
  }

  void test() {
    auto input = makeInput();
    auto exprSetRef = compileExpression("vector(c0)", input->type());
    std::vector<std::string> functions = {"array_proxy"};

    for (const auto& name : functions) {
      auto other =
          compileExpression(fmt::format("{}(c0)", name), input->type());
      if (!hasSameResults(exprSetRef, other, input)) {
        VELOX_UNREACHABLE(fmt::format("testing failed at function {}", name));
      }
    }
  }
};

BENCHMARK_MULTI(vector) {
  NestedArrayWriterBenchmark benchmark;
  return benchmark.run("vector");
}

BENCHMARK_MULTI(simple) {
  NestedArrayWriterBenchmark benchmark;
  return benchmark.run("simple");
}

} // namespace
} // namespace facebook::velox::exec

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  facebook::velox::exec::NestedArrayWriterBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();

  return 0;
}

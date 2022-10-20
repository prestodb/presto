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

// Benchmark a function that constructs an array of size n with values 0...n.
namespace facebook::velox::exec {

namespace {

template <bool optimizeResize>
class VectorFunctionImpl : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    LocalDecodedVector decoded_(context, *args[0], rows); // NOLINT

    // Prepare results.
    BaseVector::ensureWritable(rows, ARRAY(BIGINT()), context.pool(), result);
    auto flatResult = result->as<ArrayVector>();
    auto currentOffset = 0;
    auto elementsFlat = flatResult->elements()->asFlatVector<int64_t>();

    // Compute total size needed for elements.
    auto totalSize = 0;

    if constexpr (optimizeResize) {
      // Note: this is an optimization that is special for the logic of this
      // function and not general, hence can't be done in the simple function
      // interface.
      rows.applyToSelected([&](vector_size_t row) {
        totalSize += decoded_->valueAt<int64_t>(row);
      });
      elementsFlat->resize(totalSize, false);
    }

    rows.applyToSelected([&](vector_size_t row) {
      auto length = decoded_->valueAt<int64_t>(row);

      flatResult->setOffsetAndSize(row, currentOffset, length);
      flatResult->setNull(row, false);

      if constexpr (!optimizeResize) {
        totalSize += length;
        elementsFlat->resize(totalSize, false);
      }

      for (auto i = 0; i < length; i++) {
        if (WITH_NULLS && i % 5) {
          elementsFlat->setNull(currentOffset + i, true);
        } else {
          elementsFlat->set(currentOffset + i, i);
        }
      }

      currentOffset += length;
    });
  }
};

template <typename T>
struct SimpleFunctionResize {
  bool call(exec::ArrayWriter<int64_t>& out, const int64_t& n) {
    out.resize(n);
    for (int i = 0; i < n; i++) {
      if (WITH_NULLS && i % 5) {
        out[i] = std::nullopt;
      } else {
        out[i] = i;
      }
    }
    return true;
  }
};

template <typename T>
struct SimpleFunctionPushBack {
  bool call(exec::ArrayWriter<int64_t>& out, const int64_t& n) {
    for (int i = 0; i < n; i++) {
      if (WITH_NULLS && i % 5) {
        out.push_back(std::nullopt);
      } else {
        out.push_back(i);
      }
    }
    return true;
  }
};

template <typename T>
struct SimpleGeneralInterface {
  bool call(exec::ArrayWriter<int64_t>& out, const int64_t& n) {
    for (int i = 0; i < n; i++) {
      if (WITH_NULLS && i % 5) {
        out.add_null();
      } else {
        auto& item = out.add_item();
        item = i;
      }
    }
    return true;
  }
};

class ArrayWriterBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArrayWriterBenchmark() : FunctionBenchmarkBase() {
    registerFunction<SimpleFunctionResize, Array<int64_t>, int64_t>(
        {"simpl_resize"});
    registerFunction<SimpleFunctionPushBack, Array<int64_t>, int64_t>(
        {"simple_push_back"});
    registerFunction<SimpleGeneralInterface, Array<int64_t>, int64_t>(
        {"simple_general"});

    facebook::velox::exec::registerVectorFunction(
        "vector_resize_optimized",
        {exec::FunctionSignatureBuilder()
             .returnType("array(bigint)")
             .argumentType("bigint")
             .build()},
        std::make_unique<VectorFunctionImpl<true>>());

    facebook::velox::exec::registerVectorFunction(
        "vector_basic",
        {exec::FunctionSignatureBuilder()
             .returnType("array(bigint)")
             .argumentType("bigint")
             .build()},
        std::make_unique<VectorFunctionImpl<false>>());
  }

  vector_size_t size = 1'000;
  size_t totalItemsCount = (size) * (size + 1) / 2;

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
    auto exprSetRef = compileExpression("vector_basic(c0)", input->type());
    std::vector<std::string> functions = {
        "vector_resize_optimized",
        "simple_push_back",
        "simple_resize",
        "simple_general",
    };

    for (const auto& name : functions) {
      auto other =
          compileExpression(fmt::format("{}(c0)", name), input->type());
      if (!hasSameResults(exprSetRef, other, input)) {
        VELOX_UNREACHABLE(fmt::format("testing failed at function {}", name));
      }
    }
  }

  size_t runStdRef() {
    for (auto k = 0; k < 100; k++) {
      std::vector<std::optional<std::vector<std::optional<int>>>> arrayVector;
      arrayVector.resize(1000);
      for (auto i = 0; i < 1000; i++) {
        arrayVector[i] = std::vector<std::optional<int>>();
        auto& current = *arrayVector[i];
        current.resize(i);
        for (int j = 0; j < i; j++) {
          if (WITH_NULLS && i % 5) {
            current[j] = std::nullopt;
          } else {
            current[j] = j; // NOLINT
          }
        }
      }
      folly::doNotOptimizeAway(arrayVector);
    }
    return totalItemsCount;
  }
};

BENCHMARK_MULTI(vector_basic) {
  ArrayWriterBenchmark benchmark;
  return benchmark.run("vector_basic");
}

BENCHMARK_MULTI(vector_resize_optimized) {
  ArrayWriterBenchmark benchmark;
  return benchmark.run("vector_resize_optimized");
}

BENCHMARK_MULTI(simple_resize) {
  ArrayWriterBenchmark benchmark;
  return benchmark.run("simple_resize");
}

BENCHMARK_MULTI(simple_push_back) {
  ArrayWriterBenchmark benchmark;
  return benchmark.run("simple_push_back");
}

BENCHMARK_MULTI(simple_general) {
  ArrayWriterBenchmark benchmark;
  return benchmark.run("simple_general");
}

BENCHMARK_MULTI(std_reference) {
  ArrayWriterBenchmark benchmark;
  return benchmark.runStdRef();
}
} // namespace
} // namespace facebook::velox::exec

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  facebook::velox::exec::ArrayWriterBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();
  return 0;
}

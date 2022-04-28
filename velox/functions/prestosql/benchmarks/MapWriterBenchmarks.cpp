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

// This file includes benchmarks that evaluate performance of map writer.
// It benchmark a function that constructs map<int, int> (i-> i*2).

namespace facebook::velox::exec {

namespace {

template <bool oneResize = false>
class VectorFunctionImpl : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    LocalDecodedVector decoded_(context, *args[0], rows); // NOLINT

    // Prepare results.
    BaseVector::ensureWritable(
        rows, MAP(BIGINT(), BIGINT()), context->pool(), result);

    auto flatResult = (*result)->as<MapVector>();
    auto currentOffset = 0;
    auto keysFlat = flatResult->mapKeys()->asFlatVector<int64_t>();
    auto valuesFlat = flatResult->mapValues()->asFlatVector<int64_t>();

    auto totalSize = 0;

    if constexpr (oneResize) {
      rows.applyToSelected([&](vector_size_t row) {
        totalSize += decoded_->valueAt<int64_t>(row);
      });

      keysFlat->resize(totalSize, false);
      valuesFlat->resize(totalSize, false);
    }

    rows.applyToSelected([&](vector_size_t row) {
      auto length = decoded_->valueAt<int64_t>(row);

      flatResult->setNull(row, false);
      flatResult->setOffsetAndSize(row, currentOffset, length);

      if constexpr (!oneResize) {
        totalSize += length;
        keysFlat->resize(totalSize, false);
        valuesFlat->resize(totalSize, false);
      }

      for (auto i = 0; i < length; i++) {
        keysFlat->set(currentOffset + i, i);
        if (WITH_NULLS && i % 5) {
          valuesFlat->setNull(currentOffset + i, true);
        } else {
          valuesFlat->set(currentOffset + i, i * 2);
        }
      }

      currentOffset += length;
    });
  }
};

template <typename T>
struct SimpleGeneral {
  template <typename OutT>
  bool call(OutT& out, const int64_t& n) {
    for (int i = 0; i < n; i++) {
      if (WITH_NULLS && i % 5) {
        out.add_null() = i;
      } else {
        out.add_item() = std::make_tuple(i, i * 2);
      }
    }
    return true;
  }
};

template <typename T>
struct SimpleEmplace {
  template <typename OutT>
  bool call(OutT& out, const int64_t& n) {
    for (int i = 0; i < n; i++) {
      if (WITH_NULLS && i % 5) {
        out.emplace(i, std::nullopt);
      } else {
        out.emplace(i, i * 2);
      }
    }
    return true;
  }
};

template <typename T>
struct SimpleResizeSubscript {
  template <typename OutT>
  bool call(OutT& out, const int64_t& n) {
    out.resize(n);
    for (int i = 0; i < n; i++) {
      if (WITH_NULLS && i % 5) {
        out[i] = std::make_tuple(i, std::nullopt);
      } else {
        out[i] = std::make_tuple(i, i * 2);
      }
    }
    return true;
  }
};

class MapWriterBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  MapWriterBenchmark() : FunctionBenchmarkBase() {
    registerFunction<SimpleResizeSubscript, Map<int64_t, int64_t>, int64_t>(
        {"simple_resize_subscript"});
    registerFunction<SimpleGeneral, Map<int64_t, int64_t>, int64_t>(
        {"simple_general"});
    registerFunction<SimpleEmplace, Map<int64_t, int64_t>, int64_t>(
        {"simple_emplace"});

    facebook::velox::exec::registerVectorFunction(
        "vector_resize_per_row",
        {exec::FunctionSignatureBuilder()
             .returnType("map(bigint, bigint)")
             .argumentType("bigint")
             .build()},
        std::make_unique<VectorFunctionImpl<false>>());

    facebook::velox::exec::registerVectorFunction(
        "vector_one_resize",
        {exec::FunctionSignatureBuilder()
             .returnType("map(bigint, bigint)")
             .argumentType("bigint")
             .build()},
        std::make_unique<VectorFunctionImpl<true>>());
  }

  vector_size_t size = 1'000;

  auto makeInput() {
    std::vector<int64_t> inputData(size, 0);
    for (auto i = 0; i < size; i++) {
      inputData[i] = i;
    }

    auto input = vectorMaker_.rowVector({vectorMaker_.flatVector(inputData)});
    return input;
  }

  void run(const std::string& functionName, size_t n) {
    folly::BenchmarkSuspender suspender;
    auto input = makeInput();
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", functionName), input->type());
    suspender.dismiss();

    doRun(exprSet, input, n);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t n) {
    int cnt = 0;
    for (auto i = 0; i < n; i++) {
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
    auto exprSetRef =
        compileExpression("vector_resize_per_row(c0)", input->type());
    std::vector<std::string> functions = {
        "simple_resize_subscript",
        "simple_general",
        "simple_emplace",
    };

    for (const auto& name : functions) {
      auto other =
          compileExpression(fmt::format("{}(c0)", name), input->type());
      if (!hasSameResults(exprSetRef, other, input)) {
        VELOX_UNREACHABLE(fmt::format("testing failed at function {}", name));
      }
    }
  }
};

BENCHMARK(vector_one_resize, n) {
  MapWriterBenchmark benchmark;
  return benchmark.run("vector_one_resize", n);
}

BENCHMARK(vector_resize_per_row, n) {
  MapWriterBenchmark benchmark;
  return benchmark.run("vector_resize_per_row", n);
}

BENCHMARK(simple_resize_subscript, n) {
  MapWriterBenchmark benchmark;
  return benchmark.run("simple_resize_subscript", n);
}

BENCHMARK(simple_emplace, n) {
  MapWriterBenchmark benchmark;
  return benchmark.run("simple_emplace", n);
}

BENCHMARK(simple_general, n) {
  MapWriterBenchmark benchmark;
  return benchmark.run("simple_general", n);
}

} // namespace
} // namespace facebook::velox::exec

int main(int /*argc*/, char** /*argv*/) {
  facebook::velox::exec::MapWriterBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();
  return 0;
}

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
#include <tuple>

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"

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

    // Prepare results.
    BaseVector::ensureWritable(
        rows, ROW({BIGINT(), BIGINT()}), context.pool(), result);

    auto rowVector = result->as<RowVector>();
    rowVector->childAt(0)->resize(rows.size());
    rowVector->childAt(1)->resize(rows.size());
    auto flat1 = rowVector->childAt(0)->asFlatVector<int64_t>();
    auto flat2 = rowVector->childAt(1)->asFlatVector<int64_t>();

    rows.applyToSelected([&](vector_size_t row) {
      rowVector->setNull(row, false);
      auto n = decoded_->valueAt<int64_t>(row);
      flat1->set(row, n);
      flat2->set(row, n + 1);
    });
  }
};

template <typename T>
struct RowOfPairGeneralFunc {
  template <typename OutT>
  void call(OutT& out, const int64_t& n) {
    out.template get_writer_at<0>() = n;
    out.template get_writer_at<1>() = n + 1;
  }
};

template <typename T>
struct RowOfPairTupleAssignFunc {
  template <typename OutT>
  void call(OutT& out, const int64_t& n) {
    out = std::make_tuple(n, n + 1);
  }
};

template <typename T>
struct ArrayOfRowsFunc {
  template <typename OutT>
  void call(OutT& out, const int64_t& n) {
    for (int i = 0; i < 20; i++) {
      auto& row = out.add_item();
      row.template get_writer_at<0>() = n;
      row.template get_writer_at<1>() = n + 1;
    }
  }
};

template <typename T>
struct ComplexRowFunc {
  template <typename OutT>
  void call(OutT& out, const int64_t& n) {
    auto& array = out.template get_writer_at<0>();
    for (auto i = 0; i < n % 100; i++) {
      array.push_back(i);
    }
    out.template get_writer_at<1>() = n;

    out.template get_writer_at<2>().append("aaa");
    out.template get_writer_at<2>().append(std::to_string(n));

    auto& map = out.template get_writer_at<3>();
    for (auto i = 0; i < n % 100; i++) {
      map.emplace(i, i * 100);
    }
  }
};

class RowWriterBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  RowWriterBenchmark() : FunctionBenchmarkBase() {
    registerFunction<RowOfPairGeneralFunc, Row<int64_t, int64_t>, int64_t>(
        {"basic_row_simple_general"});
    registerFunction<RowOfPairTupleAssignFunc, Row<int64_t, int64_t>, int64_t>(
        {"basic_row_simple_tuple_assign"});

    registerFunction<ArrayOfRowsFunc, Array<Row<int64_t, int64_t>>, int64_t>(
        {"array_of_rows"});

    registerFunction<
        ComplexRowFunc,
        Row<Array<int64_t>, int64_t, Varchar, Map<int64_t, int64_t>>,
        int64_t>({"complex_row"});

    facebook::velox::exec::registerVectorFunction(
        "basic_row_vector",
        {exec::FunctionSignatureBuilder()
             .returnType("row(bigint, bigint)")
             .argumentType("bigint")
             .build()},
        std::make_unique<VectorFunctionImpl>());
  }

  vector_size_t size = 1000;

  auto makeInput() {
    std::vector<int64_t> inputData(size, 0);
    for (auto i = 0; i < size; i++) {
      inputData[i] = i;
    }

    auto input = vectorMaker_.rowVector({vectorMaker_.flatVector(inputData)});
    return input;
  }

  size_t run(const std::string& functionName, size_t n) {
    folly::BenchmarkSuspender suspender;
    auto input = makeInput();
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", functionName), input->type());
    suspender.dismiss();
    return doRun(exprSet, input, n);
  }

  size_t doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t n) {
    int cnt = 0;
    for (auto i = 0; i < n; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
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
    auto exprSetRef = compileExpression("basic_row_vector(c0)", input->type());
    std::vector<std::string> functions = {
        "basic_row_simple_tuple_assign",
        "basic_row_simple_general",
    };

    for (const auto& name : functions) {
      auto other =
          compileExpression(fmt::format("{}(c0)", name), input->type());
      VELOX_CHECK(
          hasSameResults(exprSetRef, other, input),
          fmt::format("testing failed at function {}", name));
    }
  }
};

BENCHMARK_MULTI(basic_row_vector) {
  RowWriterBenchmark benchmark;
  return benchmark.run("basic_row_vector", 100);
}

BENCHMARK_MULTI(basic_row_simple_tuple_assign) {
  RowWriterBenchmark benchmark;
  return benchmark.run("basic_row_simple_tuple_assign", 100);
}

BENCHMARK_MULTI(basic_row_simple_general) {
  RowWriterBenchmark benchmark;
  return benchmark.run("basic_row_simple_general", 100);
}

BENCHMARK_MULTI(array_of_rows) {
  RowWriterBenchmark benchmark;
  return benchmark.run("array_of_rows", 100);
}

BENCHMARK_MULTI(complex_row) {
  RowWriterBenchmark benchmark;
  return benchmark.run("complex_row", 100);
}

} // namespace
} // namespace facebook::velox::exec

int main(int /*argc*/, char** /*argv*/) {
  facebook::velox::exec::RowWriterBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();
  return 0;
}

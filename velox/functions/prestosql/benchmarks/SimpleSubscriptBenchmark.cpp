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
#include <memory>

#include "velox/functions/Registerer.h"
#include "velox/functions/Udf.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

size_t static constexpr kIterationCount = 100;
DEFINE_int32(vector_size, 1000, "vector size");
DEFINE_double(null_ratio, 0.2, "null ratio");
DEFINE_int32(container_length, 10, "container length");

namespace facebook::velox {
namespace {

void checkIndex(int64_t index, int64_t arraySize) {
  if (UNLIKELY(index <= 0)) {
    if (index == 0) {
      VELOX_USER_FAIL("SQL array indices start at 1");

    } else {
      VELOX_USER_FAIL("Array subscript is negative.");
    }
  }

  if (UNLIKELY(index - 1 >= arraySize)) {
    VELOX_USER_FAIL("Array subscript out of bounds.");
  }
}

template <typename T>
struct ArraySubscriptSimpleFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Fast path for primitives.
  template <typename Out, typename In>
  bool call(Out& out, const In& input, int64_t index) {
    static_assert(std::is_fundamental_v<Out>);
    checkIndex(index, input.size());
    if (!input[index - 1].has_value()) {
      return false;
    }
    out = input[index - 1].value();
    return true;
  }

  // Generic implementation.
  bool call(
      out_type<Generic<T1>>& out,
      const arg_type<Array<Generic<T1>>>& input,
      int64_t index) {
    checkIndex(index, input.size());
    if (!input[index - 1].has_value()) {
      return false;
    }
    out.copy_from(input[index - 1].value());
    return true;
  }
};

// This version have reuse_strings_from_arg enabled.
template <typename T>
struct ArraySubscriptSimpleFuncString {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  // String version that avoids copy of strings.
  bool call(
      out_type<Varchar>& out,
      const arg_type<Array<Varchar>>& input,
      int64_t index) {
    checkIndex(index, input.size());
    if (!input[index - 1].has_value()) {
      return false;
    }
    out.setNoCopy(input[index - 1].value());
    return true;
  }
};

class SimpleSubscriptBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit SimpleSubscriptBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerGeneralFunctions();
    // The general version for complex nested types.
    facebook::velox::registerFunction<
        ArraySubscriptSimpleFunc,
        Generic<T1>,
        Array<Generic<T1>>,
        int64_t>({"subscript_simple"});

    // Fast path for primitives.
    facebook::velox::registerFunction<
        ArraySubscriptSimpleFunc,
        int64_t,
        Array<int64_t>,
        int64_t>({"subscript_simple"});
    facebook::velox::registerFunction<
        ArraySubscriptSimpleFunc,
        double,
        Array<double>,
        int64_t>({"subscript_simple"});
    facebook::velox::registerFunction<
        ArraySubscriptSimpleFuncString,
        Varchar,
        Array<Varchar>,
        int64_t>({"subscript_simple"});

    opts_.vectorSize = FLAGS_vector_size;
    opts_.nullRatio = FLAGS_null_ratio;
    opts_.containerLength = FLAGS_container_length;
    opts_.complexElementsMaxSize = 1000000000; // 1GB.
    // We have it false to make sure that we actually generate a valid
    // subscript.
    opts_.containerVariableLength = false;
    for (int i = 0; i < FLAGS_vector_size; i++) {
      indicesData_.push_back((i % FLAGS_container_length) + 1);
    }
  }

  RowVectorPtr generateData(const TypePtr& type) {
    VectorFuzzer fuzzer(opts_, pool());
    auto input = fuzzer.fuzzFlat(ARRAY(type));
    auto indicesVector = vectorMaker_.flatVector<int64_t>(indicesData_);
    return vectorMaker_.rowVector({input, indicesVector});
  }

  void run(
      const std::string& query,
      const TypePtr& type,
      size_t times = kIterationCount) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData(type);
    auto exprSet = compileExpression(query, asRowType(data->type()));
    suspender.dismiss();

    doRun(exprSet, data, times);
  }

  void test() {
    testArraySubscript(INTEGER());
    testArraySubscript(BIGINT());
    testArraySubscript(VARCHAR());
    testArraySubscript(ARRAY(VARCHAR()));
    testArraySubscript(ARRAY(INTEGER()));
    testArraySubscript(ARRAY(ARRAY(INTEGER())));
    testArraySubscript(MAP(INTEGER(), INTEGER()));
    testArraySubscript(MAP(VARCHAR(), INTEGER()));
    testArraySubscript(MAP(INTEGER(), ARRAY(BIGINT())));
    testArraySubscript(MAP(ARRAY(INTEGER()), BIGINT()));
    testArraySubscript(ROW({INTEGER()}));
    testArraySubscript(ROW({INTEGER(), ARRAY(INTEGER())}));
    testArraySubscript(ROW({MAP(INTEGER(), INTEGER()), ARRAY(INTEGER())}));
    testArraySubscript(ARRAY(ROW({INTEGER(), ARRAY(INTEGER())})));
  }

 private:
  void
  doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector, size_t times) {
    int cnt = 0;
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, rowVector.get());
    SelectivityVector rows(rowVector->size());
    std::vector<VectorPtr> results(1);
    for (auto i = 0; i < times; i++) {
      exprSet.eval(rows, evalCtx, results);
      BaseVector::flattenVector(results[0]);
      cnt += results[0]->size();
      results[0]->prepareForReuse();
    }
    folly::doNotOptimizeAway(cnt);
  }

  void testArraySubscript(const TypePtr& type) {
    auto data = generateData(type);
    auto exprSet1 =
        compileExpression("subscript(c0, c1)", asRowType(data->type()));
    auto exprSet2 =
        compileExpression("subscript_simple(c0, c1)", asRowType(data->type()));
    SelectivityVector rows(data->size());

    VectorPtr result1, result2;
    {
      std::vector<VectorPtr> results(1);
      exec::EvalCtx evalCtx(&execCtx_, &exprSet1, data.get());
      exprSet1.eval(rows, evalCtx, results);
      result1 = results[0];
    }

    {
      std::vector<VectorPtr> results(1);
      exec::EvalCtx evalCtx(&execCtx_, &exprSet2, data.get());
      exprSet2.eval(rows, evalCtx, results);
      result2 = results[0];
    }

    test::assertEqualVectors(result1, result2);
  }

  std::vector<int64_t> indicesData_;
  VectorFuzzer::Options opts_;
};

} // namespace
} // namespace facebook::velox

using namespace facebook::velox;

std::unique_ptr<SimpleSubscriptBenchmark> benchmark;

BENCHMARK(ArraySubscript_ArrayInt) {
  benchmark->run("subscript(c0, c1)", BIGINT());
}

BENCHMARK(ArraySubscriptSimple_ArrayInt) {
  benchmark->run("subscript_simple(c0, c1)", BIGINT());
}

BENCHMARK(ArraySubscriptSimple_NoFastPath_ArrayInt) {
  benchmark->run("subscript_simple(c0, c1)", INTEGER());
}

BENCHMARK(ArraySubscript_NestedArrayInt) {
  benchmark->run("subscript(c0, c1)", ARRAY(BIGINT()));
}

BENCHMARK(ArraySubscriptSimple_NestedArrayInt) {
  benchmark->run("subscript_simple(c0, c1)", ARRAY(BIGINT()));
}

BENCHMARK(ArraySubscript_TripleNestedArrayInt) {
  benchmark->run("subscript(c0, c1)", ARRAY(ARRAY(BIGINT())));
}

BENCHMARK(ArraySubscriptSimple_TripleNestedArrayInt) {
  benchmark->run("subscript_simple(c0, c1)", ARRAY(ARRAY(BIGINT())));
}

BENCHMARK(ArraySubscript_ArrayString) {
  benchmark->run("subscript(c0, c1)", VARCHAR());
}

BENCHMARK(ArraySubscriptSimple_ArrayString) {
  benchmark->run("subscript_simple(c0, c1)", VARCHAR());
}

BENCHMARK(ArraySubscript_NestedArrayString) {
  benchmark->run("subscript(c0, c1)", ARRAY(VARCHAR()));
}

BENCHMARK(ArraySubscriptSimple_NestedArrayString) {
  benchmark->run("subscript_simple(c0, c1)", ARRAY(VARCHAR()));
}

BENCHMARK(ArraySubscriptSimple_MapIntInt) {
  benchmark->run("subscript_simple(c0, c1)", MAP(BIGINT(), BIGINT()));
}

BENCHMARK(ArraySubscript_MapIntInt) {
  benchmark->run("subscript(c0, c1)", MAP(BIGINT(), BIGINT()));
}

BENCHMARK(ArraySubscriptSimple_MapStringInt) {
  benchmark->run("subscript_simple(c0, c1)", MAP(VARCHAR(), BIGINT()));
}

BENCHMARK(ArraySubscript_MapStringInt) {
  benchmark->run("subscript(c0, c1)", MAP(VARCHAR(), BIGINT()));
}

BENCHMARK(ArraySubscriptSimple_MapIntArray) {
  benchmark->run("subscript_simple(c0, c1)", MAP(BIGINT(), ARRAY(BIGINT())));
}

BENCHMARK(ArraySubscript_MapIntArray) {
  benchmark->run("subscript(c0, c1)", MAP(BIGINT(), ARRAY(BIGINT())));
}

BENCHMARK(ArraySubscriptSimple_MapArrayInt) {
  benchmark->run("subscript_simple(c0, c1)", MAP(ARRAY(BIGINT()), BIGINT()));
}

BENCHMARK(ArraySubscript_MapArrayInt) {
  benchmark->run("subscript(c0, c1)", MAP(ARRAY(BIGINT()), BIGINT()));
}

BENCHMARK(ArraySubscriptSimple_RowInt) {
  benchmark->run("subscript_simple(c0, c1)", ROW({INTEGER()}));
}

BENCHMARK(ArraySubscript_RowInt) {
  benchmark->run("subscript(c0, c1)", ROW({INTEGER()}));
}

BENCHMARK(ArraySubscriptSimple_RowIntArray) {
  benchmark->run(
      "subscript_simple(c0, c1)", ROW({INTEGER(), ARRAY(INTEGER())}));
}

BENCHMARK(ArraySubscript_RowIntArray) {
  benchmark->run("subscript(c0, c1)", ROW({INTEGER(), ARRAY(INTEGER())}));
}

BENCHMARK(ArraySubscriptSimple_RowMapArray) {
  benchmark->run(
      "subscript_simple(c0, c1)",
      ROW({MAP(INTEGER(), INTEGER()), ARRAY(INTEGER())}));
}

BENCHMARK(ArraySubscript_RowMapArray) {
  benchmark->run(
      "subscript(c0, c1)", ROW({MAP(INTEGER(), INTEGER()), ARRAY(INTEGER())}));
}

BENCHMARK(ArraySubscriptSimple_ArrayRowIntArray) {
  benchmark->run(
      "subscript_simple(c0, c1)", ARRAY(ROW({INTEGER(), ARRAY(INTEGER())})));
}

BENCHMARK(ArraySubscript_ArrayRowIntArray) {
  benchmark->run(
      "subscript(c0, c1)", ARRAY(ROW({INTEGER(), ARRAY(INTEGER())})));
}

BENCHMARK(ArraySubscriptSimple_ArrayRowIntInt) {
  benchmark->run(
      "subscript_simple(c0, c1)", ARRAY(ROW({INTEGER(), INTEGER()})));
}

BENCHMARK(ArraySubscript_ArrayRowIntInt) {
  benchmark->run("subscript(c0, c1)", ARRAY(ROW({INTEGER(), INTEGER()})));
}

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  benchmark = std::make_unique<SimpleSubscriptBenchmark>();
  benchmark->test();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}

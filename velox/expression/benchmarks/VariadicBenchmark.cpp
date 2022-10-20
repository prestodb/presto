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

#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// Benchmark a function that concats its string inputs.

namespace facebook::velox::functions {

template <typename T>
struct StringConcatSimple {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& out,
      const arg_type<Varchar>& str1,
      const arg_type<Varchar>& str2,
      const arg_type<Varchar>& str3,
      const arg_type<Varchar>& str4,
      const arg_type<Varchar>& str5) {
    stringImpl::concatStatic(out, str1, str2, str3, str4, str5);

    return true;
  }
};

template <typename T>
struct StringConcatVariadic {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& out,
      const arg_type<Variadic<Varchar>>& strings) {
    for (const auto& string : strings) {
      out += string.value();
    }

    return true;
  }
};

void registerVectorFunction() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, "concat_vector");
}

void registerSimpleFunctions() {
  registerFunction<
      StringConcatSimple,
      Varchar,
      Varchar,
      Varchar,
      Varchar,
      Varchar,
      Varchar>({"concat_simple"});

  registerFunction<StringConcatVariadic, Varchar, Variadic<Varchar>>(
      {"concat_simple_variadic"});
}

namespace {

class VariadicBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  VariadicBenchmark() : FunctionBenchmarkBase() {
    registerVectorFunction();
    registerSimpleFunctions();
  }

  RowVectorPtr makeData(vector_size_t numArgs) {
    const vector_size_t size = 1'000;

    std::vector<VectorPtr> args;

    for (vector_size_t i = 0; i < numArgs; ++i) {
      args.emplace_back(
          vectorMaker_.flatVector<StringView>(size, [](vector_size_t row) {
            return StringView(fmt::format("{}", row));
          }));
    }

    return vectorMaker_.rowVector(args);
  }

  size_t run(const std::string& functionName, vector_size_t numArgs = 5) {
    folly::BenchmarkSuspender suspender;
    auto rowVector = makeData(numArgs);

    std::string args = "";
    for (vector_size_t i = 0; i < numArgs; ++i) {
      if (i > 0) {
        args += ", ";
      }

      args += fmt::format("c{}", i);
    }

    auto exprSet = compileExpression(
        fmt::format("{}({})", functionName, args), rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

  bool hasSameResults(
      exec::ExprSet& expr1,
      exec::ExprSet& expr2,
      const RowVectorPtr& input) {
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
    auto input = makeData(5);
    auto exprSetRef =
        compileExpression("concat_vector(c0, c1, c2, c3, c4)", input->type());
    std::vector<std::string> functions = {
        "concat_simple",
        "concat_simple_variadic",
    };

    for (const auto& name : functions) {
      auto other = compileExpression(
          fmt::format("{}(c0, c1, c2, c3, c4)", name), input->type());
      if (!hasSameResults(exprSetRef, other, input)) {
        VELOX_UNREACHABLE(fmt::format("testing failed at function {}", name));
      }
    }
  }
};

BENCHMARK_MULTI(simple) {
  VariadicBenchmark benchmark;
  return benchmark.run("concat_simple");
}

BENCHMARK_MULTI(vectorVariadic) {
  VariadicBenchmark benchmark;
  return benchmark.run("concat_vector");
}

BENCHMARK_MULTI(simpleVariadic) {
  VariadicBenchmark benchmark;
  return benchmark.run("concat_simple_variadic");
}

BENCHMARK_MULTI(vectorVariadicLotsOfArgs) {
  VariadicBenchmark benchmark;
  return benchmark.run("concat_vector", 100);
}

BENCHMARK_MULTI(simpleVariadicLotsOfArgs) {
  VariadicBenchmark benchmark;
  return benchmark.run("concat_simple_variadic", 100);
}
} // namespace
} // namespace facebook::velox::functions

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  facebook::velox::functions::VariadicBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();
  return 0;
}

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

#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace {
using namespace facebook::velox;

template <typename T>
struct SimpleFunctionTwoArgs {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(
      bool& out,
      const arg_type<Generic<T1>>& /*lhs*/,
      const arg_type<Generic<T1>>& /*rhs*/) {
    // Custom simple function that will always return true.
    out = true;
  }
};

// Vector function that returns false for everything.
class VectorFunctionTwoArgs : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& /*args*/,
      const TypePtr& /*outputType*/,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    BaseVector::ensureWritable(rows, BOOLEAN(), context->pool(), result);
    auto resultVector = (*result)->asUnchecked<FlatVector<bool>>();

    rows.applyToSelected([&](auto row) { resultVector->set(row, false); });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("boolean")
                .argumentType("bigint")
                .argumentType("bigint")
                .build()};
  }
};

class VectorFunctionOneArg : public VectorFunctionTwoArgs {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& ptr,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    VectorFunctionTwoArgs::apply(rows, args, ptr, context, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("boolean")
                .argumentType("bigint")
                .build()};
  }
};
} // namespace

namespace facebook::velox::functions {

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_function_two_args,
    VectorFunctionTwoArgs::signatures(),
    std::make_unique<VectorFunctionTwoArgs>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_function_one_arg,
    VectorFunctionOneArg::signatures(),
    std::make_unique<VectorFunctionOneArg>());

void registerVectorFunctions() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_function_two_args, "my_function");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_vector_function_one_arg, "my_function_one_arg");

  registerFunction<SimpleFunctionTwoArgs, bool, Generic<T1>, Generic<T1>>(
      {"my_function"});
  registerFunction<SimpleFunctionTwoArgs, bool, Generic<T1>, Generic<T1>>(
      {"my_function_one_arg"});
}
} // namespace facebook::velox::functions

namespace facebook::velox::exec {
namespace {

class FunctionResolutionTest : public functions::test::FunctionBaseTest {
 protected:
  void checkResults(const std::string& func, int32_t expected) {
    auto results = evaluateOnce<int32_t, int32_t, int32_t>(
        fmt::format("{}(c0, c1)", func), 1, 2);
    ASSERT_EQ(results.value(), expected);
  }
};

// Several `call` functions all that accepts two integers. With different
// runtime representations.
template <typename T>
struct TestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(int32_t& out, int32_t, int32_t) {
    out = 1;
    return true;
  }

  bool call(int32_t& out, const arg_type<Variadic<int32_t>>&) {
    out = 2;
    return true;
  }

  bool call(
      int32_t& out,
      const arg_type<Generic<T1>>&,
      const arg_type<Generic<T1>>&) {
    out = 3;
    return true;
  }

  bool call(int32_t& out, const arg_type<Variadic<Any>>&) {
    out = 4;
    return true;
  }

  bool call(int32_t& out, const int32_t&, const arg_type<Variadic<Any>>&) {
    out = 5;
    return true;
  }
};

TEST_F(FunctionResolutionTest, rank1Picked) {
  registerFunction<TestFunction, int32_t, int32_t, int32_t>({"f1"});
  registerFunction<TestFunction, int32_t, Variadic<int32_t>>({"f1"});
  registerFunction<TestFunction, int32_t, Generic<T1>, Generic<T1>>({"f1"});
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f1"});

  checkResults("f1", 1);
}

TEST_F(FunctionResolutionTest, rank2Picked) {
  registerFunction<TestFunction, int32_t, Variadic<int32_t>>({"f2"});
  registerFunction<TestFunction, int32_t, Generic<T1>, Generic<T1>>({"f2"});
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f2"});
  checkResults("f2", 2);
}

TEST_F(FunctionResolutionTest, rank3Picked) {
  registerFunction<TestFunction, int32_t, Generic<T1>, Generic<T1>>({"f3"});
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f3"});
  checkResults("f3", 3);
}

TEST_F(FunctionResolutionTest, rank4Picked) {
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f4"});
  checkResults("f4", 4);
}

// Test when two functions have the same rank.
TEST_F(FunctionResolutionTest, sameRank) {
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f5"});
  registerFunction<TestFunction, int32_t, int32_t, Variadic<Any>>({"f5"});
  checkResults("f5", 5);
}

TEST_F(FunctionResolutionTest, vectorOverSimpleFunction) {
  functions::registerVectorFunctions();

  auto rowVector = makeRowVector(
      {"a", "b"},
      {
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<int64_t>({1, 2, 3}),
      });

  auto evalResult = evaluate("my_function(a, b)", rowVector);
  // If vector function take priority then results will be all false.
  test::assertEqualVectors(
      makeFlatVector<bool>({false, false, false}), evalResult);

  evalResult = evaluate("my_function_one_arg(a, b)", rowVector);
  // In this case, the simple function should win over vector function
  test::assertEqualVectors(
      makeFlatVector<bool>({true, true, true}), evalResult);
}

} // namespace
} // namespace facebook::velox::exec

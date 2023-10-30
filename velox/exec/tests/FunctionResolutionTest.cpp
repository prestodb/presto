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

#include <optional>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

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
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BaseVector::ensureWritable(rows, BOOLEAN(), context.pool(), result);
    auto resultVector = result->asUnchecked<FlatVector<bool>>();

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
      exec::EvalCtx& context,
      VectorPtr& result) const override {
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

// Return false always.
template <typename T>
struct Func1 {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  // If input is Array<float> out is double.
  bool call(double&, const arg_type<Array<float>>&) {
    return false;
  }

  // If input is Array<x> out is x.
  bool call(out_type<Generic<T1>>&, const arg_type<Array<Generic<T1>>>&) {
    return false;
  }

  // If input is Map<int32_t, int32_t> output is int64_t.
  bool call(int64_t&, const arg_type<Map<int32_t, int32_t>>&) {
    return false;
  }

  // If input is Map<x,y> out is Row(x,y).
  bool call(
      out_type<Row<Generic<T1>, Generic<T2>>>&,
      const arg_type<Map<Generic<T1>, Generic<T2>>>&) {
    return false;
  }
};

TEST_F(FunctionResolutionTest, testGenericOutputTypeResolution) {
  registerFunction<Func1, Generic<T1>, Array<Generic<T1>>>(
      {"test_generic_out"});
  registerFunction<Func1, double, Array<float>>({"test_generic_out"});
  registerFunction<
      Func1,
      Row<Generic<T1>, Generic<T2>>,
      Map<Generic<T1>, Generic<T2>>>({"test_generic_out"});
  registerFunction<Func1, int64_t, Map<int32_t, int32_t>>({"test_generic_out"});

  auto test = [&](const TypePtr& expected, const TypePtr& inputType) {
    auto type = exec::simpleFunctions()
                    .resolveFunction("test_generic_out", {inputType})
                    ->type();
    EXPECT_TRUE(type->equivalent(*expected));
  };

  // When input is flaot output is double.
  test(DOUBLE(), ARRAY(REAL()));

  // Output is array element type.
  test(DOUBLE(), ARRAY(DOUBLE()));
  test(BIGINT(), ARRAY(BIGINT()));

  // Output is int64_t when input is Map<int32_t, int32_t>.
  test(BIGINT(), MAP(INTEGER(), INTEGER()));

  // Output is map values.
  test(ROW({INTEGER(), DOUBLE()}), MAP(INTEGER(), DOUBLE()));
  test(ROW({INTEGER(), BIGINT()}), MAP(INTEGER(), BIGINT()));
  test(ROW({INTEGER(), ARRAY(REAL())}), MAP(INTEGER(), ARRAY(REAL())));
}

template <typename T>
struct FuncHyperLogLog {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  bool call(out_type<HyperLogLog>&) {
    return false;
  }
};

TEST_F(FunctionResolutionTest, resolveCustomTypeHyperLogLog) {
  registerFunction<FuncHyperLogLog, HyperLogLog>({"f_hyper_log_log"});

  auto type =
      exec::simpleFunctions().resolveFunction("f_hyper_log_log", {})->type();
  EXPECT_EQ(type->toString(), HYPERLOGLOG()->toString());
}

template <typename T>
struct FuncJson {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  bool call(out_type<Json>&) {
    return false;
  }
};

TEST_F(FunctionResolutionTest, resolveCustomTypeJson) {
  registerFunction<FuncJson, Json>({"f_json"});

  auto type = exec::simpleFunctions().resolveFunction("f_json", {})->type();
  EXPECT_EQ(type->toString(), JSON()->toString());
}

template <typename T>
struct FuncTimestampWithTimeZone {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  bool call(out_type<TimestampWithTimezone>&) {
    return false;
  }
};

TEST_F(FunctionResolutionTest, resolveCustomTypeTimestampWithTimeZone) {
  registerFunction<FuncTimestampWithTimeZone, TimestampWithTimezone>(
      {"f_timestampzone"});

  auto type =
      exec::simpleFunctions().resolveFunction("f_timestampzone", {})->type();
  EXPECT_EQ(type->toString(), TIMESTAMP_WITH_TIME_ZONE()->toString());
}

// A function that takes TInput and returns int, TInput determined at
// registration.
template <typename T>
struct SingleInputFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  template <typename TInput>
  void call(int64_t& out, const TInput&) {
    out = 1;
  }
};

// A function that takes two inputs of types TInput1, TInput2 and return int,
// input types determined at registration.
template <typename T>
struct TwoInputFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  template <typename TInput1, typename TInput2>
  void call(int64_t& out, const TInput1&, const TInput2&) {
    out = 1;
  }
};

TEST_F(FunctionResolutionTest, constrainedGenerics) {
  registerFunction<SingleInputFunc, int64_t, Orderable<T1>>({"orderable"});
  registerFunction<SingleInputFunc, int64_t, Comparable<T1>>({"comparable"});
  registerFunction<SingleInputFunc, int64_t, Generic<T1>>({"generic"});

  auto& registry = exec::simpleFunctions();

  auto testSingleInput = [&](const auto& inputType) {
    // Test orderable.
    {
      auto resolved = registry.resolveFunction("orderable", {inputType});
      if (inputType->isOrderable()) {
        EXPECT_EQ(TypeKind::BIGINT, resolved->type()->kind());
      } else {
        EXPECT_EQ(std::nullopt, resolved);
      }
    }

    // Test comparable.
    {
      auto resolved = registry.resolveFunction("comparable", {inputType});

      if (inputType->isComparable()) {
        EXPECT_EQ(TypeKind::BIGINT, resolved->type()->kind());
      } else {
        EXPECT_EQ(std::nullopt, resolved);
      }
    }

    // Test generic.
    EXPECT_EQ(
        TypeKind::BIGINT,
        registry.resolveFunction("generic", {inputType})->type()->kind());
  };

  auto functionType = std::make_shared<FunctionType>(
      std::vector<TypePtr>{BIGINT(), VARCHAR()}, BOOLEAN());

  testSingleInput(INTEGER());
  testSingleInput(functionType);
  testSingleInput(ARRAY(INTEGER()));
  testSingleInput(MAP(INTEGER(), INTEGER()));
  testSingleInput(ROW({INTEGER(), MAP(INTEGER(), INTEGER())}));
  testSingleInput(ARRAY(ARRAY(INTEGER())));

  // Make sure we can't assign different properties to the same variable.
  VELOX_ASSERT_THROW(
      (registerFunction<TwoInputFunc, int64_t, Generic<T1>, Comparable<T1>>(
          {"generic"})),
      "Cant assign different properties to the same variable __user_T1");
  VELOX_ASSERT_THROW(
      (registerFunction<TwoInputFunc, int64_t, Orderable<T1>, Comparable<T1>>(
          {"generic"})),
      "Cant assign different properties to the same variable __user_T1");

  // Expect two args to be the same and comprable.
  registerFunction<TwoInputFunc, int64_t, Comparable<T1>, Comparable<T1>>(
      {"same_comparable"});

  EXPECT_EQ(
      TypeKind::BIGINT,
      registry.resolveFunction("same_comparable", {BIGINT(), BIGINT()})
          ->type()
          ->kind());

  EXPECT_EQ(
      std::nullopt,
      registry.resolveFunction(
          "same_comparable",
          {MAP(INTEGER(), functionType), MAP(INTEGER(), functionType)}));
  EXPECT_EQ(
      std::nullopt,
      registry.resolveFunction("same_comparable", {BIGINT(), DOUBLE()}));

  // Orderable nested in array.
  registerFunction<SingleInputFunc, int64_t, Array<Orderable<T1>>>(
      {"nested_orderable"});

  EXPECT_EQ(
      std::nullopt,
      registry.resolveFunction(
          "nested_orderable", {ARRAY(MAP(BIGINT(), BIGINT()))}));
  EXPECT_EQ(
      std::nullopt, registry.resolveFunction("nested_orderable", {INTEGER()}));
  EXPECT_EQ(
      TypeKind::BIGINT,
      registry.resolveFunction("nested_orderable", {ARRAY(INTEGER())})
          ->type()
          ->kind());
}

} // namespace
} // namespace facebook::velox::exec

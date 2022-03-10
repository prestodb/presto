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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/type/Type.h"

namespace facebook::velox {

namespace {

template <typename T>
struct FuncOne {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Set func_one as non-deterministic.
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& /* result */,
      const arg_type<velox::Varchar>& /* arg1 */) {
    return true;
  }
};

template <typename T>
struct FuncTwo {
  template <typename T1, typename T2>
  FOLLY_ALWAYS_INLINE bool
  call(int64_t& /* result */, const T1& /* arg1 */, const T2& /* arg2 */) {
    return true;
  }
};

template <typename T>
struct FuncThree {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      ArrayWriter<int64_t>& /* result */,
      const ArrayVal<int64_t>& /* arg1 */) {
    return true;
  }
};

template <typename T>
struct FuncFour {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& /* result */,
      const arg_type<velox::Varchar>& /* arg1 */) {
    return true;
  }
};

template <typename T>
struct FuncFive {
  FOLLY_ALWAYS_INLINE bool call(
      int64_t& /* result */,
      const int64_t& /* arg1 */) {
    return true;
  }
};

template <typename T>
struct VariadicFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& /* result */,
      const arg_type<Variadic<velox::Varchar>>& /* arg1 */) {
    return true;
  }
};

class VectorFuncOne : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx* /* context */,
      velox::VectorPtr* /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // varchar -> bigint
    return {velox::exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("varchar")
                .build()};
  }
};

class VectorFuncTwo : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx* /* context */,
      velox::VectorPtr* /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // array(varchar) -> array(bigint)
    return {velox::exec::FunctionSignatureBuilder()
                .returnType("array(bigint)")
                .argumentType("array(varchar)")
                .build()};
  }
};

class VectorFuncThree : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx* /* context */,
      velox::VectorPtr* /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // ... -> opaque
    return {velox::exec::FunctionSignatureBuilder()
                .returnType("opaque")
                .argumentType("any")
                .build()};
  }
};

class VectorFuncFour : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx* /* context */,
      velox::VectorPtr* /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // map(K,V) -> array(K)
    return {velox::exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("array(K)")
                .argumentType("map(K,V)")
                .build()};
  }

  // Make it non-deterministic.
  bool isDeterministic() const override {
    return false;
  }
};

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_one,
    VectorFuncOne::signatures(),
    std::make_unique<VectorFuncOne>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_two,
    VectorFuncTwo::signatures(),
    std::make_unique<VectorFuncTwo>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_three,
    VectorFuncThree::signatures(),
    std::make_unique<VectorFuncThree>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_vector_func_four,
    VectorFuncFour::signatures(),
    std::make_unique<VectorFuncFour>());

inline void registerTestFunctions() {
  // If no alias is specified, ensure it will fallback to the struct name.
  registerFunction<FuncOne, Varchar, Varchar>({"func_one"});

  // func_two has two signatures.
  registerFunction<FuncTwo, int64_t, int64_t, int32_t>({"func_two"});
  registerFunction<FuncTwo, int64_t, int64_t, int16_t>({"func_two"});

  // func_three has two aliases.
  registerFunction<FuncThree, Array<int64_t>, Array<int64_t>>(
      {"func_three_alias1", "func_three_alias2"});

  // We swap func_four and func_five while registering.
  registerFunction<FuncFour, Varchar, Varchar>({"func_five"});
  registerFunction<FuncFive, int64_t, int64_t>({"func_four"});

  registerFunction<VariadicFunc, Varchar, Variadic<Varchar>>({"variadic_func"});

  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_one, "vector_func_one");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_two, "vector_func_two");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_three, "vector_func_three");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_vector_func_four, "vector_func_four");
}
} // namespace

class FunctionRegistryTest : public ::testing::Test {
 public:
  FunctionRegistryTest() {
    registerTestFunctions();
  }

  void testResolveVectorFunction(
      const std::string& functionName,
      const std::vector<TypePtr>& types,
      const TypePtr& expected) {
    checkEqual(velox::resolveFunction(functionName, types), expected);
    checkEqual(velox::resolveVectorFunction(functionName, types), expected);
  }

  void checkEqual(const TypePtr& actual, const TypePtr& expected) {
    if (expected) {
      EXPECT_EQ(*actual, *expected);
    } else {
      EXPECT_EQ(actual, nullptr);
    }
  }
};

TEST_F(FunctionRegistryTest, getFunctionSignatures) {
  auto functionSignatures = getFunctionSignatures();
  ASSERT_EQ(functionSignatures.size(), 11);

  ASSERT_EQ(functionSignatures.count("func_one"), 1);
  ASSERT_EQ(functionSignatures.count("func_two"), 1);
  ASSERT_EQ(functionSignatures.count("func_three"), 0);
  ASSERT_EQ(functionSignatures.count("func_three_alias1"), 1);
  ASSERT_EQ(functionSignatures.count("func_three_alias2"), 1);
  ASSERT_EQ(functionSignatures.count("func_four"), 1);
  ASSERT_EQ(functionSignatures.count("func_five"), 1);
  ASSERT_EQ(functionSignatures.count("variadic_func"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_one"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_two"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_three"), 1);
  ASSERT_EQ(functionSignatures.count("vector_func_four"), 1);

  ASSERT_EQ(functionSignatures["func_one"].size(), 1);
  ASSERT_EQ(functionSignatures["func_two"].size(), 2);
  ASSERT_EQ(functionSignatures["func_three"].size(), 0);
  ASSERT_EQ(functionSignatures["func_three_alias1"].size(), 1);
  ASSERT_EQ(functionSignatures["func_three_alias2"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_one"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_two"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_three"].size(), 1);
  ASSERT_EQ(functionSignatures["vector_func_four"].size(), 1);

  ASSERT_EQ(
      functionSignatures["func_one"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .build()
          ->toString());

  std::vector<std::string> funcTwoSignatures;
  std::transform(
      functionSignatures["func_two"].begin(),
      functionSignatures["func_two"].end(),
      std::back_inserter(funcTwoSignatures),
      [](auto& signature) { return signature->toString(); });
  ASSERT_THAT(
      funcTwoSignatures,
      testing::UnorderedElementsAre(
          exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("bigint")
              .argumentType("integer")
              .build()
              ->toString(),
          exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType("bigint")
              .argumentType("smallint")
              .build()
              ->toString()));

  ASSERT_EQ(
      functionSignatures["func_three_alias1"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(bigint)")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["func_three_alias2"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(bigint)")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["func_four"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("bigint")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["func_five"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["variadic_func"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .variableArity()
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_one"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("bigint")
          .argumentType("varchar")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_two"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("array(bigint)")
          .argumentType("array(varchar)")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_three"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .returnType("opaque")
          .argumentType("any")
          .build()
          ->toString());

  ASSERT_EQ(
      functionSignatures["vector_func_four"].at(0)->toString(),
      exec::FunctionSignatureBuilder()
          .typeVariable("K")
          .typeVariable("V")
          .returnType("array(K)")
          .argumentType("map(K,V)")
          .build()
          ->toString());
}

TEST_F(FunctionRegistryTest, hasSimpleFunctionSignature) {
  auto result = resolveFunction("func_one", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());
}

TEST_F(FunctionRegistryTest, hasSimpleFunctionSignatureWrongArgType) {
  auto result = resolveFunction("func_one", {INTEGER()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasSimpleFunctionSignatureWrongFunctionName) {
  auto result = resolveFunction("method_one", {VARCHAR()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasVariadicFunctionSignature) {
  auto result = resolveFunction("variadic_func", {});
  ASSERT_EQ(*result, *VARCHAR());

  result = resolveFunction("variadic_func", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());

  result = resolveFunction("variadic_func", {VARCHAR(), VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());

  result = resolveFunction("variadic_func", {INTEGER()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature) {
  testResolveVectorFunction("vector_func_one", {VARCHAR()}, BIGINT());
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature2) {
  testResolveVectorFunction(
      "vector_func_two", {ARRAY(VARCHAR())}, ARRAY(BIGINT()));
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature3) {
  testResolveVectorFunction("vector_func_three", {REAL()}, OPAQUE<void>());
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature4) {
  testResolveVectorFunction(
      "vector_func_four", {MAP(BIGINT(), VARCHAR())}, ARRAY(BIGINT()));
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignatureWrongArgType) {
  testResolveVectorFunction("vector_func_one", {INTEGER()}, nullptr);
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignatureWrongFunctionName) {
  testResolveVectorFunction("vector_method_one", {VARCHAR()}, nullptr);
}

TEST_F(FunctionRegistryTest, registerFunctionTwice) {
  // For better or worse, there are code paths that depend on the ability to
  // register the same functions repeatedly and have those repeated calls
  // ignored.
  registerFunction<FuncOne, Varchar, Varchar>({"func_one"});
  registerFunction<FuncOne, Varchar, Varchar>({"func_one"});

  auto& simpleFunctions = exec::SimpleFunctions();
  auto signatures = simpleFunctions.getFunctionSignatures("func_one");
  // The function should only be registered once, despite the multiple calls to
  // registerFunction.
  ASSERT_EQ(signatures.size(), 1);
}

template <typename T>
struct TestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<Varchar>& out,
      const arg_type<Varchar>&,
      const arg_type<Varchar>&) {
    out = "1"_sv;
  }

  void call(int32_t& out, const arg_type<Variadic<Varchar>>&) {
    out = 2;
  }

  void
  call(float& out, const arg_type<Generic<T1>>&, const arg_type<Generic<T1>>&) {
    out = 3;
  }

  void call(int64_t& out, const arg_type<Variadic<Generic<>>>&) {
    out = 4;
  }

  void call(
      double& out,
      const arg_type<Varchar>&,
      const arg_type<Variadic<Generic<>>>&) {
    out = 5;
  }
};

TEST_F(FunctionRegistryTest, resolveFunctionsBasedOnPriority) {
  std::string func = "func_with_priority";

  registerFunction<TestFunction, double, Varchar, Variadic<Generic<>>>({func});
  registerFunction<TestFunction, Varchar, Varchar, Varchar>({func});
  registerFunction<TestFunction, int64_t, Variadic<Generic<>>>({func});
  registerFunction<TestFunction, int32_t, Variadic<Varchar>>({func});
  registerFunction<TestFunction, float, Generic<T1>, Generic<T1>>({func});

  auto result1 = resolveFunction(func, {VARCHAR(), VARCHAR()});
  ASSERT_EQ(*result1, *VARCHAR());

  auto result2 = resolveFunction(func, {VARCHAR(), VARCHAR(), VARCHAR()});
  ASSERT_EQ(*result2, *INTEGER());

  auto result3 = resolveFunction(func, {VARCHAR(), INTEGER()});
  ASSERT_EQ(*result3, *DOUBLE());

  auto result4 = resolveFunction(func, {INTEGER(), VARCHAR()});
  ASSERT_EQ(*result4, *BIGINT());

  auto result5 = resolveFunction(func, {INTEGER(), INTEGER()});
  ASSERT_EQ(*result5, *REAL());
}

} // namespace facebook::velox

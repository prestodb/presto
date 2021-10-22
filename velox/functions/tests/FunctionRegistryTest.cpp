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

VELOX_UDF_BEGIN(func_one)

// Set func_one as non-deterministic.
static constexpr bool is_deterministic = false;

FOLLY_ALWAYS_INLINE bool call(
    out_type<velox::Varchar>& /* result */,
    const arg_type<velox::Varchar>& /* arg1 */) {
  return true;
}
VELOX_UDF_END();

template <typename T1, typename T2>
VELOX_UDF_BEGIN(func_two)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& /* result */,
    const T1& /* arg1 */,
    const T2& /* arg2 */) {
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(func_three)
FOLLY_ALWAYS_INLINE bool call(
    ArrayWriter<int64_t>& /* result */,
    const ArrayVal<int64_t>& /* arg1 */) {
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(func_four)
FOLLY_ALWAYS_INLINE bool call(
    out_type<velox::Varchar>& /* result */,
    const arg_type<velox::Varchar>& /* arg1 */) {
  return true;
}
VELOX_UDF_END();

VELOX_UDF_BEGIN(func_five)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& /* result */,
    const int64_t& /* arg1 */) {
  return true;
}
VELOX_UDF_END();

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
  registerFunction<udf_func_one, Varchar, Varchar>();

  // func_two has two signatures.
  registerFunction<udf_func_two<int64_t, int32_t>, int64_t, int64_t, int32_t>(
      {"func_two"});
  registerFunction<udf_func_two<int64_t, int16_t>, int64_t, int64_t, int16_t>(
      {"func_two"});

  // func_three has two aliases.
  registerFunction<udf_func_three, Array<int64_t>, Array<int64_t>>(
      {"func_three_alias1", "func_three_alias2"});

  // We swap func_four and func_five while registering.
  registerFunction<udf_func_four, Varchar, Varchar>({"func_five"});
  registerFunction<udf_func_five, int64_t, int64_t>({"func_four"});

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
};

TEST_F(FunctionRegistryTest, getFunctionSignatures) {
  auto functionSignatures = getFunctionSignatures();
  ASSERT_EQ(functionSignatures.size(), 10);

  ASSERT_EQ(functionSignatures.count("func_one"), 1);
  ASSERT_EQ(functionSignatures.count("func_two"), 1);
  ASSERT_EQ(functionSignatures.count("func_three"), 0);
  ASSERT_EQ(functionSignatures.count("func_three_alias1"), 1);
  ASSERT_EQ(functionSignatures.count("func_three_alias2"), 1);
  ASSERT_EQ(functionSignatures.count("func_four"), 1);
  ASSERT_EQ(functionSignatures.count("func_five"), 1);
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

TEST_F(FunctionRegistryTest, hasScalarFunctionSignature) {
  auto result = resolveFunction("func_one", {VARCHAR()});
  ASSERT_EQ(*result, *VARCHAR());
}

TEST_F(FunctionRegistryTest, hasScalarFunctionSignatureWrongArgType) {
  auto result = resolveFunction("func_one", {INTEGER()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasScalarFunctionSignatureWrongFunctionName) {
  auto result = resolveFunction("method_one", {VARCHAR()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature) {
  auto result = resolveFunction("vector_func_one", {VARCHAR()});
  ASSERT_EQ(*result, *BIGINT());
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature2) {
  auto result = resolveFunction("vector_func_two", {ARRAY(VARCHAR())});
  ASSERT_EQ(*result, *ARRAY(BIGINT()));
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature3) {
  auto result = resolveFunction("vector_func_three", {REAL()});
  ASSERT_EQ(*result, *OPAQUE<void>());
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignature4) {
  auto result = resolveFunction("vector_func_four", {MAP(BIGINT(), VARCHAR())});
  ASSERT_EQ(*result, *ARRAY(BIGINT()));
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignatureWrongArgType) {
  auto result = resolveFunction("vector_func_one", {INTEGER()});
  ASSERT_EQ(result, nullptr);
}

TEST_F(FunctionRegistryTest, hasVectorFunctionSignatureWrongFunctionName) {
  auto result = resolveFunction("vector_method_one", {VARCHAR()});
  ASSERT_EQ(result, nullptr);
}

} // namespace facebook::velox

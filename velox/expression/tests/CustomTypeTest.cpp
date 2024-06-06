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

#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/OpaqueCustomTypes.h"

namespace facebook::velox::test {

class CustomTypeTest : public functions::test::FunctionBaseTest {};

namespace {
struct FancyInt {
  const int64_t n;

  explicit FancyInt(int64_t _n) : n{_n} {}
};

class FancyIntType : public OpaqueType {
  FancyIntType() : OpaqueType(std::type_index(typeid(FancyInt))) {}

 public:
  static const std::shared_ptr<const FancyIntType>& get() {
    static const std::shared_ptr<const FancyIntType> instance{
        new FancyIntType()};

    return instance;
  }

  std::string toString() const override {
    return name();
  }

  const char* name() const override {
    return "fancy_int";
  }
};

class FancyIntTypeFactories : public CustomTypeFactories {
 public:
  TypePtr getType() const override {
    return FancyIntType::get();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    VELOX_UNSUPPORTED();
  }
};

class ToFancyIntFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto flatInput = args[0]->as<SimpleVector<int64_t>>();

    BaseVector::ensureWritable(rows, outputType, context.pool(), result);
    auto flatResult = result->asFlatVector<std::shared_ptr<void>>();

    rows.applyToSelected([&](auto row) {
      flatResult->set(row, std::make_shared<FancyInt>(flatInput->valueAt(row)));
    });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // bigint -> fancy_int
    return {exec::FunctionSignatureBuilder()
                .returnType("fancy_int")
                .argumentType("bigint")
                .build()};
  }
};

class FromFancyIntFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto flatInput = args[0]->as<SimpleVector<std::shared_ptr<void>>>();

    BaseVector::ensureWritable(rows, BIGINT(), context.pool(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    rows.applyToSelected([&](auto row) {
      flatResult->set(
          row, std::static_pointer_cast<FancyInt>(flatInput->valueAt(row))->n);
    });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // fancy_int -> bigint
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("fancy_int")
                .build()};
  }
};

// Define type to use in simple functions.
struct FancyIntT {
  using type = std::shared_ptr<FancyInt>;
  static constexpr const char* typeName = "fancy_int";
};
using TheFancyInt = CustomType<FancyIntT>;

template <typename T>
struct FancyPlusFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<TheFancyInt>& result,
      const arg_type<TheFancyInt>& a,
      const arg_type<TheFancyInt>& b) {
    result = std::make_shared<FancyInt>(a->n + b->n);
  }
};

class AlwaysFailingTypeFactories : public CustomTypeFactories {
 public:
  TypePtr getType() const override {
    VELOX_UNSUPPORTED();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    VELOX_UNSUPPORTED();
  }
};
} // namespace

/// Register custom type based on OpaqueType. Register a vector function that
/// produces this type, another vector function that consumes this type, and a
/// simple function that takes and returns this type. Verify function signatures
/// and evaluate some expressions.
TEST_F(CustomTypeTest, customType) {
  ASSERT_TRUE(registerCustomType(
      "fancy_int", std::make_unique<FancyIntTypeFactories>()));

  ASSERT_FALSE(registerCustomType(
      "fancy_int", std::make_unique<AlwaysFailingTypeFactories>()));

  registerFunction<FancyPlusFunction, TheFancyInt, TheFancyInt, TheFancyInt>(
      {"fancy_plus"});

  exec::registerVectorFunction(
      "to_fancy_int",
      ToFancyIntFunction::signatures(),
      std::make_unique<ToFancyIntFunction>());
  exec::registerVectorFunction(
      "from_fancy_int",
      FromFancyIntFunction::signatures(),
      std::make_unique<FromFancyIntFunction>());

  // Verify signatures.
  auto signatures = getSignatureStrings("fancy_plus");
  ASSERT_EQ(1, signatures.size());
  ASSERT_EQ(1, signatures.count("(fancy_int,fancy_int) -> fancy_int"));

  signatures = getSignatureStrings("to_fancy_int");
  ASSERT_EQ(1, signatures.size());
  ASSERT_EQ(1, signatures.count("(bigint) -> fancy_int"));

  signatures = getSignatureStrings("from_fancy_int");
  ASSERT_EQ(1, signatures.size());
  ASSERT_EQ(1, signatures.count("(fancy_int) -> bigint"));

  // Evaluate expressions.
  auto data = makeFlatVector<int64_t>({1, 2, 3, 4, 5});

  auto result =
      evaluate("from_fancy_int(to_fancy_int(c0))", makeRowVector({data}));
  assertEqualVectors(data, result);

  result = evaluate(
      "from_fancy_int(to_fancy_int(c0 + 10)) - 10", makeRowVector({data}));
  assertEqualVectors(data, result);

  result = evaluate(
      "from_fancy_int(fancy_plus(to_fancy_int(c0), to_fancy_int(10)))",
      makeRowVector({data}));
  auto expected = makeFlatVector<int64_t>({11, 12, 13, 14, 15});
  assertEqualVectors(expected, result);

  // Cleanup.
  ASSERT_TRUE(unregisterCustomType("fancy_int"));
  ASSERT_FALSE(unregisterCustomType("fancy_int"));
}

TEST_F(CustomTypeTest, getCustomTypeNames) {
  auto names = getCustomTypeNames();
  ASSERT_EQ(
      (std::unordered_set<std::string>{
          "JSON",
          "HYPERLOGLOG",
          "TIMESTAMP WITH TIME ZONE",
          "UUID",
      }),
      names);

  ASSERT_TRUE(registerCustomType(
      "fancy_int", std::make_unique<FancyIntTypeFactories>()));

  names = getCustomTypeNames();
  ASSERT_EQ(
      (std::unordered_set<std::string>{
          "JSON",
          "HYPERLOGLOG",
          "TIMESTAMP WITH TIME ZONE",
          "UUID",
          "FANCY_INT",
      }),
      names);

  ASSERT_TRUE(unregisterCustomType("fancy_int"));
}

TEST_F(CustomTypeTest, nullConstant) {
  ASSERT_TRUE(registerCustomType(
      "fancy_int", std::make_unique<FancyIntTypeFactories>()));

  auto names = getCustomTypeNames();
  for (const auto& name : names) {
    auto type = getCustomType(name);
    auto null = BaseVector::createNullConstant(type, 10, pool());
    EXPECT_TRUE(null->isConstantEncoding());
    EXPECT_TRUE(type->equivalent(*null->type()));
    EXPECT_EQ(type->toString(), null->type()->toString());
    for (auto i = 0; i < 10; ++i) {
      EXPECT_TRUE(null->isNullAt(i));
    }
  }

  ASSERT_TRUE(unregisterCustomType("fancy_int"));
}

struct Tuple {
  int64_t x;
  int64_t y;
};

static constexpr char kName[] = "tuple_type";
using TupleTypeRegistrar = OpaqueCustomTypeRegister<Tuple, kName>;
// The type used in the simple function interface.
using TupleType = typename TupleTypeRegistrar::SimpleType;

template <typename T>
struct FuncMake {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(out_type<TupleType>& result, int64_t a, int64_t b) {
    result = std::make_shared<Tuple>(Tuple{a, b});
  }
};

template <typename T>
struct FuncPlus {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  void call(
      out_type<TupleType>& result,
      const arg_type<TupleType>& a,
      const arg_type<TupleType>& b) {
    result = std::make_shared<Tuple>(Tuple{a->x + b->x, a->y + b->y});
  }
};

template <typename T>
struct FuncReduce {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  void call(int64_t& result, const arg_type<TupleType>& a) {
    result = a->x + a->y;
  }
};

TEST_F(CustomTypeTest, testOpaqueCustomTypeAutoCreation) {
  // register untyped version.
  registerFunction<FuncMake, std::shared_ptr<Tuple>, int64_t, int64_t>(
      {"make_tuple_untyped"});

  ASSERT_ANY_THROW((
      registerFunction<FuncMake, TupleType, int64_t, int64_t>({"make_tuple"})));

  TupleTypeRegistrar::registerType();
  registerFunction<FuncMake, TupleType, int64_t, int64_t>({"make_tuple"});
  registerFunction<FuncPlus, TupleType, TupleType, TupleType>({"plus_tuple"});
  registerFunction<FuncReduce, int64_t, TupleType>({"reduce_tuple"});

  // Verify signatures.
  {
    auto signatures = getSignatureStrings("plus_tuple");
    ASSERT_EQ(1, signatures.size());
    ASSERT_EQ(1, signatures.count("(tuple_type,tuple_type) -> tuple_type"));
  }

  {
    auto signatures = getSignatureStrings("make_tuple_untyped");
    ASSERT_EQ(1, signatures.size());
    ASSERT_EQ(1, signatures.count("(bigint,bigint) -> opaque"));
  }

  {
    auto signatures = getSignatureStrings("make_tuple");
    ASSERT_EQ(1, signatures.size());
    ASSERT_EQ(1, signatures.count("(bigint,bigint) -> tuple_type"));
  }

  {
    auto signatures = getSignatureStrings("reduce_tuple");
    ASSERT_EQ(1, signatures.size());
    ASSERT_EQ(1, signatures.count("(tuple_type) -> bigint"));
  }

  auto data = makeFlatVector<int64_t>({1, 2, 3});

  // Evaluate expressions.
  {
    auto expected = makeFlatVector<int64_t>({4, 8, 12});
    auto result = evaluate(
        "reduce_tuple(plus_tuple(make_tuple(c0, c0), make_tuple(c0, c0)))",
        makeRowVector({data}));
    test::assertEqualVectors(expected, result);
  }

  {
    auto expected = makeFlatVector<int64_t>({2, 4, 6});
    auto result =
        evaluate("reduce_tuple(make_tuple(c0, c0))", makeRowVector({data}));
    test::assertEqualVectors(expected, result);
  }

  VELOX_ASSERT_THROW(
      evaluate(
          "reduce_tuple(make_tuple_untyped(c0, c0))", makeRowVector({data})),
      "");
}
} // namespace facebook::velox::test

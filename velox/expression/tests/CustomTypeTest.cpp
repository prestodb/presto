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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

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
    return "fancy_int";
  }
};

class FancyIntTypeFactories : public CustomTypeFactories {
 public:
  TypePtr getType(std::vector<TypePtr> /* childTypes */) const override {
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
} // namespace

/// Register custom type based on OpaqueType along with a function that produces
/// this type and another function that consumes this type. Evaluate simple
/// expressions.
TEST_F(CustomTypeTest, customType) {
  registerType("fancy_int", std::make_unique<FancyIntTypeFactories>());
  exec::registerVectorFunction(
      "to_fancy_int",
      ToFancyIntFunction::signatures(),
      std::make_unique<ToFancyIntFunction>());
  exec::registerVectorFunction(
      "from_fancy_int",
      FromFancyIntFunction::signatures(),
      std::make_unique<FromFancyIntFunction>());

  auto data = makeFlatVector<int64_t>({1, 2, 3, 4, 5});

  auto result =
      evaluate("from_fancy_int(to_fancy_int(c0))", makeRowVector({data}));
  assertEqualVectors(data, result);

  result = evaluate(
      "from_fancy_int(to_fancy_int(c0 + 10)) - 10", makeRowVector({data}));
  assertEqualVectors(data, result);
}
} // namespace facebook::velox::test

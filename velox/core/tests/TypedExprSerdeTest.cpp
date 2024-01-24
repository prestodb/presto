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
#include "velox/core/Expressions.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::core::test {

class TypedExprSerDeTest : public testing::Test,
                           public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  TypedExprSerDeTest() {
    Type::registerSerDe();

    ITypedExpr::registerSerDe();
  }

  void testSerde(const TypedExprPtr& expression) {
    auto serialized = expression->serialize();

    auto copy =
        velox::ISerializable::deserialize<ITypedExpr>(serialized, pool());

    ASSERT_EQ(expression->toString(), copy->toString());
    ASSERT_EQ(*expression->type(), *copy->type());
    ASSERT_EQ(*expression, *copy);
  }

  std::vector<RowVectorPtr> data_;
};

TEST_F(TypedExprSerDeTest, input) {
  auto expression = std::make_shared<InputTypedExpr>(
      ROW({"a", "b", "c"}, {BIGINT(), BOOLEAN(), VARCHAR()}));

  testSerde(expression);
}

TEST_F(TypedExprSerDeTest, fieldAccess) {
  std::shared_ptr<ITypedExpr> expression =
      std::make_shared<FieldAccessTypedExpr>(BIGINT(), "a");
  testSerde(expression);
  ASSERT_EQ(expression->toString(), "\"a\"");

  expression = std::make_shared<DereferenceTypedExpr>(
      VARCHAR(),
      std::make_shared<FieldAccessTypedExpr>(
          ROW({"a", "b"}, {VARCHAR(), BOOLEAN()}), "ab"),
      0);
  testSerde(expression);
  ASSERT_EQ(expression->toString(), "\"ab\"[a]");
}

TEST_F(TypedExprSerDeTest, constant) {
  auto expression = std::make_shared<ConstantTypedExpr>(BIGINT(), 127LL);
  testSerde(expression);

  expression =
      std::make_shared<ConstantTypedExpr>(makeFlatVector<int32_t>({123, 234}));
  testSerde(expression);

  expression = std::make_shared<ConstantTypedExpr>(makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5},
  }));
  testSerde(expression);
}

TEST_F(TypedExprSerDeTest, call) {
  // a + b
  auto expression = std::make_shared<CallTypedExpr>(
      BIGINT(),
      std::vector<TypedExprPtr>{
          std::make_shared<FieldAccessTypedExpr>(BIGINT(), "a"),
          std::make_shared<FieldAccessTypedExpr>(BIGINT(), "b"),
      },
      "plus");

  testSerde(expression);

  // f(g(h(a, b), c))
  expression = std::make_shared<CallTypedExpr>(
      VARCHAR(),
      std::vector<TypedExprPtr>{
          std::make_shared<CallTypedExpr>(
              DOUBLE(),
              std::vector<TypedExprPtr>{
                  std::make_shared<CallTypedExpr>(
                      BIGINT(),
                      std::vector<TypedExprPtr>{
                          std::make_shared<FieldAccessTypedExpr>(BIGINT(), "a"),
                          std::make_shared<FieldAccessTypedExpr>(BIGINT(), "b"),
                      },
                      "h"),
                  std::make_shared<FieldAccessTypedExpr>(BIGINT(), "c"),
              },
              "g"),
      },
      "f");
  testSerde(expression);
}

TEST_F(TypedExprSerDeTest, cast) {
  auto expression = std::make_shared<CastTypedExpr>(
      BIGINT(), std::make_shared<FieldAccessTypedExpr>(VARCHAR(), "a"), false);
  testSerde(expression);

  expression = std::make_shared<CastTypedExpr>(
      VARCHAR(), std::make_shared<FieldAccessTypedExpr>(BIGINT(), "a"), true);
  testSerde(expression);
}

TEST_F(TypedExprSerDeTest, concat) {
  auto expression = std::make_shared<ConcatTypedExpr>(
      std::vector<std::string>{"x", "y", "z"},
      std::vector<TypedExprPtr>{
          std::make_shared<FieldAccessTypedExpr>(BIGINT(), "a"),
          std::make_shared<FieldAccessTypedExpr>(DOUBLE(), "b"),
          std::make_shared<FieldAccessTypedExpr>(VARBINARY(), "c"),
      });
  testSerde(expression);
}

TEST_F(TypedExprSerDeTest, lambda) {
  // x -> (x > 10)
  auto expression = std::make_shared<LambdaTypedExpr>(
      ROW({"x"}, {BIGINT()}),
      std::make_shared<CallTypedExpr>(
          BOOLEAN(),
          std::vector<TypedExprPtr>{
              std::make_shared<FieldAccessTypedExpr>(BIGINT(), "x"),
              std::make_shared<ConstantTypedExpr>(BIGINT(), 10LL),
          },
          "gt"));
  testSerde(expression);
}

} // namespace facebook::velox::core::test

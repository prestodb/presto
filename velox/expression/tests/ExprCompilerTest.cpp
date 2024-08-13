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
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FieldReference.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {

class ExprCompilerTest : public testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    functions::prestosql::registerAllScalarFunctions();
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
  }

  core::TypedExprPtr makeTypedExpr(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::parseExpr(text, {});
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  core::TypedExprPtr andCall(
      const core::TypedExprPtr& a,
      const core::TypedExprPtr& b) {
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{a, b}, "and");
  }

  core::TypedExprPtr orCall(
      const core::TypedExprPtr& a,
      const core::TypedExprPtr& b) {
    return std::make_shared<core::CallTypedExpr>(
        BOOLEAN(), std::vector<core::TypedExprPtr>{a, b}, "or");
  }

  core::TypedExprPtr concatCall(
      const std::vector<core::TypedExprPtr>& args,
      TypePtr returnType = nullptr) {
    VELOX_CHECK_GE(args.size(), 2);
    if (!returnType) {
      returnType = args[0]->type();
    }
    return std::make_shared<core::CallTypedExpr>(returnType, args, "concat");
  }

  core::TypedExprPtr call(
      const std::string& name,
      const std::vector<core::TypedExprPtr>& args) {
    std::vector<TypePtr> argTypes;
    for (auto& arg : args) {
      argTypes.push_back(arg->type());
    }
    auto returnType = parse::resolveScalarFunctionType(name, argTypes);
    return std::make_shared<core::CallTypedExpr>(returnType, args, name);
  }

  core::TypedExprPtr bigint(int64_t value) {
    return std::make_shared<core::ConstantTypedExpr>(BIGINT(), value);
  }

  core::TypedExprPtr varchar(const std::string& value) {
    return std::make_shared<core::ConstantTypedExpr>(VARCHAR(), variant(value));
  }

  std::function<core::TypedExprPtr(const std::string& name)> makeField(
      const RowTypePtr& rowType) {
    return [&](const std::string& name) -> core::TypedExprPtr {
      auto type = rowType->findChild(name);
      return std::make_shared<core::FieldAccessTypedExpr>(type, name);
    };
  }

  std::unique_ptr<ExprSet> compile(const core::TypedExprPtr& expr) {
    return std::make_unique<ExprSet>(
        std::vector<core::TypedExprPtr>{expr}, execCtx_.get());
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

TEST_F(ExprCompilerTest, constantFolding) {
  auto rowType = ROW({"a"}, {BIGINT()});

  auto field = makeField(rowType);

  // a + (1 + 5) => a + 6
  auto expression =
      call("plus", {field("a"), call("plus", {bigint(1), bigint(5)})});
  ASSERT_EQ("plus(a, 6:BIGINT)", compile(expression)->toString());

  // (a + 1) + 5 is not folded.
  expression = call("plus", {call("plus", {field("a"), bigint(1)}), bigint(5)});
  ASSERT_EQ(
      "plus(plus(a, 1:BIGINT), 5:BIGINT)", compile(expression)->toString());
}

TEST_F(ExprCompilerTest, andFlattening) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {BOOLEAN(), BOOLEAN(), BOOLEAN(), BOOLEAN()});

  auto field = makeField(rowType);

  auto expression =
      andCall(field("a"), andCall(field("b"), andCall(field("c"), field("d"))));
  ASSERT_EQ("and(a, b, c, d)", compile(expression)->toString());

  expression =
      andCall(andCall(andCall(field("a"), field("b")), field("c")), field("d"));
  ASSERT_EQ("and(a, b, c, d)", compile(expression)->toString());

  expression =
      andCall(andCall(field("a"), field("b")), andCall(field("c"), field("d")));
  ASSERT_EQ("and(a, b, c, d)", compile(expression)->toString());

  expression =
      orCall(field("a"), andCall(field("b"), andCall(field("c"), field("d"))));
  ASSERT_EQ("or(a, and(b, c, d))", compile(expression)->toString());

  // Verify no flattening happens when AND is mixed with OR.
  expression =
      andCall(field("a"), orCall(field("b"), andCall(field("c"), field("d"))));
  ASSERT_EQ("and(a, or(b, and(c, d)))", compile(expression)->toString());
}

TEST_F(ExprCompilerTest, orFlattening) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {BOOLEAN(), BOOLEAN(), BOOLEAN(), BOOLEAN()});

  auto field = makeField(rowType);

  auto expression =
      orCall(field("a"), orCall(field("b"), orCall(field("c"), field("d"))));
  ASSERT_EQ("or(a, b, c, d)", compile(expression)->toString());

  expression =
      orCall(orCall(orCall(field("a"), field("b")), field("c")), field("d"));
  ASSERT_EQ("or(a, b, c, d)", compile(expression)->toString());

  expression =
      orCall(orCall(field("a"), field("b")), orCall(field("c"), field("d")));
  ASSERT_EQ("or(a, b, c, d)", compile(expression)->toString());

  expression =
      andCall(field("a"), orCall(field("b"), orCall(field("c"), field("d"))));
  ASSERT_EQ("and(a, or(b, c, d))", compile(expression)->toString());
}

TEST_F(ExprCompilerTest, concatStringFlattening) {
  auto rowType =
      ROW({"a", "b", "c", "d"}, {VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()});

  auto field = makeField(rowType);

  auto expression = concatCall(
      {field("a"),
       concatCall({field("b"), concatCall({field("c"), field("d")})})});
  ASSERT_EQ("concat(a, b, c, d)", compile(expression)->toString());

  expression = concatCall(
      {concatCall({field("a"), field("b"), field("c")}),
       field("a"),
       concatCall({field("d"), field("d")})});
  ASSERT_EQ("concat(a, b, c, a, d, d)", compile(expression)->toString());

  expression = concatCall(
      {field("a"), concatCall({field("b"), field("c")}), field("d")});
  ASSERT_EQ("concat(a, b, c, d)", compile(expression)->toString());

  // Constant folding happens after flattening.
  expression = concatCall(
      {field("a"), concatCall({varchar("---"), varchar("...")}), field("b")});
  ASSERT_EQ(
      "concat(a, ---:VARCHAR, ...:VARCHAR, b)",
      compile(expression)->toString());
}

TEST_F(ExprCompilerTest, concatArrayFlattening) {
  // Verify that array concat is flattened only if all its inputs are of the
  // same type.
  auto rowType =
      ROW({"array1", "array2", "intVal"},
          {ARRAY(INTEGER()), ARRAY(INTEGER()), INTEGER()});

  auto field = makeField(rowType);

  // concat(array1, concat(array2, array2)) => concat(array1, array2, array2)
  auto expression = concatCall(
      {field("array1"), concatCall({field("array2"), field("array2")})});
  ASSERT_EQ("concat(array1, array2, array2)", compile(expression)->toString());

  // concat(array1, concat(array2, concat(array2, intVal)))
  // => concat(array1, array2, concat(array2, intVal))
  expression = concatCall(
      {field("array1"),
       concatCall(
           {field("array2"), concatCall({field("array2"), field("intVal")})})});
  ASSERT_EQ(
      "concat(array1, array2, concat(array2, intVal))",
      compile(expression)->toString());

  // concat(intVal, concat(array2, concat(array2, array1)))
  // => concat(intVal, concat(array2, array2, array1))
  expression = concatCall(
      {field("intVal"),
       concatCall(
           {field("array2"), concatCall({field("array2"), field("array1")})})},
      ARRAY(INTEGER()));
  ASSERT_EQ(
      "concat(intVal, concat(array2, array2, array1))",
      compile(expression)->toString());

  // concat(concat(array2, concat(array2, array1)), intVal)
  // => concat(concat(array2, array2, array1), intVal, )
  expression = concatCall(
      {concatCall(
           {field("array2"), concatCall({field("array2"), field("array1")})}),
       field("intVal")},
      ARRAY(INTEGER()));
  ASSERT_EQ(
      "concat(concat(array2, array2, array1), intVal)",
      compile(expression)->toString());
}

TEST_F(ExprCompilerTest, functionNameNotRegistered) {
  auto expression = std::make_shared<core::CallTypedExpr>(
      VARCHAR(),
      std::vector<core::TypedExprPtr>{varchar("---"), varchar("...")},
      "not_registered_function");

  VELOX_ASSERT_THROW(
      compile(expression),
      "Scalar function name not registered: not_registered_function, "
      "called with arguments: (VARCHAR, VARCHAR).");
}

TEST_F(ExprCompilerTest, functionSignatureNotRegistered) {
  // Concat of two integers isn't supported.
  auto expression = concatCall({bigint(1), bigint(2)});

  VELOX_ASSERT_THROW(
      compile(expression),
      "Scalar function concat not registered with arguments: (BIGINT, BIGINT). "
      "Found function registered with the following signatures:\n"
      "((varchar,varchar...) -> varchar)");
}

TEST_F(ExprCompilerTest, constantFromFlatVector) {
  auto expression = std::make_shared<core::ConstantTypedExpr>(
      makeFlatVector<int64_t>({137, 23, -10}));

  ASSERT_TRUE(expression->valueVector()->isConstantEncoding());

  auto exprSet = compile(expression);
  ASSERT_EQ("137:BIGINT", compile(expression)->toString());
}

TEST_F(ExprCompilerTest, customTypeConstant) {
  auto expression =
      std::make_shared<core::ConstantTypedExpr>(JSON(), "[1, 2, 3]");

  auto exprSet = compile(expression);
  ASSERT_EQ("[1, 2, 3]:JSON", compile(expression)->toString());
}

TEST_F(ExprCompilerTest, rewrites) {
  auto rowType = ROW({"c0", "c1"}, {ARRAY(VARCHAR()), BIGINT()});
  auto arraySortSql =
      "array_sort(c0, (x, y) -> if(length(x) < length(y), -1, if(length(x) > length(y), 1, 0)))";

  ASSERT_NO_THROW(std::make_unique<ExprSet>(
      std::vector<core::TypedExprPtr>{
          makeTypedExpr(arraySortSql, rowType),
          makeTypedExpr("c1 + 5", rowType),
      },
      execCtx_.get()));

  auto exprSet = compile(makeTypedExpr(
      "reduce(c0, 1, (s, x) -> s + x * 2, s -> s)",
      ROW({"c0"}, {ARRAY(BIGINT())})));
  ASSERT_EQ(exprSet->size(), 1);
  ASSERT_EQ(
      exprSet->expr(0)->toString(),
      "plus(1:BIGINT, array_sum_propagate_element_null(transform(c0, (x) -> multiply(x, 2:BIGINT))))");

  exprSet = compile(makeTypedExpr(
      "reduce(c0, 1, (s, x) -> (s + 2) - x, s -> s)",
      ROW({"c0"}, {ARRAY(BIGINT())})));
  ASSERT_EQ(exprSet->size(), 1);
  ASSERT_EQ(
      exprSet->expr(0)->toString(),
      "plus(1:BIGINT, array_sum_propagate_element_null(transform(c0, (x) -> minus(2:BIGINT, x))))");

  exprSet = compile(makeTypedExpr(
      "reduce(c0, 1, (s, x) -> if(x % 2 = 0, s + 3, s), s -> s)",
      ROW({"c0"}, {ARRAY(BIGINT())})));
  ASSERT_EQ(exprSet->size(), 1);
  ASSERT_EQ(
      exprSet->expr(0)->toString(),
      "plus(1:BIGINT, array_sum_propagate_element_null(transform(c0, (x) -> switch(eq(mod(x, 2:BIGINT), 0:BIGINT), 3:BIGINT, 0:BIGINT))))");
}

TEST_F(ExprCompilerTest, eliminateUnnecessaryCast) {
  auto exprSet =
      compile(makeTypedExpr("cast(c0 as BIGINT)", ROW({{"c0", BIGINT()}})));
  ASSERT_EQ(exprSet->size(), 1);
  ASSERT_TRUE(dynamic_cast<const FieldReference*>(exprSet->expr(0).get()));
}

TEST_F(ExprCompilerTest, lambdaExpr) {
  // Ensure that metadata computation correctly pulls in distinct fields from
  // captured columns.

  // Case 1: Standalone Expression
  auto exprSet = compile(makeTypedExpr(
      "find_first_index(c0, (x) -> (x = c1))",
      ROW({"c0", "c1"}, {ARRAY(VARCHAR()), VARCHAR()})));
  ASSERT_EQ(exprSet->size(), 1);
  auto distinctFields = exprSet->expr(0)->distinctFields();
  ASSERT_EQ(distinctFields.size(), 2);

  // Case 2: Shared expression where the metadata is recomputed.
  exprSet = compile(makeTypedExpr(
      "if (find_first_index(c0, (x) -> (x = c1)) - 1 = 0, null::bigint, "
      "find_first_index(c0, (x) -> (x = c1)) - 1)",
      ROW({"c0", "c1"}, {ARRAY(VARCHAR()), VARCHAR()})));
  ASSERT_EQ(exprSet->size(), 1);
  distinctFields = exprSet->expr(0)->distinctFields();
  ASSERT_EQ(distinctFields.size(), 2);
}

} // namespace facebook::velox::exec::test

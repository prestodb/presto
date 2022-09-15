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
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {

class ExprCompilerTest : public testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
  }

  core::TypedExprPtr andCall(
      const core::TypedExprPtr& arg,
      const core::TypedExprPtr& moreArgs...) {
    std::vector<core::TypedExprPtr> args{arg, {moreArgs}};
    return std::make_shared<core::CallTypedExpr>(BOOLEAN(), args, "and");
  }

  core::TypedExprPtr orCall(
      const core::TypedExprPtr& arg,
      const core::TypedExprPtr& moreArgs...) {
    std::vector<core::TypedExprPtr> args{arg, {moreArgs}};
    return std::make_shared<core::CallTypedExpr>(BOOLEAN(), args, "or");
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
    return std::make_shared<core::ConstantTypedExpr>(value);
  }

  std::function<core::FieldAccessTypedExprPtr(const std::string& name)>
  makeField(const RowTypePtr& rowType) {
    return [&](const std::string& name) -> core::FieldAccessTypedExprPtr {
      auto type = rowType->findChild(name);
      return std::make_shared<core::FieldAccessTypedExpr>(type, name);
    };
  }

  std::unique_ptr<ExprSet> compile(const core::TypedExprPtr& expr) {
    return std::make_unique<ExprSet>(
        std::vector<core::TypedExprPtr>{expr}, execCtx_.get());
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
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
} // namespace facebook::velox::exec::test

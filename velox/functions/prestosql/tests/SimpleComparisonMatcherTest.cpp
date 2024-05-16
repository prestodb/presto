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
#include <gtest/gtest.h>

#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/SimpleComparisonMatcher.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions::prestosql {
namespace {

class SimpleComparisonMatcherTest : public testing::Test,
                                    public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions(prefix_);
    parse::registerTypeResolver();
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    parse::ParseOptions options;
    options.functionPrefix = prefix_;
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  const std::string prefix_ = "tp.";
};

class TestFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_UNSUPPORTED();
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("array(T)")
                .argumentType("array(T)")
                .argumentType("function(T,T,bigint)")
                .build()};
  }
};

TEST_F(SimpleComparisonMatcherTest, basic) {
  exec::registerVectorFunction(
      prefix_ + "test_array_sort",
      TestFunction::signatures(),
      std::make_unique<TestFunction>());

  const auto inputType =
      ROW({"a"}, {ARRAY(ROW({"f", "g"}, {BIGINT(), BIGINT()}))});

  auto testMatcher = [&](const std::string& expr,
                         std::optional<bool> lessThan) {
    SCOPED_TRACE(expr);
    auto parsedExpr = parseExpression(
        fmt::format("test_array_sort(a, (x, y) -> {})", expr), inputType);

    auto lambdaExpr = std::dynamic_pointer_cast<const core::LambdaTypedExpr>(
        parsedExpr->inputs()[1]);

    auto comparison =
        functions::prestosql::isSimpleComparison(prefix_, *lambdaExpr);

    ASSERT_EQ(lessThan.has_value(), comparison.has_value());
    if (lessThan.has_value()) {
      ASSERT_EQ(lessThan.value(), comparison->isLessThen);

      auto field = dynamic_cast<const core::DereferenceTypedExpr*>(
          comparison->expr.get());
      ASSERT_TRUE(field != nullptr);
      ASSERT_EQ(0, field->index());
    }
  };

  // Different ways to define x < y (asc) sort order.
  testMatcher("if(x.f > y.f, 1, if(x.f < y.f, -1, 0))", true);
  testMatcher("if(x.f > y.f, 1, if(y.f > x.f, -1, 0))", true);
  testMatcher("if(x.f > y.f, 1, if(x.f = y.f, 0, -1))", true);

  testMatcher("if(x.f < y.f, -1, if(x.f > y.f, 1, 0))", true);
  testMatcher("if(x.f < y.f, -1, if(y.f < x.f, 1, 0))", true);
  testMatcher("if(x.f < y.f, -1, if(x.f = y.f, 0, 1))", true);

  testMatcher("if(x.f = y.f, 0, if(x.f < y.f, -1, 1))", true);
  testMatcher("if(x.f = y.f, 0, if(y.f > x.f, -1, 1))", true);
  testMatcher("if(x.f = y.f, 0, if(x.f > y.f, 1, -1))", true);
  testMatcher("if(x.f = y.f, 0, if(y.f < x.f, 1, -1))", true);

  // Different ways to define x > y (desc) sort order.
  testMatcher("if (x.f < y.f, 1, if (x.f > y.f, -1, 0))", false);
  testMatcher("if (x.f < y.f, 1, if (y.f < x.f, -1, 0))", false);
  testMatcher("if (x.f < y.f, 1, if (y.f = x.f, 0, -1))", false);

  testMatcher("if (x.f > y.f, -1, if (x.f < y.f, 1, 0))", false);
  testMatcher("if (x.f > y.f, -1, if (y.f > x.f, 1, 0))", false);
  testMatcher("if (x.f > y.f, -1, if (y.f = x.f, 0, 1))", false);

  testMatcher("if(x.f = y.f, 0, if(x.f < y.f, 1, -1))", false);
  testMatcher("if(x.f = y.f, 0, if(y.f > x.f, 1, -1))", false);
  testMatcher("if(x.f = y.f, 0, if(x.f > y.f, -1, 1))", false);
  testMatcher("if(x.f = y.f, 0, if(y.f < x.f, -1, 1))", false);

  // Non-matching expressions.
  testMatcher("if(x.f + y.f > 0, 1, -1)", std::nullopt);
  testMatcher("if(x.f < y.f, 1, -1)", std::nullopt);
  testMatcher("if(x.f = y.f, 0, if(x.f > y.f, -1, 0))", std::nullopt);
  testMatcher("if(x.f = y.f, 1, if(x.f > y.f, -1, 0))", std::nullopt);
  testMatcher("if(x.f > (y.f + 5), 1, if(x.f < y.f, -1, 0))", std::nullopt);
  testMatcher("x.f + y.f", std::nullopt);
}

} // namespace
} // namespace facebook::velox::functions::prestosql

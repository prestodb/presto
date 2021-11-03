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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();

class InTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  auto in(std::optional<T> lhs, std::vector<std::optional<T>> rhs) {
    // We don't use evaluateOnce() because we can't get NaN through the DuckDB
    // parser.
    std::vector<std::shared_ptr<const velox::core::ITypedExpr>> args;
    const auto argType = CppToType<T>::create();
    args.push_back(std::make_shared<core::FieldAccessTypedExpr>(argType, "c0"));
    for (const std::optional<T>& element : rhs) {
      args.push_back(std::make_shared<core::ConstantTypedExpr>(
          argType,
          element ? variant(*element) : variant::null(argType->kind())));
    }
    exec::ExprSet expr(
        {std::make_shared<core::CallTypedExpr>(BOOLEAN(), args, "in")},
        &execCtx_);
    auto eval = [&](RowVectorPtr data) -> std::optional<bool> {
      exec::EvalCtx evalCtx(&execCtx_, &expr, data.get());
      std::vector<VectorPtr> results(1);
      expr.eval(SelectivityVector(1), &evalCtx, &results);
      if (!results[0]->isNullAt(0))
        return results[0]->as<SimpleVector<bool>>()->valueAt(0);
      return std::nullopt;
    };
    auto flatResult =
        eval(makeRowVector({makeNullableFlatVector(std::vector{lhs})}));
    auto constResult = eval(makeRowVector({makeConstant(lhs, 1)}));
    CHECK(flatResult == constResult)
        << "flatResult="
        << (flatResult ? folly::to<std::string>(*flatResult) : "null")
        << " constResult="
        << (constResult ? folly::to<std::string>(*constResult) : "null");
    return flatResult;
  }
};

TEST_F(InTest, Int64) {
  EXPECT_EQ(in<int64_t>(1, {1, 2}), true);
  EXPECT_EQ(in<int64_t>(2, {1, 2}), true);
  EXPECT_EQ(in<int64_t>(3, {1, 2}), false);
  EXPECT_EQ(in<int64_t>(std::nullopt, {1, 2}), std::nullopt);
  EXPECT_EQ(in<int64_t>(1, {1, std::nullopt, 2}), true);
  EXPECT_EQ(in<int64_t>(2, {1, std::nullopt, 2}), true);
  EXPECT_EQ(in<int64_t>(3, {1, std::nullopt, 2}), std::nullopt);
  EXPECT_EQ(in<int64_t>(std::nullopt, {1, std::nullopt, 2}), std::nullopt);
}

TEST_F(InTest, Float) {
  EXPECT_EQ(in<float>(1.0, {-1.0, 1.0, 2.0}), true);
  EXPECT_EQ(in<float>(0, {-1.0, 1.0}), false);
  EXPECT_EQ(in<float>(0, {std::nullopt, 1.0}), std::nullopt);
  EXPECT_EQ(in<float>(-0.0, {-1.0, 0.0, 1.0}), true);
  EXPECT_EQ(in<float>(kNan, {-1.0, 0.0, 1.0}), false);
  EXPECT_EQ(in<float>(kNan, {kNan, -1.0, 0.0, 1.0}), true);
}

TEST_F(InTest, Double) {
  EXPECT_EQ(in<double>(1.0, {-1.0, 1.0, 2.0}), true);
  EXPECT_EQ(in<double>(0, {-1.0, 1.0}), false);
  EXPECT_EQ(in<double>(-0.0, {-1.0, 0.0, 1.0}), true);
  EXPECT_EQ(in<double>(kNan, {-1.0, 0.0, 1.0}), false);
  EXPECT_EQ(in<double>(kNan, {kNan, -1.0, 0.0, 1.0}), true);
  EXPECT_EQ(in<double>(kInf, {kNan, -1.0, 0.0, 1.0}), false);
  EXPECT_EQ(in<double>(kInf, {kNan, -1.0, 0.0, 1.0, kInf}), true);
  EXPECT_EQ(in<double>(-kInf, {kNan, -1.0, 0.0, 1.0}), false);
  EXPECT_EQ(in<double>(-kInf, {kNan, -1.0, 0.0, 1.0, kInf}), false);
  EXPECT_EQ(in<double>(-kInf, {kNan, -1.0, 0.0, 1.0, -kInf}), true);
}

TEST_F(InTest, String) {
  EXPECT_EQ(in<std::string>("", {"", std::nullopt}), true);
  EXPECT_EQ(in<std::string>("a", {"", std::nullopt}), std::nullopt);
  EXPECT_EQ(in<std::string>("a", {"", "b"}), false);
  EXPECT_EQ(
      in<std::string>("0123456789abcdef", {"0123456789abcdef", std::nullopt}),
      true);
  EXPECT_EQ(
      in<std::string>("0123456789abcdef-", {"0123456789abcdef", std::nullopt}),
      std::nullopt);
}

TEST_F(InTest, Timestamp) {
  EXPECT_EQ(
      in<Timestamp>(Timestamp(0, 0), {Timestamp(1, 0), std::nullopt}),
      std::nullopt);
  EXPECT_EQ(
      in<Timestamp>(Timestamp(0, 0), {Timestamp(0, 0), Timestamp()}), true);
}

TEST_F(InTest, Bool) {
  EXPECT_EQ(in<bool>(true, {true, false, std::nullopt}), true);
  EXPECT_EQ(in<bool>(true, {false, std::nullopt}), std::nullopt);
  EXPECT_EQ(in<bool>(true, {false}), false);
  EXPECT_EQ(in<bool>(false, {true, false, std::nullopt}), true);
  EXPECT_EQ(in<bool>(false, {false, std::nullopt}), true);
  EXPECT_EQ(in<bool>(false, {false}), true);
}

/// TODO Re-enable this test after changing the signature of IN predicate to
/// use an ARRAY.
TEST_F(InTest, DISABLED_Const) {
  const auto eval = [&](const std::string& expr) {
    return evaluateOnce<bool, bool>(expr, false);
  };
  EXPECT_EQ(eval("5 in (1, 2, 3)"), false);
  EXPECT_EQ(eval("5 in (1, 2)"), false);
  EXPECT_EQ(eval("1 in (1, 2)"), true);
  EXPECT_EQ(eval("1 in (1, 2)"), true);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test

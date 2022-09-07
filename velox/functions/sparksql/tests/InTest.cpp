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
#include "velox/expression/VectorFunction.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();

/// Wraps input in a dictionary that repeats first row.
class TestingDictionaryFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK(rows.isAllSelected());
    const auto size = rows.size();
    auto indices = allocateIndices(size, context.pool());
    result = BaseVector::wrapInDictionary(nullptr, indices, size, args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T, integer -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};

class InTest : public SparkFunctionBaseTest {
 protected:
  InTest() {
    exec::registerVectorFunction(
        "testing_dictionary",
        TestingDictionaryFunction::signatures(),
        std::make_unique<TestingDictionaryFunction>());
  }

  template <typename T>
  auto in(std::optional<T> lhs, std::vector<std::optional<T>> rhs) {
    // We don't use evaluateOnce() because we can't get NaN through the DuckDB
    // parser.

    auto getExpr = [&](bool asDictionary) {
      std::vector<std::shared_ptr<const velox::core::ITypedExpr>> args;
      const auto argType = CppToType<T>::create();
      args.push_back(
          std::make_shared<core::FieldAccessTypedExpr>(argType, "c0"));
      VectorPtr rhsArrayVector =
          vectorMaker_.arrayVectorNullable<T>({std::optional(rhs)});
      if (asDictionary) {
        auto indices = makeIndices(rhs.size(), [](auto /*row*/) { return 0; });
        rhsArrayVector = wrapInDictionary(indices, rhs.size(), rhsArrayVector);
      }

      args.push_back(std::make_shared<core::ConstantTypedExpr>(
          BaseVector::wrapInConstant(1, 0, rhsArrayVector)));
      return exec::ExprSet(
          {std::make_shared<core::CallTypedExpr>(BOOLEAN(), args, "in")},
          &execCtx_);
    };

    auto eval = [&](RowVectorPtr data,
                    bool asDictonary = false) -> std::optional<bool> {
      auto expr = getExpr(asDictonary);
      exec::EvalCtx evalCtx(&execCtx_, &expr, data.get());
      std::vector<VectorPtr> results(1);
      expr.eval(SelectivityVector(1), evalCtx, results);
      // auto last = args.back();
      if (!results[0]->isNullAt(0))
        return results[0]->as<SimpleVector<bool>>()->valueAt(0);
      return std::nullopt;
    };

    auto testForDictionary = [&](bool asDictionary = false) {
      auto lhsVector =
          makeRowVector({makeNullableFlatVector(std::vector{lhs})});
      auto flatResult = eval(lhsVector, asDictionary);
      auto lhsConstantVector = makeRowVector({makeConstant(lhs, 1)});
      auto constResult = eval(lhsConstantVector, asDictionary);
      CHECK(flatResult == constResult)
          << "flatResult="
          << (flatResult ? folly::to<std::string>(*flatResult) : "null")
          << " constResult="
          << (constResult ? folly::to<std::string>(*constResult) : "null");
      return flatResult;
    };

    // Check when dictionary encoded
    auto noDictionary = testForDictionary(false);
    auto withDictionary = testForDictionary(true);
    CHECK(noDictionary == withDictionary)
        << "Dictionary encodings do not match";
    return noDictionary;
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

TEST_F(InTest, Date) {
  EXPECT_EQ(in<Date>(Date(0), {Date(1), std::nullopt}), std::nullopt);
  EXPECT_EQ(in<Date>(Date(0), {Date(0), Date()}), true);
}

TEST_F(InTest, Bool) {
  EXPECT_EQ(in<bool>(true, {true, false, std::nullopt}), true);
  EXPECT_EQ(in<bool>(true, {false, std::nullopt}), std::nullopt);
  EXPECT_EQ(in<bool>(true, {false}), false);
  EXPECT_EQ(in<bool>(false, {true, false, std::nullopt}), true);
  EXPECT_EQ(in<bool>(false, {false, std::nullopt}), true);
  EXPECT_EQ(in<bool>(false, {false}), true);
}

TEST_F(InTest, Const) {
  const auto eval = [&](const std::string& expr) {
    return evaluateOnce<bool, bool>(expr, false);
  };
  EXPECT_EQ(eval("5 in (1, 2, 3)"), false);
  EXPECT_EQ(eval("5 in (1, 2)"), false);
  EXPECT_EQ(eval("1 in (1, 2)"), true);
  EXPECT_EQ(eval("1 in (1, 2)"), true);
}

/// Test IN applied to first argument that is dictionary encoded, but has the
/// same value in all requested rows.
TEST_F(InTest, ConstantDictionary) {
  auto data = makeFlatVector<int32_t>({1, 2, 3, 4});
  EXPECT_EQ(
      evaluateOnce<bool>(
          "testing_dictionary(c0) IN (1, 2, 3)", makeRowVector({data})),
      true);

  EXPECT_EQ(
      evaluateOnce<bool>(
          "testing_dictionary(c0) IN (2, 3, 4)", makeRowVector({data})),
      false);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test

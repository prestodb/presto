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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox {

using namespace facebook::velox::test;

class SimpleFunctionInitTest : public functions::test::FunctionBaseTest {};

namespace {
template <typename T>
struct NonDefaultWithArrayInitFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void initialize(
      const core::QueryConfig& /*config*/,
      const arg_type<int32_t>* /*first*/,
      const arg_type<velox::Array<int32_t>>* second) {
    if (second == nullptr) {
      return;
    }

    elements_.reserve(second->size());

    for (const auto& entry : *second) {
      if (!entry.has_value()) {
        continue;
      }
      elements_.push_back(entry.value());
    }
  }

  bool callNullable(
      out_type<ArrayWriterT<int32_t>>& out,
      const arg_type<int32_t>* first,
      const arg_type<Array<int32_t>>* /*second*/) {
    if (!first) {
      return false;
    }

    if (!elements_.empty()) {
      for (auto i : elements_) {
        out.push_back(i + *first);
      }
    } else {
      out.push_back(*first);
    }

    return true;
  }

 private:
  std::vector<int32_t> elements_;
};

} // namespace

// Ensure initialization supports complex types.
TEST_F(SimpleFunctionInitTest, initializationArray) {
  registerFunction<
      NonDefaultWithArrayInitFunction,
      ArrayWriterT<int32_t>,
      int32_t,
      Array<int32_t>>({"non_default_behavior_with_init"});

  auto testFn =
      [&](const std::optional<int32_t>& first,
          const std::vector<std::optional<int32_t>>& second,
          const std::vector<std::optional<std::vector<std::optional<int32_t>>>>&
              expected) {
        std::vector<std::shared_ptr<const velox::core::ITypedExpr>> args;
        args.push_back(
            std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"));

        auto rhsArrayVector = makeNullableArrayVector<int32_t>({second});
        args.push_back(std::make_shared<core::ConstantTypedExpr>(
            BaseVector::wrapInConstant(1, 0, rhsArrayVector)));
        exec::ExprSet expr(
            {std::make_shared<core::CallTypedExpr>(
                ARRAY(INTEGER()), args, "non_default_behavior_with_init")},
            &execCtx_);
        auto eval = [&](RowVectorPtr data, VectorPtr expectedVector) {
          exec::EvalCtx evalCtx(&execCtx_, &expr, data.get());
          std::vector<VectorPtr> results(1);
          expr.eval(SelectivityVector(1), &evalCtx, &results);
          assertEqualVectors(results[0], expectedVector);
        };
        auto expectedResult = makeVectorWithNullArrays<int32_t>(expected);
        eval(
            makeRowVector({makeNullableFlatVector(std::vector{first})}),
            expectedResult);
        // Should get same results if both inputs are constant.
        eval(makeRowVector({makeConstant(first, 1)}), expectedResult);
      };

  testFn({0}, {1, 2}, {{{1, 2}}});
  testFn({1}, {1, 2}, {{{2, 3}}});
  testFn({std::nullopt}, {1, 2}, {{std::nullopt}});
  testFn({1}, {}, {{{1}}});
  testFn({1}, {std::nullopt}, {{{1}}});
}

namespace {
template <typename T>
struct NonDefaultWithMapInitFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void initialize(
      const core::QueryConfig& /*config*/,
      const arg_type<int32_t>* /*first*/,
      const arg_type<velox::Map<int32_t, int64_t>>* second) {
    if (second == nullptr) {
      return;
    }

    for (const auto& entry : *second) {
      sum_ += entry.second.value();
    }
  }

  bool callNullable(
      out_type<int64_t>& out,
      const arg_type<int32_t>* first,
      const arg_type<velox::Map<int32_t, int64_t>>* /*second*/) {
    if (!first) {
      return false;
    }

    out = *first + sum_;
    return true;
  }

 private:
  int64_t sum_{0};
};

} // namespace

TEST_F(SimpleFunctionInitTest, initializationMap) {
  registerFunction<
      NonDefaultWithMapInitFunction,
      int64_t,
      int32_t,
      Map<int32_t, int64_t>>({"non_default_behavior_with_map_init"});

  auto mapVectorPtr = makeMapVector<int32_t, int64_t>(
      1,
      [](auto row) { return 3; },
      [](auto row) { return row; },
      [](auto row) { return row; });
  auto inputVector = makeNullableFlatVector<int32_t>({1, 2, 3});
  auto expectedResults = makeFlatVector<int64_t>({4, 5, 6});

  std::vector<std::shared_ptr<const velox::core::ITypedExpr>> args;
  args.push_back(std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0"));

  args.push_back(std::make_shared<core::ConstantTypedExpr>(
      BaseVector::wrapInConstant(1, 0, mapVectorPtr)));
  exec::ExprSet expr(
      {std::make_shared<core::CallTypedExpr>(
          BIGINT(), args, "non_default_behavior_with_map_init")},
      &execCtx_);

  auto rowPtr = makeRowVector({inputVector});
  exec::EvalCtx evalCtx(&execCtx_, &expr, rowPtr.get());
  std::vector<VectorPtr> results(1);
  expr.eval(SelectivityVector(3), &evalCtx, &results);
  assertEqualVectors(results[0], expectedResults);
}

} // namespace facebook::velox

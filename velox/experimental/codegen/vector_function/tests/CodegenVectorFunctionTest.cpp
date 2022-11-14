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

#include <folly/Random.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "velox/experimental/codegen/vector_function/GeneratedVectorFunction-inl.h" // NOLINT (CLANGTIDY  ) facebook-hte-InlineHeader
#include "velox/experimental/codegen/vector_function/StringTypes.h"

namespace facebook::velox::codegen {

struct GeneratedVectorFunctionConfigDouble {
  struct Generated1 {
    using VeloxInputType = std::tuple<DoubleType, DoubleType>;
    using VeloxOutputType = std::tuple<DoubleType>;
    struct State {
      TempsAllocator allocator;
    } state;

    template <typename IN, typename OUT>
    void operator()(IN&& input, OUT&& output) {
      std::get<0>(std::forward<OUT>(output)) =
          *std::get<0>(std::forward<IN>(input)) +
          *std::get<1>(std::forward<IN>(input));
    }
  };

  struct Generated2 {
    using VeloxInputType = std::tuple<DoubleType, DoubleType>;
    using VeloxOutputType = std::tuple<DoubleType>;
    struct State {
      TempsAllocator allocator;
    } state;

    template <typename IN, typename OUT>
    void operator()(IN&& input, OUT&& output) {
      std::get<0>(std::forward<OUT>(output)) =
          *std::get<0>(std::forward<IN>(input)) -
          *std::get<1>(std::forward<IN>(input));
    }
  };
  using Type1 =
      std::tuple<Generated1, std::index_sequence<0, 1>, std::index_sequence<0>>;
  using Type2 =
      std::tuple<Generated2, std::index_sequence<0, 1>, std::index_sequence<1>>;

  using GeneratedCodeClass = ConcatExpression<
      false,
      std::tuple<DoubleType, DoubleType>,
      std::tuple<DoubleType, DoubleType>,
      Type1,
      Type2>;

  static constexpr bool isDefaultNull = false;
  static constexpr bool isDefaultNullStrict = false;
};

TEST(TestConcat, BasicConcatRow) {
  GeneratedVectorFunctionConfigDouble::GeneratedCodeClass concat;
  auto args =
      std::make_tuple<std::optional<double>, std::optional<double>>(3, 4);
  auto output = std::tuple<std::optional<double>, std::optional<double>>();
  concat(args, output);
  EXPECT_EQ(std::get<0>(output), *std::get<0>(args) + *std::get<1>(args));
  EXPECT_EQ(std::get<1>(output), *std::get<0>(args) - *std::get<1>(args));
}

TEST(TestConcat, EvalConcatFunction) {
  const size_t rowLength = 1000;
  SelectivityVector rows(rowLength);
  rows.setAll();

  auto inRowType =
      ROW({"a", "b"},
          {std::make_shared<DoubleType>(), std::make_shared<DoubleType>()});
  auto outRowType =
      ROW({"pl", "mi"},
          {std::make_shared<DoubleType>(), std::make_shared<DoubleType>()});
  auto pool = memory::getDefaultMemoryPool();
  auto inRowVector = BaseVector::create(inRowType, rowLength, pool.get());
  auto outRowVector = BaseVector::create(outRowType, rowLength, pool.get());

  VectorPtr& in1 = inRowVector->as<RowVector>()->childAt(0);
  VectorPtr& in2 = inRowVector->as<RowVector>()->childAt(1);

  in1->resize(rowLength);
  in2->resize(rowLength);
  in1->addNulls(nullptr, rows);
  in2->addNulls(nullptr, rows);

  std::vector<VectorPtr> in{in1, in2};
  auto queryCtx = std::make_shared<core::QueryCtx>();
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());
  exec::EvalCtx context(execCtx.get(), nullptr, inRowVector->as<RowVector>());
  GeneratedVectorFunction<GeneratedVectorFunctionConfigDouble> vectorFunction;

  vectorFunction.setRowType(outRowType);

  // Initializing vector data;
  rows.applyToSelected([&](size_t row) {
    in1->asFlatVector<double>()->set(row, (double)row);
    in2->asFlatVector<double>()->set(row, 1.5 * (double)row);
  });

  vectorFunction.apply(rows, in, nullptr, &context, &outRowVector);

  VectorPtr& out1 = outRowVector->as<RowVector>()->childAt(0);
  VectorPtr& out2 = outRowVector->as<RowVector>()->childAt(1);

  EXPECT_TRUE(rows.testSelected([&](size_t row) -> ::testing::AssertionResult {
    if (out1->isNullAt(row)) {
      return ::testing::AssertionResult(false) << "out1 is null at row " << row;
    };

    if (out2->isNullAt(row)) {
      return ::testing::AssertionResult(false) << "out2 is null at row " << row;
    };

    if (out1->asFlatVector<double>()->valueAt(row) !=
        in1->asFlatVector<double>()->valueAt(row) +
            in2->asFlatVector<double>()->valueAt(row)) {
      return ::testing::AssertionResult(false) << "Wrong value at  " << row;
    };
    return ::testing::AssertionResult(true);
  }));
}

struct GeneratedVectorFunctionConfigBool {
  struct BoolExpressionAnd {
    using VeloxInputType = std::tuple<BooleanType, BooleanType>;
    using VeloxOutputType = std::tuple<BooleanType>;
    struct State {
      TempsAllocator allocator;
    } state;

    template <typename IN, typename OUT>
    void operator()(IN&& input, OUT&& output) {
      std::get<0>(std::forward<OUT>(output)) =
          *std::get<0>(std::forward<IN>(input)) &&
          *std::get<1>(std::forward<IN>(input));
    }
  };

  struct BoolExpressionOr {
    using VeloxInputType = std::tuple<BooleanType, BooleanType>;
    using VeloxOutputType = std::tuple<BooleanType>;
    struct State {
      TempsAllocator allocator;
    } state;

    template <typename IN, typename OUT>
    void operator()(IN&& input, OUT&& output) {
      std::get<0>(std::forward<OUT>(output)) =
          *std::get<0>(std::forward<IN>(input)) ||
          *std::get<1>(std::forward<IN>(input));
    }
  };
  using Type1 = std::tuple<
      BoolExpressionAnd,
      std::index_sequence<0, 1>,
      std::index_sequence<0>>;

  using Type2 = std::tuple<
      BoolExpressionOr,
      std::index_sequence<0, 1>,
      std::index_sequence<1>>;

  using GeneratedCodeClass = ConcatExpression<
      false,
      std::tuple<BooleanType, BooleanType>,
      std::tuple<BooleanType, BooleanType>,
      Type1,
      Type2>;

  static constexpr bool isDefaultNull = false;
  static constexpr bool isDefaultNullStrict = false;
};

TEST(TestBooEvalVectorFunction, EvalBoolExpression) {
  // TODO: Move those to test class
  auto pool = memory::getDefaultMemoryPool();
  const size_t vectorSize = 1000;
  auto queryCtx = std::make_shared<core::QueryCtx>();
  auto execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx.get());

  auto inRowType =
      ROW({"a", "b"},
          {std::make_shared<BooleanType>(), std::make_shared<BooleanType>()});
  auto outRowType =
      ROW({"and1", "and2"},
          {std::make_shared<BooleanType>(), std::make_shared<BooleanType>()});

  // Initializing input vectors
  auto inRowVector = BaseVector::create(inRowType, vectorSize, pool.get());
  auto outRowVector = BaseVector::create(outRowType, vectorSize, pool.get());

  VectorPtr& inputVector1 = inRowVector->as<RowVector>()->childAt(0);
  VectorPtr& inputVector2 = inRowVector->as<RowVector>()->childAt(1);

  inputVector1->resize(vectorSize);
  inputVector2->resize(vectorSize);

  for (auto i = 0; i < vectorSize; i++) {
    inputVector1->asFlatVector<bool>()->setNull(i, false);
    inputVector2->asFlatVector<bool>()->setNull(i, false);

    inputVector1->asFlatVector<bool>()->setNull(i, folly::Random::rand32() % 2);
    inputVector2->asFlatVector<bool>()->setNull(i, folly::Random::rand32() % 2);

    inputVector1->asFlatVector<bool>()->set(i, folly::Random::rand32() % 2);
    inputVector2->asFlatVector<bool>()->set(i, folly::Random::rand32() % 2);
  }

  SelectivityVector rows(vectorSize);
  rows.setAll();
  rows.resize(vectorSize);

  GeneratedVectorFunction<GeneratedVectorFunctionConfigBool> vectorFunction;
  vectorFunction.setRowType(outRowType);

  // Eval
  exec::EvalCtx context(execCtx.get(), nullptr, inRowVector->as<RowVector>());
  std::vector<VectorPtr> inputs{inputVector1, inputVector2};
  vectorFunction.apply(rows, inputs, nullptr, &context, &outRowVector);

  auto* out1 = outRowVector->as<RowVector>()->childAt(0)->asFlatVector<bool>();
  auto* out2 = outRowVector->as<RowVector>()->childAt(1)->asFlatVector<bool>();

  auto in1Flat = inputVector1->asFlatVector<bool>();
  auto in2Flat = inputVector2->asFlatVector<bool>();

  for (auto i = 0; i < vectorSize; i++) {
    ASSERT_EQ(out1->valueAt(i), in1Flat->valueAt(i) && in2Flat->valueAt(i));
    ASSERT_EQ(out2->valueAt(i), in1Flat->valueAt(i) || in2Flat->valueAt(i));
  }
}
} // namespace facebook::velox::codegen

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

#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/tests/SimpleAggregateFunctionsRegistration.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using facebook::velox::functions::aggregate::test::AggregationTestBase;

namespace facebook::velox::aggregate::test {
namespace {

const char* const kSimpleAvg = "simple_avg";
const char* const kSimpleArrayAgg = "simple_array_agg";
const char* const kSimpleCountNulls = "simple_count_nulls";

class SimpleAverageAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();

    registerSimpleAverageAggregate(kSimpleAvg);
  }
};

TEST_F(SimpleAverageAggregationTest, averageAggregate) {
  auto inputVectors = makeRowVector(
      {makeFlatVector<bool>(
           {true,
            false,
            true,
            false,
            true,
            false,
            true,
            false,
            true,
            false,
            true,
            false}),
       makeFlatVector<bool>(
           {true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            false,
            false}),
       makeNullableFlatVector<int64_t>(
           {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, std::nullopt, std::nullopt}),
       makeNullableFlatVector<double>(
           {1.1,
            2.2,
            3.3,
            4.4,
            5.5,
            6.6,
            7.7,
            8.8,
            9.9,
            11,
            std::nullopt,
            std::nullopt})});

  auto expected = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeFlatVector<double>({5, 6}),
       makeFlatVector<double>({5.5, 6.6})});
  testAggregations(
      {inputVectors}, {"c0"}, {"simple_avg(c2)", "simple_avg(c3)"}, {expected});

  expected = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeNullableFlatVector<double>({5.5, std::nullopt}),
       makeNullableFlatVector<double>({6.05, std::nullopt})});
  testAggregations(
      {inputVectors}, {"c1"}, {"simple_avg(c2)", "simple_avg(c3)"}, {expected});

  expected = makeRowVector(
      {makeFlatVector<double>(std::vector<double>{5.5}),
       makeFlatVector<double>(std::vector<double>{6.05})});
  testAggregations(
      {inputVectors}, {}, {"simple_avg(c2)", "simple_avg(c3)"}, {expected});

  inputVectors = makeRowVector({makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>({std::nullopt})});
  testAggregations({inputVectors}, {}, {"simple_avg(c0)"}, {expected});
}

class SimpleArrayAggAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();

    registerSimpleArrayAggAggregate(kSimpleArrayAgg);
  }
};

TEST_F(SimpleArrayAggAggregationTest, numbers) {
  auto inputVectors = makeRowVector(
      {makeFlatVector<bool>(
           {true,
            false,
            true,
            false,
            true,
            false,
            true,
            false,
            true,
            false,
            true,
            false}),
       makeFlatVector<bool>(
           {true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            false,
            false}),
       makeNullableFlatVector<int64_t>(
           {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, std::nullopt, std::nullopt}),
       makeNullableFlatVector<double>(
           {1.1,
            2.2,
            3.3,
            4.4,
            5.5,
            6.6,
            7.7,
            8.8,
            9.9,
            11,
            std::nullopt,
            std::nullopt})});
  auto expected = makeRowVector(
      {makeNullableArrayVector<int64_t>(
           {{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, std::nullopt, std::nullopt}}),
       makeNullableArrayVector<double>(
           {{1.1,
             2.2,
             3.3,
             4.4,
             5.5,
             6.6,
             7.7,
             8.8,
             9.9,
             11,
             std::nullopt,
             std::nullopt}})});
  testAggregations(
      {inputVectors},
      {},
      {"simple_array_agg(c2)", "simple_array_agg(c3)"},
      {"array_sort(a0)", "array_sort(a1)"},
      {expected});

  expected = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeNullableArrayVector<int64_t>(
           {{1, 3, 5, 7, 9, std::nullopt}, {2, 4, 6, 8, 10, std::nullopt}}),
       makeNullableArrayVector<double>(
           {{1.1, 3.3, 5.5, 7.7, 9.9, std::nullopt},
            {2.2, 4.4, 6.6, 8.8, 11, std::nullopt}})});
  testAggregations(
      {inputVectors},
      {"c0"},
      {"simple_array_agg(c2)", "simple_array_agg(c3)"},
      {"c0", "array_sort(a0)", "array_sort(a1)"},
      {expected});

  expected = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       vectorMaker_.arrayVectorNullable<int64_t>(
           {{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, {{std::nullopt, std::nullopt}}}),
       vectorMaker_.arrayVectorNullable<double>(
           {{{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 11}},
            {{std::nullopt, std::nullopt}}})});
  testAggregations(
      {inputVectors},
      {"c1"},
      {"simple_array_agg(c2)", "simple_array_agg(c3)"},
      {"c1", "array_sort(a0)", "array_sort(a1)"},
      {expected});

  inputVectors = makeRowVector({makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt})});
  expected = makeRowVector({vectorMaker_.arrayVectorNullable<int64_t>(
      {{{std::nullopt, std::nullopt, std::nullopt}}})});
  testAggregations({inputVectors}, {}, {"simple_array_agg(c0)"}, {expected});
}

TEST_F(SimpleArrayAggAggregationTest, nestedArray) {
  auto inputVectors = makeRowVector(
      {makeFlatVector<bool>({true, false, true, false, true, false}),
       vectorMaker_.arrayVectorNullable<int32_t>(
           {{{1, 2}},
            {{3, 4}},
            {{5, 6}},
            {{7, 8}},
            std::nullopt,
            std::nullopt}),
       vectorMaker_.arrayVectorNullable<StringView>(
           {{{"1a", "2a"}},
            {{"3a", "4a"}},
            {{"5a", "6a"}},
            {{"7a", "8a"}},
            std::nullopt,
            std::nullopt})});

  auto expected = makeRowVector(
      {makeFlatVector<bool>({true, false}),
       makeNullableNestedArrayVector<int32_t>(
           {{{{{1, 2}}, {{5, 6}}, std::nullopt}},
            {{{{3, 4}}, {{7, 8}}, std::nullopt}}}),
       makeNullableNestedArrayVector<StringView>(
           {{{{{"1a", "2a"}}, {{"5a", "6a"}}, std::nullopt}},
            {{{{"3a", "4a"}}, {{"7a", "8a"}}, std::nullopt}}})});
  testAggregations(
      {inputVectors},
      {"c0"},
      {"simple_array_agg(c1)", "simple_array_agg(c2)"},
      {"c0", "array_sort(a0)", "array_sort(a1)"},
      {expected});

  expected = makeRowVector(
      {makeNullableNestedArrayVector<int32_t>(
           {{{{{1, 2}},
              {{3, 4}},
              {{5, 6}},
              {{7, 8}},
              std::nullopt,
              std::nullopt}}}),
       makeNullableNestedArrayVector<StringView>(
           {{{{{"1a", "2a"}},
              {{"3a", "4a"}},
              {{"5a", "6a"}},
              {{"7a", "8a"}},
              std::nullopt,
              std::nullopt}}})});
  testAggregations(
      {inputVectors},
      {},
      {"simple_array_agg(c1)", "simple_array_agg(c2)"},
      {"array_sort(a0)", "array_sort(a1)"},
      {expected});
}

TEST_F(SimpleArrayAggAggregationTest, trackRowSize) {
  core::QueryConfig queryConfig({});
  auto testTractRowSize = [&](core::AggregationNode::Step step,
                              const VectorPtr& input,
                              bool testGlobal) {
    auto fn = Aggregate::create(
        "simple_array_agg",
        isPartialOutput(step) ? core::AggregationNode::Step::kPartial
                              : core::AggregationNode::Step::kSingle,
        std::vector<TypePtr>{BIGINT()},
        ARRAY(BIGINT()),
        queryConfig);

    HashStringAllocator stringAllocator{pool()};
    memory::AllocationPool allocationPool{pool()};
    fn->setAllocator(&stringAllocator);

    int32_t rowSizeOffset = bits::nbytes(1);
    int32_t offset = rowSizeOffset + sizeof(uint32_t);
    offset = bits::roundUp(offset, fn->accumulatorAlignmentSize());
    fn->setOffsets(
        offset,
        RowContainer::nullByte(0),
        RowContainer::nullMask(0),
        RowContainer::initializedByte(0),
        RowContainer::initializedMask(0),
        rowSizeOffset);

    // Make two groups for odd and even rows.
    auto size = input->size();
    std::vector<char> group1(offset + fn->accumulatorFixedWidthSize());
    std::vector<char> group2(offset + fn->accumulatorFixedWidthSize());
    std::vector<char*> groups(size);
    for (auto i = 0; i < size; ++i) {
      groups[i] = i % 2 == 0 ? group1.data() : group2.data();
    }

    std::vector<vector_size_t> indices{0, 1};
    fn->initializeNewGroups(groups.data(), indices);

    SelectivityVector rows{size};
    if (isRawInput(step)) {
      if (testGlobal) {
        fn->addSingleGroupRawInput(group1.data(), rows, {input}, false);
      } else {
        fn->addRawInput(groups.data(), rows, {input}, false);
      }
    } else {
      if (testGlobal) {
        fn->addSingleGroupIntermediateResults(
            group1.data(), rows, {input}, false);
      } else {
        fn->addIntermediateResults(groups.data(), rows, {input}, false);
      }
    }

    VELOX_CHECK_GT(*reinterpret_cast<int32_t*>(groups[0] + rowSizeOffset), 0);
    if (!testGlobal) {
      VELOX_CHECK_GT(*reinterpret_cast<int32_t*>(groups[1] + rowSizeOffset), 0);
    }
  };

  auto rawInput = makeFlatVector<int64_t>({1, 2, 3, 4, 5});
  testTractRowSize(core::AggregationNode::Step::kPartial, rawInput, true);
  testTractRowSize(core::AggregationNode::Step::kPartial, rawInput, false);

  auto intermediate =
      makeArrayVector<int64_t>({{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}});
  testTractRowSize(core::AggregationNode::Step::kFinal, intermediate, true);
  testTractRowSize(core::AggregationNode::Step::kFinal, intermediate, false);
}

// A testing aggregation function that counts the number of nulls in inputs.
// Return NULL for a group if there is no input null in the group.
class CountNullsAggregate {
 public:
  using InputType = Row<double>; // Input vector type wrapped in Row.
  using IntermediateType = int64_t; // Intermediate result type.
  using OutputType = int64_t; // Output vector type.

  static constexpr bool default_null_behavior_ = false;

  struct Accumulator {
    int64_t nullsCount_;

    Accumulator() = delete;

    explicit Accumulator(
        HashStringAllocator* /*allocator*/,
        CountNullsAggregate* /*fn*/) {
      nullsCount_ = 0;
    }

    bool addInput(
        HashStringAllocator* /*allocator*/,
        exec::optional_arg_type<double> data) {
      if (!data.has_value()) {
        nullsCount_++;
        return true;
      }
      return false;
    }

    bool combine(
        HashStringAllocator* /*allocator*/,
        exec::optional_arg_type<int64_t> nullsCount) {
      if (nullsCount.has_value()) {
        nullsCount_ += nullsCount.value();
        return true;
      }
      return false;
    }

    bool writeFinalResult(bool nonNull, exec::out_type<OutputType>& out) {
      return writeResult<OutputType>(nonNull, out);
    }

    bool writeIntermediateResult(
        bool nonNull,
        exec::out_type<IntermediateType>& out) {
      return writeResult<IntermediateType>(nonNull, out);
    }

   private:
    template <typename T>
    bool writeResult(bool nonNull, exec::out_type<T>& out) {
      if (nonNull) {
        out = nullsCount_;
        return true;
      }
      return false;
    }
  };

  using AccumulatorType = Accumulator;
};

exec::AggregateRegistrationResult registerSimpleCountNullsAggregate(
    const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("bigint")
          .argumentType("double")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<SimpleAggregateAdapter<CountNullsAggregate>>(
            step, argTypes, resultType);
      },
      false /*registerCompanionFunctions*/,
      true /*overwrite*/);
}

void registerSimpleCountNullsAggregate() {
  registerSimpleCountNullsAggregate(kSimpleCountNulls);
}

class SimpleCountNullsAggregationTest : public AggregationTestBase {
 protected:
  SimpleCountNullsAggregationTest() {
    registerSimpleCountNullsAggregate();
  }
};

TEST_F(SimpleCountNullsAggregationTest, basic) {
  auto vectors = makeRowVector(
      {makeNullableFlatVector<bool>({true, false, true, false, true, false}),
       makeNullableFlatVector<bool>({true, false, false, true, false, true}),
       makeNullableFlatVector<double>(
           {1.1, std::nullopt, std::nullopt, 4.4, std::nullopt, 5.5})});

  auto expected = makeRowVector(
      {makeNullableFlatVector<bool>({true, false}),
       makeNullableFlatVector<int64_t>({2, 1})});
  testAggregations({vectors}, {"c0"}, {"simple_count_nulls(c2)"}, {expected});

  expected = makeRowVector(
      {makeNullableFlatVector<bool>({true, false}),
       makeNullableFlatVector<int64_t>({std::nullopt, 3})});
  testAggregations({vectors}, {"c1"}, {"simple_count_nulls(c2)"}, {expected});

  expected = makeRowVector({makeNullableFlatVector<int64_t>({3})});
  testAggregations({vectors}, {}, {"simple_count_nulls(c2)"}, {expected});
}

// A testing simple avg aggregate function, and it is used to check for
// expectations for function-level variables. The validation logic is in the
// Accumulator::addInput method.
class FuncLevelVariableTestAggregate {
 public:
  using InputType = Row<int64_t>;
  using IntermediateType = Row<int64_t, double>;
  using OutputType = double;

  // These two variables are used for testing, they are set during the creation
  // of the aggregation function and will be checked in addInput().
  TypePtr inputType_;
  TypePtr resultType_;

  void initialize(
      core::AggregationNode::Step /*step*/,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& resultType) {
    VELOX_CHECK_EQ(argTypes.size(), 1);
    inputType_ = argTypes[0];
    resultType_ = resultType;
  }

  struct Accumulator {
    int64_t sum{0};
    double count{0};
    FuncLevelVariableTestAggregate* fn_;

    explicit Accumulator(
        HashStringAllocator* /*allocator*/,
        FuncLevelVariableTestAggregate* fn)
        : fn_(fn) {}

    void addInput(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<int64_t> data) {
      VELOX_CHECK_NOT_NULL(fn_->inputType_);
      VELOX_CHECK_NOT_NULL(fn_->resultType_);
      if (fn_->inputType_->isRow()) {
        VELOX_CHECK_EQ(fn_->inputType_->size(), 2);
        VELOX_CHECK_EQ(fn_->inputType_->childAt(0), BIGINT());
        VELOX_CHECK_EQ(fn_->inputType_->childAt(1), DOUBLE());
      } else {
        VELOX_CHECK_EQ(fn_->inputType_, BIGINT());
      }
      if (fn_->resultType_->isRow()) {
        VELOX_CHECK_EQ(fn_->resultType_->size(), 2);
        VELOX_CHECK_EQ(fn_->resultType_->childAt(0), BIGINT());
        VELOX_CHECK_EQ(fn_->resultType_->childAt(1), DOUBLE());
      } else {
        VELOX_CHECK_EQ(fn_->resultType_, DOUBLE());
      }
      sum += data;
      count = checkedPlus<int64_t>(count, 1);
    }

    void combine(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<IntermediateType> other) {
      VELOX_CHECK(other.at<0>().has_value());
      VELOX_CHECK(other.at<1>().has_value());
      sum += other.at<0>().value();
      count += other.at<1>().value();
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = std::make_tuple(sum, count);
      return true;
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      out = sum / count;
      return true;
    }
  };

  using AccumulatorType = Accumulator;
};

exec::AggregateRegistrationResult registerFuncLevelVariableTestAggregate(
    const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("DOUBLE")
          .intermediateType("ROW(BIGINT, DOUBLE)")
          .argumentType("BIGINT")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(argTypes.size(), 1, "{} takes at most 1 argument", name);
        return std::make_unique<
            SimpleAggregateAdapter<FuncLevelVariableTestAggregate>>(
            step, argTypes, resultType);
      },
      true /*registerCompanionFunctions*/,
      true /*overwrite*/);
}

class SimpleFuncLevelVariableAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerFuncLevelVariableTestAggregate("simple_func_level_variable_agg");
  }
};

TEST_F(SimpleFuncLevelVariableAggregationTest, simpleAggregateVariables) {
  auto inputVectors = makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4})});
  std::vector<double> finalResult = {2.5};
  auto expected = makeRowVector({makeFlatVector<double>(finalResult)});
  testAggregations(
      {inputVectors}, {}, {"simple_func_level_variable_agg(c0)"}, {expected});
  testAggregationsWithCompanion(
      {inputVectors},
      [](auto& /*builder*/) {},
      {},
      {"simple_func_level_variable_agg(c0)"},
      {{BIGINT()}},
      {},
      {expected},
      {});
}

} // namespace
} // namespace facebook::velox::aggregate::test

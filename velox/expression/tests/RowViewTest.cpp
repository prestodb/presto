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
#include <optional>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/VectorReaders.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/Comparisons.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::functions;
using namespace facebook::velox::test;

DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}

template <bool returnsOptionalValues>
class RowViewTest : public functions::test::FunctionBaseTest {
  using ViewType = exec::RowView<returnsOptionalValues, int32_t, float>;
  using ReadFunction =
      std::function<ViewType(exec::VectorReader<Row<int32_t, float>>&, size_t)>;

  static bool isNullAt(vector_size_t i) {
    return returnsOptionalValues && i % 2 == 0;
  }

 protected:
  VectorPtr makeTestRowVector() {
    std::vector<int32_t> data1;
    std::vector<float> data2;
    auto size = 10;
    for (auto i = 0; i < size; ++i) {
      data1.push_back(i);
      data2.push_back(size - i);
    };
    return makeRowVector(
        {makeFlatVector(data1), makeFlatVector(data2)}, isNullAt);
  }

  ViewType read(
      exec::VectorReader<Row<int32_t, float>>& reader,
      size_t offset) {
    if constexpr (returnsOptionalValues) {
      return reader[offset];
    } else {
      return reader.readNullFree(offset);
    }
  }

  template <typename T, size_t N>
  T at(const exec::RowView<returnsOptionalValues, int32_t, float>& row) {
    auto value = row.template at<N>();

    if constexpr (returnsOptionalValues) {
      return *value;
    } else {
      return value;
    }
  }

  template <typename T, size_t N>
  T get(const exec::RowView<returnsOptionalValues, int32_t, float>& row) {
    auto value = exec::get<N>(row);

    if constexpr (returnsOptionalValues) {
      return *value;
    } else {
      return value;
    }
  }

  void basicTest() {
    auto rowVector = makeTestRowVector();
    DecodedVector decoded;
    exec::VectorReader<Row<int32_t, float>> reader(decode(decoded, *rowVector));

    for (auto i = 0; i < rowVector->size(); ++i) {
      auto isSet = reader.isSet(i);
      ASSERT_EQ(isSet, !isNullAt(i));
      if (isSet) {
        auto&& r = read(reader, i);
        ASSERT_EQ((at<int32_t, 0>(r)), i);
        ASSERT_EQ((at<float, 1>(r)), rowVector->size() - i);
      }
    }
  }

  void encodedTest() {
    auto rowVector = makeTestRowVector();
    // Wrap in dictionary.
    auto vectorSize = rowVector->size();
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(vectorSize, pool_.get());
    auto rawIndices = indices->asMutable<vector_size_t>();
    // Assign indices such that array is reversed.
    for (size_t i = 0; i < vectorSize; ++i) {
      rawIndices[i] = vectorSize - 1 - i;
    }
    rowVector = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, vectorSize, rowVector);

    DecodedVector decoded;
    exec::VectorReader<Row<int32_t, float>> reader(decode(decoded, *rowVector));

    for (auto i = 0; i < rowVector->size(); ++i) {
      auto isSet = reader.isSet(i);
      ASSERT_EQ(isSet, !isNullAt(vectorSize - 1 - i));
      if (isSet) {
        auto&& r = read(reader, i);
        ASSERT_EQ((at<int32_t, 0>(r)), rowVector->size() - i - 1);
        ASSERT_EQ((at<float, 1>(r)), i + 1);
      }
    }
  }

  void getTest() {
    auto rowVector = makeTestRowVector();
    DecodedVector decoded;
    exec::VectorReader<Row<int32_t, float>> reader(decode(decoded, *rowVector));

    for (auto i = 0; i < rowVector->size(); ++i) {
      auto isSet = reader.isSet(i);
      ASSERT_EQ(isSet, !isNullAt(i));
      if (isSet) {
        auto&& r = read(reader, i);
        ASSERT_EQ((get<int32_t, 0>(r)), i);
        ASSERT_EQ((get<float, 1>(r)), rowVector->size() - i);
      }
    }
  }

  void compareTest() {
    auto rowVector1 = makeRowVector(
        {makeNullableFlatVector<int32_t>({std::nullopt}),
         makeNullableFlatVector<float>({1.0})});
    auto rowVector2 = makeRowVector(
        {makeNullableFlatVector<int32_t>({std::nullopt}),
         makeNullableFlatVector<float>({2.0})});
    {
      DecodedVector decoded1;
      DecodedVector decoded2;

      exec::VectorReader<Row<int32_t, float>> reader1(
          decode(decoded1, *rowVector1));
      exec::VectorReader<Row<int32_t, float>> reader2(
          decode(decoded2, *rowVector2));

      ASSERT_TRUE(reader1.isSet(0));
      ASSERT_TRUE(reader2.isSet(0));
      auto l = read(reader1, 0);
      auto r = read(reader2, 0);
      // Default flag for all operators other than `==` is
      // kNullAsIndeterminate
      VELOX_ASSERT_THROW(r < l, "Ordering nulls is not supported");
      VELOX_ASSERT_THROW(r <= l, "Ordering nulls is not supported");
      VELOX_ASSERT_THROW(r > l, "Ordering nulls is not supported");
      VELOX_ASSERT_THROW(r >= l, "Ordering nulls is not supported");

      // Default flag for `==` is kNullAsValue
      ASSERT_FALSE(r == l);

      // Test we can pass in a flag to change the behavior for compare
      ASSERT_LT(
          l.compare(
              r,
              CompareFlags::equality(
                  CompareFlags::NullHandlingMode::kNullAsValue)),
          0);
    }

    // Test indeterminate ROW<integer, float> = [null, 2.0] against
    // [null, 2.0] is indeterminate
    {
      auto rowVector = vectorMaker_.rowVector(
          {BaseVector::createNullConstant(
               ROW({{"a", INTEGER()}}), 1, pool_.get()),
           makeNullableFlatVector<float>({1.0})});

      DecodedVector decoded1;
      exec::VectorReader<Row<int32_t, float>> reader1(
          decode(decoded1, *rowVector1));
      ASSERT_TRUE(reader1.isSet(0));
      auto l = read(reader1, 0);
      auto flags = CompareFlags::equality(
          CompareFlags::NullHandlingMode::kNullAsIndeterminate);
      ASSERT_EQ(l.compare(l, flags), kIndeterminate);
    }

    // Test a layer of indirection with by wrapping the vector in a dictionary
    {
      vector_size_t size = 2;
      auto field1Vector =
          makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; });
      auto field2Vector =
          makeFlatVector<float>(size, [](vector_size_t row) { return row; });
      auto baseVector = makeRowVector(
          {field1Vector, field2Vector}, [](vector_size_t idx) { return idx; });
      BufferPtr indices =
          AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
      auto rawIndices = indices->asMutable<vector_size_t>();
      for (auto i = 0; i < size; ++i) {
        rawIndices[i] = i;
      }
      auto rowVector = wrapInDictionary(
          indices, size, wrapInDictionary(indices, size, baseVector));
      DecodedVector decoded;
      exec::VectorReader<Row<int32_t, float>> reader(
          decode(decoded, *rowVector.get()));

      // Test the equals case so that we are traversing all of the tuples and
      // ensuring correct index accessing.
      auto l = read(reader, 0);
      auto r = read(reader, 0);
      ASSERT_TRUE(l == r);
    }

    {
      auto vector1 = makeRowVector(
          {makeNullableFlatVector<int32_t>({666666666666666, 4, 7}),
           makeNullableFlatVector<float>({3.0, 1.0, 1.0})});
      auto vector2 = makeRowVector(
          {makeNullableFlatVector<int32_t>({3, 123, 5}),
           makeNullableFlatVector<float>({3.0, 0.5, 0.1})});
      vector_size_t size = vector1->size();
      BufferPtr indices =
          AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
      auto rawIndices = indices->asMutable<vector_size_t>();
      for (auto i = 0; i < size; ++i) {
        rawIndices[i] = size - i - 1;
      }
      BufferPtr indices1 =
          AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
      auto rawIndices1 = indices1->asMutable<vector_size_t>();
      for (auto i = 0; i < size; ++i) {
        rawIndices1[i] = i;
      }

      DecodedVector decoded1;
      DecodedVector decoded2;
      auto wrappedRowVector1 =
          wrapInDictionary(indices, (vector1->size()), vector1);
      auto wrappedRowVector2 =
          wrapInDictionary(indices1, (vector2->size()), vector2);
      exec::VectorReader<Row<int32_t, float>> reader1(
          decode(decoded1, *wrappedRowVector1));
      exec::VectorReader<Row<int32_t, float>> reader2(
          decode(decoded2, *wrappedRowVector2));

      ASSERT_TRUE(reader1.isSet(0));
      ASSERT_TRUE(reader2.isSet(0));
      auto l = read(reader1, 0);
      auto r = read(reader2, 0);
      ASSERT_TRUE(l > r);
    }
  }

  void e2eComparisonTest() {
    auto lhs = makeRowVector(
        {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
         makeFlatVector<float>({1.0, 2.0, 3.0, 4.0, 6.0, 0.0})});
    auto rhs = makeRowVector(
        {makeNullableFlatVector<int32_t>({5, 4, 3, 4, 5, 6}),
         makeFlatVector<float>({2.0, 2.0, 3.0, 4.0, 6.0, 1.1})});

    registerFunction<
        EqFunction,
        bool,
        Row<int32_t, float>,
        Row<int32_t, float>>({"row_eq"});
    auto result =
        evaluate<FlatVector<bool>>("row_eq(c0, c1)", makeRowVector({lhs, rhs}));
    assertEqualVectors(
        makeFlatVector<bool>({false, false, true, true, true, false}), result);

    registerFunction<
        NeqFunction,
        bool,
        Row<int32_t, float>,
        Row<int32_t, float>>({"row_neq"});
    result = evaluate<FlatVector<bool>>(
        "row_neq(c0, c1)", makeRowVector({lhs, rhs}));
    assertEqualVectors(
        makeFlatVector<bool>({true, true, false, false, false, true}), result);

    registerFunction<
        LtFunction,
        bool,
        Row<int32_t, float>,
        Row<int32_t, float>>({"row_lt"});
    result =
        evaluate<FlatVector<bool>>("row_lt(c0, c1)", makeRowVector({lhs, rhs}));
    assertEqualVectors(
        makeFlatVector<bool>({true, true, false, false, false, true}), result);

    registerFunction<
        GtFunction,
        bool,
        Row<int32_t, float>,
        Row<int32_t, float>>({"row_gt"});
    result =
        evaluate<FlatVector<bool>>("row_gt(c0, c1)", makeRowVector({lhs, rhs}));
    assertEqualVectors(
        makeFlatVector<bool>({false, false, false, false, false, false}),
        result);

    registerFunction<
        LteFunction,
        bool,
        Row<int32_t, float>,
        Row<int32_t, float>>({"row_lte"});
    result = evaluate<FlatVector<bool>>(
        "row_lte(c0, c1)", makeRowVector({lhs, rhs}));
    assertEqualVectors(
        makeFlatVector<bool>({true, true, true, true, true, true}), result);

    registerFunction<
        GteFunction,
        bool,
        Row<int32_t, float>,
        Row<int32_t, float>>({"row_gte"});
    result = evaluate<FlatVector<bool>>(
        "row_gte(c0, c1)", makeRowVector({lhs, rhs}));
    assertEqualVectors(
        makeFlatVector<bool>({false, false, true, true, true, false}), result);
  }
};

class NullableRowViewTest : public RowViewTest<true> {};

class NullFreeRowViewTest : public RowViewTest<false> {};

TEST_F(NullableRowViewTest, basic) {
  basicTest();
}

TEST_F(NullableRowViewTest, encoded) {
  encodedTest();
}

TEST_F(NullableRowViewTest, get) {
  getTest();
}

TEST_F(NullFreeRowViewTest, basic) {
  basicTest();
}

TEST_F(NullFreeRowViewTest, encoded) {
  encodedTest();
}

TEST_F(NullFreeRowViewTest, get) {
  getTest();
}

TEST_F(NullFreeRowViewTest, materialize) {
  auto result = evaluate(
      "row_constructor(1,'hi',array_constructor(1,2,3))",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  DecodedVector decoded;
  exec::VectorReader<Row<int64_t, Varchar, Array<int64_t>>> reader(
      decode(decoded, *result.get()));

  std::tuple<int64_t, std::string, std::vector<int64_t>> expected{
      1, "hi", {1, 2, 3}};
  ASSERT_EQ(reader.readNullFree(0).materialize(), expected);
}
TEST_F(NullFreeRowViewTest, compare) {
  compareTest();
}

TEST_F(NullFreeRowViewTest, e2eCompare) {
  e2eComparisonTest();
}

TEST_F(NullableRowViewTest, materialize) {
  auto result = evaluate(
      "row_constructor(1, 'hi', array_constructor(1, 2, null))",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  DecodedVector decoded;
  exec::VectorReader<Row<int64_t, Varchar, Array<int64_t>>> reader(
      decode(decoded, *result.get()));

  std::tuple<
      std::optional<int64_t>,
      std::optional<std::string>,
      std::optional<std::vector<std::optional<int64_t>>>>
      expected{1, "hi", {{1, 2, std::nullopt}}};
  ASSERT_EQ(reader[0].materialize(), expected);
}

class DynamicRowViewTest : public functions::test::FunctionBaseTest {};

TEST_F(DynamicRowViewTest, emptyRow) {
  auto rowVector = vectorMaker_.rowVector({});
  rowVector->resize(10);
  DecodedVector decoded;
  exec::VectorReader<DynamicRow> reader(decode(decoded, *rowVector.get()));
  ASSERT_FALSE(reader.mayHaveNulls());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(reader[i].size(), 0);
    ASSERT_TRUE(reader.isSet(i));
  }
}

TEST_F(DynamicRowViewTest, mixedRow) {
  auto arrayVector =
      vectorMaker_.arrayVector<int32_t>({{1}, {2, 3}, {3, 4, 5}});

  auto rowVector = vectorMaker_.rowVector(
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<bool>({true, false, true}),
       arrayVector});

  DecodedVector decoded;
  exec::VectorReader<DynamicRow> reader(decode(decoded, *rowVector.get()));
  ASSERT_FALSE(reader.mayHaveNulls());

  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(reader.isSet(i));
  }
  auto dynamicRowView = reader[1];
  auto nullFreeDynamicRowView = reader.readNullFree(1);

  EXPECT_FALSE(dynamicRowView.at(0)->tryCastTo<int64_t>());
  EXPECT_FALSE(dynamicRowView.at(0)->tryCastTo<Varchar>());
  EXPECT_TRUE(dynamicRowView.at(0)->tryCastTo<int32_t>());

  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(reader[i].at(0)->castTo<int32_t>(), i + 1);
    ASSERT_EQ(reader.readNullFree(i).at(0).castTo<int32_t>(), i + 1);
  }
  EXPECT_FALSE(nullFreeDynamicRowView.at(1).tryCastTo<int64_t>());
  EXPECT_FALSE(nullFreeDynamicRowView.at(1).tryCastTo<Varchar>());
  EXPECT_TRUE(nullFreeDynamicRowView.at(1).tryCastTo<bool>());

  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(reader[i].at(1)->castTo<bool>(), (i % 2 == 0));
    ASSERT_EQ(reader.readNullFree(i).at(1).castTo<bool>(), (i % 2 == 0));
  }

  EXPECT_FALSE(nullFreeDynamicRowView.at(2).tryCastTo<Array<Varchar>>());
  EXPECT_FALSE(nullFreeDynamicRowView.at(2).tryCastTo<Array<int64_t>>());
  auto arrayView = reader[2].at(2)->castTo<Array<int32_t>>();
  ASSERT_EQ(arrayView.size(), 3);
  ASSERT_EQ(arrayView[0], 3);
  ASSERT_EQ(arrayView[1], 4);
  ASSERT_EQ(arrayView[2], 5);
}

TEST_F(DynamicRowViewTest, rowWithNullsInFields) {
  auto rowVector = vectorMaker_.rowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt, 2})});

  DecodedVector decoded;
  exec::VectorReader<DynamicRow> reader(decode(decoded, *rowVector.get()));
  ASSERT_FALSE(reader.mayHaveNulls());
  ASSERT_TRUE(reader[0].at(0));
  ASSERT_FALSE(reader[1].at(0));
  ASSERT_TRUE(reader[2].at(0));
}

template <typename T>
struct StructWidthIfRow {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  // TODO: Ideally we would like to use DynamicRow instead of Any and make this
  // strictly typed. But function signature does not support expressions
  // row(...).
  void call(int64_t& out, const arg_type<Any>& input) {
    if (auto dyanmicRowView = input.template tryCastTo<DynamicRow>()) {
      out = dyanmicRowView->size();
    } else {
      out = 0;
    }
  }
};

TEST_F(DynamicRowViewTest, castToDynamicRowInFunction) {
  registerFunction<StructWidthIfRow, int64_t, Any>({"struct_width"});
  {
    auto flatVector = makeFlatVector<int64_t>({1, 2});

    // Input is not struct.
    auto result = evaluate("struct_width(c0)", makeRowVector({flatVector}));
    assertEqualVectors(makeFlatVector<int64_t>({0, 0}), result);

    result = evaluate(
        "struct_width(c0)", makeRowVector({makeRowVector({flatVector})}));
    assertEqualVectors(makeFlatVector<int64_t>({1, 1}), result);

    result = evaluate(
        "struct_width(c0)",
        makeRowVector({makeRowVector({flatVector, flatVector})}));
    assertEqualVectors(makeFlatVector<int64_t>({2, 2}), result);
  }
}
} // namespace

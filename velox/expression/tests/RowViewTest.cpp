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

#include "velox/expression/VectorReaders.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace {

using namespace facebook::velox;

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
    test::assertEqualVectors(makeFlatVector<int64_t>({0, 0}), result);

    result = evaluate(
        "struct_width(c0)", makeRowVector({makeRowVector({flatVector})}));
    test::assertEqualVectors(makeFlatVector<int64_t>({1, 1}), result);

    result = evaluate(
        "struct_width(c0)",
        makeRowVector({makeRowVector({flatVector, flatVector})}));
    test::assertEqualVectors(makeFlatVector<int64_t>({2, 2}), result);
  }
}
} // namespace

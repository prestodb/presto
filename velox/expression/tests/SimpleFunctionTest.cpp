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

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace {

using namespace facebook::velox;

class SimpleFunctionTest : public functions::test::FunctionBaseTest {};

// Some input data.
static std::vector<std::vector<int64_t>> arrayData = {
    {0, 1, 2, 4},
    {99, 98},
    {101, 42},
    {10001, 12345676},
};

// Function that returns an array of bigints.
VELOX_UDF_BEGIN(array_writer_func)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Array<int64_t>>& out,
    const arg_type<int64_t>& input) {
  const size_t size = arrayData[input].size();
  out.reserve(size);
  for (const auto i : arrayData[input]) {
    out.append(i);
  }
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, arrayWriter) {
  registerFunction<udf_array_writer_func, Array<int64_t>, int64_t>(
      {}, ARRAY(BIGINT()));

  const size_t rows = arrayData.size();
  auto flatVector = makeFlatVector<int64_t>(rows, [](auto row) { return row; });
  auto result = evaluate<ArrayVector>(
      "array_writer_func(c0)", makeRowVector({flatVector}));

  auto expected = vectorMaker_.arrayVector(arrayData);
  assertEqualVectors(expected, result);
}

static std::vector<std::vector<std::string>> stringArrayData = {
    {"a", "b", "c"},
    {"A long-ish sentence about apples.",
     "Another one about oranges.",
     "Just plum."},
    {"MA", "RI", "NY", "CA", "MI"},
};

// Function that returns an array of strings.
VELOX_UDF_BEGIN(array_of_strings_writer_func)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Array<Varchar>>& out,
    const arg_type<int64_t>& input) {
  const size_t size = stringArrayData[input].size();
  out.reserve(size);
  for (const auto value : stringArrayData[input]) {
    out.append(out_type<Varchar>(StringView(value)));
  }
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, arrayOfStringsWriter) {
  registerFunction<udf_array_of_strings_writer_func, Array<Varchar>, int64_t>(
      {}, ARRAY(VARCHAR()));

  const size_t rows = stringArrayData.size();
  auto flatVector = makeFlatVector<int64_t>(rows, [](auto row) { return row; });
  auto result = evaluate<ArrayVector>(
      "array_of_strings_writer_func(c0)", makeRowVector({flatVector}));

  std::vector<std::vector<StringView>> stringViews;
  for (auto i = 0; i < rows; i++) {
    stringViews.push_back({});
    for (auto j = 0; j < stringArrayData[i].size(); j++) {
      stringViews[i].push_back(StringView(stringArrayData[i][j]));
    }
  }

  auto expected = vectorMaker_.arrayVector(stringViews);
  assertEqualVectors(expected, result);
}

// Function that takes an array as input.
VELOX_UDF_BEGIN(array_reader_func)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& out,
    const arg_type<Array<int64_t>>& input) {
  out = input.size();
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, arrayReader) {
  registerFunction<udf_array_reader_func, int64_t, Array<int64_t>>();

  const size_t rows = arrayData.size();
  auto arrayVector = makeArrayVector(arrayData);
  auto result = evaluate<FlatVector<int64_t>>(
      "array_reader_func(c0)", makeRowVector({arrayVector}));

  auto arrayDataLocal = arrayData;
  auto expected = makeFlatVector<int64_t>(
      rows, [&arrayDataLocal](auto row) { return arrayDataLocal[row].size(); });
  assertEqualVectors(expected, result);
}

// Some input data for the rowVector.
static std::vector<int64_t> rowVectorCol1 = {0, 22, 44, 55, 99, 101, 9, 0};
static std::vector<double> rowVectorCol2 =
    {9.1, 22.4, 44.55, 99.9, 1.01, 9.8, 10001.1, 0.1};

// Function that returns a tuple.
VELOX_UDF_BEGIN(row_writer_func)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Row<int64_t, double>>& out,
    const arg_type<int64_t>& input) {
  out = std::make_tuple(rowVectorCol1[input], rowVectorCol2[input]);
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, rowWriter) {
  registerFunction<udf_row_writer_func, Row<int64_t, double>, int64_t>(
      {}, ROW({BIGINT(), DOUBLE()}));

  const size_t rows = rowVectorCol1.size();
  auto flatVector = makeFlatVector<int64_t>(rows, [](auto row) { return row; });
  auto result =
      evaluate<RowVector>("row_writer_func(c0)", makeRowVector({flatVector}));

  auto vector1 = vectorMaker_.flatVector(rowVectorCol1);
  auto vector2 = vectorMaker_.flatVector(rowVectorCol2);
  auto expected = makeRowVector({vector1, vector2});
  assertEqualVectors(expected, result);
}

// Function that takes a tuple as a parameter.
VELOX_UDF_BEGIN(row_reader_func)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& out,
    const arg_type<Row<int64_t, double>>& input) {
  out = *input.template at<0>();
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, rowReader) {
  registerFunction<udf_row_reader_func, int64_t, Row<int64_t, double>>();

  const size_t rows = rowVectorCol1.size();
  auto vector1 = vectorMaker_.flatVector(rowVectorCol1);
  auto vector2 = vectorMaker_.flatVector(rowVectorCol2);
  auto internalRowVector = makeRowVector({vector1, vector2});
  auto result = evaluate<FlatVector<int64_t>>(
      "row_reader_func(c0)", makeRowVector({internalRowVector}));

  auto expected = vectorMaker_.flatVector(rowVectorCol1);
  assertEqualVectors(expected, result);
}

// Function that returns an array of rows.
VELOX_UDF_BEGIN(array_row_writer_func)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Array<Row<int64_t, double>>>& out,
    const arg_type<int32_t>& input) {
  // Appends each row three times.
  auto tuple = std::make_tuple(rowVectorCol1[input], rowVectorCol2[input]);
  out.append(std::optional(tuple));
  out.append(std::optional(tuple));
  out.append(std::optional(tuple));
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, arrayRowWriter) {
  registerFunction<
      udf_array_row_writer_func,
      Array<Row<int64_t, double>>,
      int32_t>({}, ARRAY(ROW({BIGINT(), DOUBLE()})));

  const size_t rows = rowVectorCol1.size();
  auto flatVector = makeFlatVector<int32_t>(rows, [](auto row) { return row; });
  auto result = evaluate<ArrayVector>(
      "array_row_writer_func(c0)", makeRowVector({flatVector}));

  std::vector<std::vector<variant>> data;
  for (int64_t i = 0; i < rows; ++i) {
    data.push_back({
        variant::row({rowVectorCol1[i], rowVectorCol2[i]}),
        variant::row({rowVectorCol1[i], rowVectorCol2[i]}),
        variant::row({rowVectorCol1[i], rowVectorCol2[i]}),
    });
  }
  auto expected =
      vectorMaker_.arrayOfRowVector(ROW({BIGINT(), DOUBLE()}), data);
  assertEqualVectors(expected, result);
}

// Function that takes an array of rows as an argument..
VELOX_UDF_BEGIN(array_row_reader_func)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& out,
    const arg_type<Array<Row<int64_t, double>>>& input) {
  out = 0;
  for (size_t i = 0; i < input.size(); i++) {
    out += *input.at(i)->template at<0>();
  }
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, arrayRowReader) {
  registerFunction<
      udf_array_row_reader_func,
      int64_t,
      Array<Row<int64_t, double>>>();

  const size_t rows = rowVectorCol1.size();
  std::vector<std::vector<variant>> data;

  for (int64_t i = 0; i < rows; ++i) {
    data.push_back({
        variant::row({rowVectorCol1[i], rowVectorCol2[i]}),
        variant::row({rowVectorCol1[i], rowVectorCol2[i]}),
        variant::row({rowVectorCol1[i], rowVectorCol2[i]}),
    });
  }
  auto arrayVector =
      vectorMaker_.arrayOfRowVector(ROW({BIGINT(), DOUBLE()}), data);
  auto result = evaluate<FlatVector<int64_t>>(
      "array_row_reader_func(c0)", makeRowVector({arrayVector}));

  auto localData = rowVectorCol1;
  auto expected = makeFlatVector<int64_t>(
      rows, [&localData](auto row) { return localData[row] * 3; });
  assertEqualVectors(expected, result);
}

using MyType = std::pair<int64_t, double>;

// Function that returns a tuple containing an opaque type
VELOX_UDF_BEGIN(row_opaque_writer_func)
FOLLY_ALWAYS_INLINE bool call(
    out_type<Row<std::shared_ptr<MyType>, int64_t>>& out,
    const arg_type<int64_t>& input) {
  out = std::make_tuple(
      std::make_shared<MyType>(rowVectorCol1[input], rowVectorCol2[input]),
      input + 10);
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, rowOpaqueWriter) {
  registerFunction<
      udf_row_opaque_writer_func,
      Row<std::shared_ptr<MyType>, int64_t>,
      int64_t>();

  const size_t rows = rowVectorCol1.size();
  auto flatVector = makeFlatVector<int64_t>(rows, [](auto row) { return row; });
  auto result = evaluate<RowVector>(
      "row_opaque_writer_func(c0)", makeRowVector({flatVector}));
  auto opaqueOutput =
      std::dynamic_pointer_cast<FlatVector<std::shared_ptr<void>>>(
          result->childAt(0));
  auto bigintOutput =
      std::dynamic_pointer_cast<FlatVector<int64_t>>(result->childAt(1));

  // Opaque flat vector are not comparable with equalValueAt(), so we check it
  // manually.
  for (size_t i = 0; i < rows; i++) {
    auto val = std::static_pointer_cast<MyType>(opaqueOutput->valueAt(i));
    ASSERT_EQ(rowVectorCol1[i], val->first);
    ASSERT_EQ(rowVectorCol2[i], val->second);

    ASSERT_EQ(i + 10, bigintOutput->valueAt(i));
  }
}

// Function that takes a tuple containing an opaque type as a parameter.
VELOX_UDF_BEGIN(row_opaque_reader_func)
FOLLY_ALWAYS_INLINE bool call(
    int64_t& out,
    const arg_type<Row<std::shared_ptr<MyType>, int64_t>>& input) {
  const auto& myType = *input.template at<0>();
  out = myType->first;
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, rowOpaqueReader) {
  registerFunction<
      udf_row_opaque_reader_func,
      int64_t,
      Row<std::shared_ptr<MyType>, int64_t>>();

  const size_t rows = rowVectorCol1.size();
  auto vector1 = makeFlatVector<std::shared_ptr<void>>(rows, [&](auto row) {
    return std::make_shared<MyType>(rowVectorCol1[row], rowVectorCol2[row]);
  });
  auto vector2 = vectorMaker_.flatVector(rowVectorCol1);
  auto internalRowVector = makeRowVector({vector1, vector2});
  auto result = evaluate<FlatVector<int64_t>>(
      "row_opaque_reader_func(c0)", makeRowVector({internalRowVector}));

  auto expected = vectorMaker_.flatVector(rowVectorCol1);
  assertEqualVectors(expected, result);
}

// Nullability tests:

// Test that function with default null behavior won't get called when inputs
// are all null.
VELOX_UDF_BEGIN(default_null_behavior)
FOLLY_ALWAYS_INLINE bool call(out_type<bool>&, int64_t) {
  throw std::runtime_error(
      "Function not supposed to be called on null inputs.");
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, defaultNullBehavior) {
  registerFunction<udf_default_null_behavior, bool, int64_t>();

  // Make a vector filled with nulls.
  auto flatVector = makeFlatVector<int64_t>(
      10, [](auto row) { return row; }, [](auto) { return true; });

  // Check that default null behavior functions don't get called on a null
  // input.
  EXPECT_NO_THROW(evaluate<SimpleVector<bool>>(
      "default_null_behavior(c0)", makeRowVector({flatVector})));
}

// Test that function with non-default null behavior receives parameters as
// nulls. Returns whether the received parameter was null.
VELOX_UDF_BEGIN(non_default_null_behavior)
FOLLY_ALWAYS_INLINE bool callNullable(
    out_type<bool>& out,
    const int64_t* input) {
  out = (input == nullptr);
  return true;
}
VELOX_UDF_END();

TEST_F(SimpleFunctionTest, nonDefaultNullBehavior) {
  registerFunction<udf_non_default_null_behavior, bool, int64_t>();

  // Make a vector filled with nulls.
  const size_t rows = 10;
  auto flatVector = makeFlatVector<int64_t>(
      rows, [](auto row) { return row; }, [](auto) { return true; });

  // Check that nullable function is returning the right results.
  auto result = evaluate<FlatVector<bool>>(
      "non_default_null_behavior(c0)", makeRowVector({flatVector}));
  auto expected = makeFlatVector<bool>(rows, [](auto) { return true; });
  assertEqualVectors(expected, result);
}

} // namespace

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

#include <fmt/core.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cstdint>

#include "velox/expression/VectorWriters.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"

namespace facebook::velox {

using namespace facebook::velox::test;
namespace {
// Function that creates array with values 0...n-1.
// Uses all possible functions in the array proxy interface.
template <typename T>
struct Func {
  template <typename TOut>
  void call(TOut& out, const int64_t& n) {
    for (int i = 0; i < n; i++) {
      switch (i % 5) {
        case 0:
          out.add_item() = i;
          break;
        case 1:
          out.push_back(i);
          break;
        case 2:
          out.add_null();
          break;
        case 3:
          out.resize(out.size() + 1);
          out[out.size() - 1] = i;
          break;
        case 4:
          out.resize(out.size() + 1);
          out.back() = std::nullopt;
          break;
      }
    }
  }
};

class ArrayWriterTest : public functions::test::FunctionBaseTest {
 public:
  VectorPtr prepareResult(const TypePtr& arrayType, vector_size_t size = 1) {
    VectorPtr result;
    BaseVector::ensureWritable(
        SelectivityVector(size), arrayType, this->execCtx_.pool(), result);
    return result;
  }

  template <typename T>
  void testE2E(const std::string& testFunctionName) {
    registerFunction<Func, Array<T>, int64_t>({testFunctionName});

    auto result = evaluate(
        fmt::format("{}(c0)", testFunctionName),
        makeRowVector(
            {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})}));

    std::vector<std::vector<std::optional<T>>> expected;
    for (auto i = 1; i <= 10; i++) {
      expected.push_back({});
      auto& currentExpected = expected[expected.size() - 1];
      for (auto j = 0; j < i; j++) {
        switch (j % 5) {
          case 0:
            currentExpected.push_back(j);
            break;
          case 1:
            currentExpected.push_back(j);
            break;
          case 2:
            currentExpected.push_back(std::nullopt);
            break;
          case 3:
            currentExpected.push_back(j);
            break;
          case 4:
            currentExpected.push_back(std::nullopt);
            break;
        }
      }
    }
    assertEqualVectors(result, makeNullableArrayVector(expected));
  }

  struct TestWriter {
    VectorPtr result;
    std::unique_ptr<exec::VectorWriter<Array<int64_t>>> writer =
        std::make_unique<exec::VectorWriter<Array<int64_t>>>();
  };

  TestWriter makeTestWriter() {
    TestWriter writer;

    writer.result =
        prepareResult(std::make_shared<ArrayType>(ArrayType(BIGINT())));
    writer.writer->init(*writer.result->as<ArrayVector>());
    writer.writer->setOffset(0);
    return writer;
  }
};

TEST_F(ArrayWriterTest, addNull) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& arrayWriter = vectorWriter->current();
  arrayWriter.add_null();
  arrayWriter.add_null();
  arrayWriter.add_null();

  vectorWriter->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{
      {std::nullopt, std::nullopt, std::nullopt}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, pushBackNull) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& arrayWriter = vectorWriter->current();
  arrayWriter.push_back(std::nullopt);
  arrayWriter.push_back(std::optional<int64_t>{std::nullopt});
  arrayWriter.push_back(std::nullopt);
  vectorWriter->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{
      {std::nullopt, std::nullopt, std::nullopt}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, emptyArray) {
  auto [result, vectorWriter] = makeTestWriter();

  vectorWriter->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{{}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, pushBack) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& arrayWriter = vectorWriter->current();

  arrayWriter.push_back(1);
  arrayWriter.push_back(2);
  arrayWriter.push_back(std::optional<int64_t>{3});

  vectorWriter->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{{1, 2, 3}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, addItem) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& arrayWriter = vectorWriter->current();
  {
    auto& intWriter = arrayWriter.add_item();
    intWriter = 1;
  }

  {
    auto& intWriter = arrayWriter.add_item();
    intWriter = 2;
  }

  {
    auto& intWriter = arrayWriter.add_item();
    intWriter = 3;
  }

  vectorWriter->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{{1, 2, 3}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, subscript) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& arrayWriter = vectorWriter->current();
  arrayWriter.resize(3);
  arrayWriter[0] = std::nullopt;
  arrayWriter[1] = 2;
  arrayWriter[2] = 3;

  vectorWriter->commit();

  auto expected =
      std::vector<std::vector<std::optional<int64_t>>>{{std::nullopt, 2, 3}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, multipleRows) {
  auto expected = std::vector<std::vector<std::optional<int64_t>>>{
      {1, 2, 3},
      {},
      {1, 2, 3, 4, 5, 6, 7},
      {std::nullopt, std::nullopt, 1, 2},
      {},
      {}};
  auto result = prepareResult(
      std::make_shared<ArrayType>(ArrayType(BIGINT())), expected.size());

  exec::VectorWriter<Array<int64_t>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());

  for (auto i = 0; i < expected.size(); i++) {
    vectorWriter.setOffset(i);
    auto& proxy = vectorWriter.current();
    // The simple function interface will receive a proxy.
    for (auto j = 0; j < expected[i].size(); j++) {
      proxy.push_back(expected[i][j]);
    }
    // This commit is called by the vector function adapter.
    vectorWriter.commit(true);
  }

  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, e2ePrimitives) {
  testE2E<int8_t>("array_writer_f_int8");
  testE2E<int16_t>("array_writer_f_int16");
  testE2E<int32_t>("array_writer_f_int32");
  testE2E<int64_t>("array_writer_f_int64");
  testE2E<float>("array_writer_f_float");
  testE2E<double>("array_writer_f_double");
  testE2E<bool>("array_writer_f_bool");
}

TEST_F(ArrayWriterTest, testTimeStamp) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(TIMESTAMP())));

  exec::VectorWriter<Array<Timestamp>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());
  vectorWriter.setOffset(0);
  auto& arrayWriter = vectorWriter.current();
  // General interface.
  auto& timeStamp = arrayWriter.add_item();
  timeStamp = Timestamp::fromMillis(1);
  arrayWriter.add_null();

  // STD like interface.
  arrayWriter.push_back(Timestamp::fromMillis(2));
  arrayWriter.push_back(std::nullopt);
  arrayWriter.resize(6);
  arrayWriter[4] = std::nullopt;
  arrayWriter[5] = Timestamp::fromMillis(3);

  vectorWriter.commit();

  auto expected = std::vector<std::vector<std::optional<Timestamp>>>{
      {Timestamp::fromMillis(1),
       std::nullopt,
       Timestamp::fromMillis(2),
       std::nullopt,
       std::nullopt,
       Timestamp::fromMillis(3)}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, testVarChar) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(VARCHAR())));

  exec::VectorWriter<Array<Varchar>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());
  vectorWriter.setOffset(0);
  auto& arrayWriter = vectorWriter.current();
  // General interface is allowed only for arrays of strings.
  {
    auto& stringWriter = arrayWriter.add_item();
    stringWriter.resize(2);
    stringWriter.data()[0] = 'h';
    stringWriter.data()[1] = 'i';
  }

  arrayWriter.add_null();

  {
    auto& stringWriter = arrayWriter.add_item();
    UDFOutputString::assign(stringWriter, "welcome");
  }

  {
    auto& stringWriter = arrayWriter.add_item();
    UDFOutputString::assign(
        stringWriter,
        "test a long string, a bit longer than that, longer, and longer");
  }
  vectorWriter.commit();
  auto expected = std::vector<std::vector<std::optional<StringView>>>{
      {"hi"_sv,
       std::nullopt,
       "welcome"_sv,
       "test a long string, a bit longer than that, longer, and longer"_sv}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, testVarBinary) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(VARBINARY())));

  exec::VectorWriter<Array<Varbinary>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());
  vectorWriter.setOffset(0);
  auto& arrayWriter = vectorWriter.current();
  // General interface is allowed only for arrays of strings.
  {
    auto& stringWriter = arrayWriter.add_item();
    stringWriter.resize(2);
    stringWriter.data()[0] = 'h';
    stringWriter.data()[1] = 'i';
  }

  arrayWriter.add_null();

  {
    auto& stringWriter = arrayWriter.add_item();
    UDFOutputString::assign(stringWriter, "welcome");
  }

  {
    auto& stringWriter = arrayWriter.add_item();
    UDFOutputString::assign(
        stringWriter,
        "test a long string, a bit longer than that, longer, and longer");
  }
  vectorWriter.commit();
  auto expected = std::vector<std::vector<std::optional<StringView>>>{
      {"hi"_sv,
       std::nullopt,
       "welcome"_sv,
       "test a long string, a bit longer than that, longer, and longer"_sv}};

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(result->size());
  decoded.decode(*result, rows);
  exec::VectorReader<Array<Varbinary>> reader(&decoded);
  ASSERT_EQ(reader[0].size(), expected[0].size());
  for (auto i = 0; i < reader[0].size(); i++) {
    ASSERT_EQ(reader[0][i], expected[0][i]);
  }
}

TEST_F(ArrayWriterTest, nestedArray) {
  auto elementType =
      ArrayType(std::make_shared<ArrayType>(ArrayType(INTEGER())));
  auto result = prepareResult(std::make_shared<ArrayType>(elementType));

  exec::VectorWriter<Array<Array<int32_t>>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());
  vectorWriter.setOffset(0);
  auto& arrayWriter = vectorWriter.current();
  // Only general interface is allowed for nested arrays.
  {
    auto& innerArrayWriter = arrayWriter.add_item();
    innerArrayWriter.resize(2);
    innerArrayWriter[0] = 1;
    innerArrayWriter[1] = 2;
  }

  arrayWriter.add_null();

  {
    auto& innerArrayWriter = arrayWriter.add_item();
    innerArrayWriter.resize(3);
    innerArrayWriter[0] = 1;
    innerArrayWriter[1] = std::nullopt;
    innerArrayWriter[2] = 2;
  }

  vectorWriter.commit();
  using array_type = std::optional<std::vector<std::optional<int32_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = std::nullopt;
  array_type array3 = {{1, std::nullopt, 2}};

  assertEqualVectors(
      result,
      makeNullableNestedArrayVector<int32_t>({{{array1, array2, array3}}}));
}

// Creates a matrix of size n*n with numbers 1 to n^2-1 for every input n,
// and nulls in the diagonal.
template <typename T>
struct MakeMatrixFunc {
  template <typename TOut>
  void call(TOut& out, const int64_t& n) {
    int count = 0;
    for (auto i = 0; i < n; i++) {
      auto& matrixRow = out.add_item();
      matrixRow.resize(n);
      for (auto j = 0; j < n; j++) {
        if (i == j) {
          matrixRow[j] = std::nullopt;
        } else {
          matrixRow[j] = count;
        }
        count++;
      }
    }
    VELOX_DCHECK(count == n * n);
  }
};

TEST_F(ArrayWriterTest, nestedArrayE2E) {
  registerFunction<MakeMatrixFunc, Array<Array<int64_t>>, int64_t>(
      {"make_matrix"});

  auto result = evaluate(
      "make_matrix(c0)",
      makeRowVector(
          {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})}));

  // Build the expected output.
  using matrix_row = std::vector<std::optional<int64_t>>;
  using matrix_type = std::vector<std::optional<matrix_row>>;
  std::vector<std::optional<matrix_type>> expected;
  for (auto k = 1; k <= 10; k++) {
    auto& expectedMatrix = **expected.insert(expected.end(), matrix_type());
    auto n = k;
    int count = 0;

    // This is the same loop from NestedArrayFunc.
    for (auto i = 0; i < n; i++) {
      // The only line that is different from NestedArrayFunc.
      auto& matrixRow =
          **expectedMatrix.insert(expectedMatrix.end(), matrix_row());

      matrixRow.resize(n);
      for (auto j = 0; j < n; j++) {
        if (i == j) {
          matrixRow[j] = std::nullopt;
        } else {
          matrixRow[j] = count;
        }
        count++;
      }
    }
  }

  assertEqualVectors(result, makeNullableNestedArrayVector<int64_t>(expected));
}

TEST_F(ArrayWriterTest, copyFromEmptyArray) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& arrayWriter = vectorWriter->current();
  std::vector<int64_t> data = {};
  arrayWriter.copy_from(data);

  vectorWriter->commit();
  vectorWriter->finish();

  assertEqualVectors(result, makeArrayVector<int64_t>({{}}));
}

TEST_F(ArrayWriterTest, copyFromIntArray) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& arrayWriter = vectorWriter->current();
  std::vector<int64_t> data = {1, 2, 3, 4};
  arrayWriter.copy_from(data);

  vectorWriter->commit();
  vectorWriter->finish();

  assertEqualVectors(result, makeNullableArrayVector<int64_t>({{1, 2, 3, 4}}));
}

TEST_F(ArrayWriterTest, copyFromStringArray) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(VARCHAR())));

  exec::VectorWriter<Array<Varchar>> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());
  vectorWriter.setOffset(0);

  auto& arrayWriter = vectorWriter.current();
  std::vector<std::string> data = {"hi", "welcome"};
  arrayWriter.copy_from(data);

  vectorWriter.commit();
  vectorWriter.finish();
  auto expected = std::vector<std::vector<std::optional<StringView>>>{
      {"hi"_sv, "welcome"_sv}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayWriterTest, copyFromNestedArray) {
  auto elementType =
      ArrayType(std::make_shared<ArrayType>(ArrayType(BIGINT())));
  auto result = prepareResult(std::make_shared<ArrayType>(elementType));

  exec::VectorWriter<Array<Array<int64_t>>> vectorWriter;
  vectorWriter.init(*result.get()->as<ArrayVector>());
  vectorWriter.setOffset(0);

  std::vector<std::vector<int64_t>> data = {{}, {1, 2, 3, 4}, {1}};
  auto& arrayWriter = vectorWriter.current();
  arrayWriter.copy_from(data);

  vectorWriter.commit();
  vectorWriter.finish();

  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = {{}};
  array_type array2 = {{1, 2, 3, 4}};
  array_type array3 = {{1}};

  assertEqualVectors(
      result,
      makeNullableNestedArrayVector<int64_t>({{{array1, array2, array3}}}));
}

auto makeCopyFromTestData() {
  std::vector<std::unordered_map<int64_t, int64_t>> data;

  data.clear();
  data.resize(10);
  for (auto i = 0; i < data.size(); i++) {
    auto& map = data[i];
    for (auto j = 0; j < i; j++) {
      map.emplace(i, j + i);
    }
  }
  return data;
}

template <typename T>
struct CopyFromFunc {
  template <typename TOut>
  void call(TOut& out) {
    out.copy_from(makeCopyFromTestData());
  }
};

TEST_F(ArrayWriterTest, copyFromE2EMapArray) {
  registerFunction<CopyFromFunc, Array<Map<int64_t, int64_t>>>({"copy_from"});

  auto result =
      evaluate("copy_from()", makeRowVector({makeFlatVector<int64_t>(1)}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Array<Map<int64_t, int64_t>>> reader(&decoded);
  auto data = makeCopyFromTestData();

  auto arrayView = reader[0];
  ASSERT_EQ(arrayView.size(), data.size());

  for (auto i = 0; i < arrayView.size(); i++) {
    const auto& mapView = *arrayView[i];
    const auto& dataMap = data[i];
    ASSERT_EQ(mapView.size(), dataMap.size());
    auto it1 = mapView.begin();
    auto it2 = dataMap.begin();
    for (; it1 != mapView.end(); it1++, it2++) {
      ASSERT_EQ(it1->first, it2->first);

      auto mapViewValue = it1->second;
      auto dataMapValue = it2->second;
      ASSERT_EQ(*mapViewValue, dataMapValue);
    }
  }
}

template <typename T>
struct CopyFromInputFunc {
  template <typename TOut, typename TIn>
  void callNullFree(TOut& out, const TIn& input) {
    out.copy_from(input);
  }
};

template <typename T>
struct CopyFromNullableInputFunc {
  template <typename TOut, typename TIn>
  void call(TOut& out, const TIn& input) {
    out.copy_from(input);
  }
};

TEST_F(ArrayWriterTest, copyFromNullFreeNestedViewType) {
  registerFunction<
      CopyFromInputFunc,
      Array<Map<int64_t, int64_t>>,
      Array<Map<int64_t, int64_t>>>({"copy_from_input1"});

  auto mapVector1 = makeMapVector<int64_t, int64_t>({{{1, 2}, {3, 4}}});
  auto mapVector2 = makeMapVector<int64_t, int64_t>({{}});
  auto mapVector3 = makeMapVector<int64_t, int64_t>({{{5, 6}}});

  auto result = evaluate(
      "copy_from_input1(array_constructor(c0, c1, c2))",
      makeRowVector({mapVector1, mapVector2, mapVector3}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Array<Map<int64_t, int64_t>>> reader(&decoded);

  auto arrayView = reader.readNullFree(0);
  ASSERT_EQ(arrayView.size(), 3);

  auto map1 = arrayView[0];
  ASSERT_EQ(map1.size(), 2);
  ASSERT_EQ(map1[1], 2);
  ASSERT_EQ(map1[3], 4);

  auto map2 = arrayView[1];
  ASSERT_EQ(map2.size(), 0);

  auto map3 = arrayView[2];
  ASSERT_EQ(map3.size(), 1);
  ASSERT_EQ(map3[5], 6);
}

TEST_F(ArrayWriterTest, copyFromNullFreeArrayView) {
  registerFunction<CopyFromInputFunc, Array<int64_t>, Array<int64_t>>(
      {"copy_from_input2"});

  auto result = evaluate(
      "copy_from_input2(array_constructor(1, 2, 3, 4, 5))",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Array<int64_t>> reader(&decoded);

  auto arrayView = reader.readNullFree(0);
  ASSERT_EQ(arrayView.size(), 5);

  for (auto i = 0; i < 5; i++) {
    ASSERT_EQ(arrayView[i], i + 1);
  }
}

TEST_F(ArrayWriterTest, copyFromNullableArrayView) {
  registerFunction<CopyFromNullableInputFunc, Array<int64_t>, Array<int64_t>>(
      {"copy_from_nullable"});

  auto result = evaluate(
      "copy_from_nullable(array_constructor(1, null, 3, null, 5))",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Array<int64_t>> reader(&decoded);

  auto arrayView = reader[0];
  ASSERT_EQ(
      arrayView.materialize(),
      (std::vector<std::optional<int64_t>>{
          1, std::nullopt, 3, std::nullopt, 5}));
}

TEST_F(ArrayWriterTest, copyFromNestedNullableArrayView) {
  registerFunction<
      CopyFromNullableInputFunc,
      Array<Array<int64_t>>,
      Array<Array<int64_t>>>({"copy_from_nullable_nested"});

  auto result = evaluate(
      "copy_from_nullable_nested(array_constructor(array_constructor(1), array_constructor(3, null, 5)))",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Array<Array<int64_t>>> reader(&decoded);

  auto arrayView = reader[0];
  ASSERT_EQ(
      arrayView.materialize(),
      (std::vector<std::optional<std::vector<std::optional<int64_t>>>>{
          {{1}}, {{3, std::nullopt, 5}}}));
}

template <typename T>
struct AddItemsTestFunc {
  template <typename TOut, typename TIn>
  void call(TOut& out, const TIn& input) {
    out.add_items(input);
    out.add_items(input);
    out.add_items(std::vector<int64_t>{1, 2, 3});
  }

  // Will be called when there is no nulls in the input.
  template <typename TOut, typename TIn>
  void callNullFree(TOut& out, const TIn& input) {
    out.add_items(std::vector<int64_t>{1, 2, 3});
    out.add_items(input);
    out.add_items(input);
  }
};

TEST_F(ArrayWriterTest, addItems) {
  registerFunction<AddItemsTestFunc, Array<int64_t>, Array<int64_t>>(
      {"add_items_test"});
  DecodedVector decoded;
  SelectivityVector rows(1);

  {
    // callNullFree path.
    auto result = evaluate(
        "add_items_test(array_constructor(10, 20))",
        makeRowVector({makeFlatVector<int64_t>(1)}));

    // Test results.
    decoded.decode(*result, rows);
    exec::VectorReader<Array<int64_t>> reader(&decoded);
    ASSERT_EQ(
        reader.readNullFree(0).materialize(),
        (std::vector<int64_t>{1, 2, 3, 10, 20, 10, 20}));
  }

  {
    // call path.
    auto result = evaluate(
        "add_items_test(array_constructor(10, null))",
        makeRowVector({makeFlatVector<int64_t>(1)}));

    // Test results.
    decoded.decode(*result, rows);
    exec::VectorReader<Array<int64_t>> reader(&decoded);
    ASSERT_EQ(
        reader[0].materialize(),
        (std::vector<std::optional<int64_t>>{
            10, std::nullopt, 10, std::nullopt, 1, 2, 3}));
  }
}

// Make sure nested vectors are resized to actual size after writing.
TEST_F(ArrayWriterTest, finishPostSize) {
  using out_t = Array<Array<int32_t>>;

  auto result = prepareResult(CppToType<out_t>::create());

  exec::VectorWriter<out_t> vectorWriter;
  vectorWriter.init(*result->as<ArrayVector>());
  vectorWriter.setOffset(0);

  // Add 3 items in top level array and 10 in inner array.
  auto& arrayWriter = vectorWriter.current();
  arrayWriter.add_item();
  arrayWriter.add_item();
  auto& innerArrayWriter = arrayWriter.add_item();
  innerArrayWriter.resize(10);

  vectorWriter.commit();
  vectorWriter.finish();

  auto* arrayElements = result->as<ArrayVector>()->elements().get();
  ASSERT_EQ(arrayElements->size(), 3);
  ASSERT_EQ(arrayElements->as<ArrayVector>()->elements()->size(), 10);
}

// ArrayWriter should append and not overwrite elements vectors.
TEST_F(ArrayWriterTest, appendToElements) {
  using out_t = Array<int32_t>;

  auto result = prepareResult(CppToType<out_t>::create(), 2);

  {
    // Write array at offset 0.
    exec::VectorWriter<out_t> vectorWriter;
    vectorWriter.init(*result->as<ArrayVector>());
    vectorWriter.setOffset(0);

    auto& arrayWriter = vectorWriter.current();
    arrayWriter.copy_from(std::vector<int32_t>({1, 2, 3}));
    vectorWriter.commit();
    vectorWriter.finish();
  }
  {
    // Write array at offset 1 using another writer.
    exec::VectorWriter<out_t> vectorWriter;
    vectorWriter.init(*result->as<ArrayVector>());
    vectorWriter.setOffset(1);

    auto& arrayWriter = vectorWriter.current();
    arrayWriter.copy_from(std::vector<int32_t>({4, 5, 6}));
    vectorWriter.commit();
    vectorWriter.finish();
  }

  auto* arrayElements = result->as<ArrayVector>()->elements().get();
  ASSERT_EQ(arrayElements->size(), 6);
  ASSERT_EQ(arrayElements->asFlatVector<int32_t>()->valueAt(3), 4);
}
} // namespace
} // namespace facebook::velox

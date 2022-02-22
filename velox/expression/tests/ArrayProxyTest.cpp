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

#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {
// Function that creates array with values 0...n-1.
// Uses all possible functions in the array proxy interface.
template <typename T>
struct Func {
  template <typename TOut>
  bool call(TOut& out, const int64_t& n) {
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
    return true;
  }
};

class ArrayProxyTest : public functions::test::FunctionBaseTest {
 public:
  VectorPtr prepareResult(const TypePtr& arrayType, vector_size_t size = 1) {
    VectorPtr result;
    BaseVector::ensureWritable(
        SelectivityVector(size), arrayType, this->execCtx_.pool(), &result);
    return result;
  }

  template <typename T>
  void testE2E(const std::string& testFunctionName) {
    registerFunction<Func, ArrayProxyT<T>, int64_t>({testFunctionName});

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
    std::unique_ptr<exec::VectorWriter<ArrayProxyT<int64_t>>> writer =
        std::make_unique<exec::VectorWriter<ArrayProxyT<int64_t>>>();
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

TEST_F(ArrayProxyTest, addNull) {
  auto [result, writer] = makeTestWriter();

  auto& proxy = writer->current();
  proxy.add_null();
  proxy.add_null();
  proxy.add_null();
  writer->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{
      {std::nullopt, std::nullopt, std::nullopt}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, pushBackNull) {
  auto [result, writer] = makeTestWriter();

  auto& proxy = writer->current();
  proxy.push_back(std::nullopt);
  proxy.push_back(std::optional<int64_t>{std::nullopt});
  proxy.push_back(std::nullopt);
  writer->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{
      {std::nullopt, std::nullopt, std::nullopt}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, emptyArray) {
  auto [result, writer] = makeTestWriter();

  writer->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{{}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, pushBack) {
  auto [result, writer] = makeTestWriter();

  auto& proxy = writer->current();
  proxy.push_back(1);
  proxy.push_back(2);
  proxy.push_back(std::optional<int64_t>{3});
  writer->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{{1, 2, 3}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, addItem) {
  auto [result, writer] = makeTestWriter();

  auto& arrayProxy = writer->current();
  {
    auto& intProxy = arrayProxy.add_item();
    intProxy = 1;
  }

  {
    auto& intProxy = arrayProxy.add_item();
    intProxy = 2;
  }

  {
    auto& intProxy = arrayProxy.add_item();
    intProxy = 3;
  }

  writer->commit();

  auto expected = std::vector<std::vector<std::optional<int64_t>>>{{1, 2, 3}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, subscript) {
  auto [result, writer] = makeTestWriter();

  auto& arrayProxy = writer->current();
  arrayProxy.resize(3);
  arrayProxy[0] = std::nullopt;
  arrayProxy[1] = 2;
  arrayProxy[2] = 3;

  writer->commit();

  auto expected =
      std::vector<std::vector<std::optional<int64_t>>>{{std::nullopt, 2, 3}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, multipleRows) {
  auto expected = std::vector<std::vector<std::optional<int64_t>>>{
      {1, 2, 3},
      {},
      {1, 2, 3, 4, 5, 6, 7},
      {std::nullopt, std::nullopt, 1, 2},
      {},
      {}};
  auto result = prepareResult(
      std::make_shared<ArrayType>(ArrayType(BIGINT())), expected.size());

  exec::VectorWriter<ArrayProxyT<int64_t>> writer;
  writer.init(*result->as<ArrayVector>());

  for (auto i = 0; i < expected.size(); i++) {
    writer.setOffset(i);
    auto& proxy = writer.current();
    // The simple function interface will receive a proxy.
    for (auto j = 0; j < expected[i].size(); j++) {
      proxy.push_back(expected[i][j]);
    }
    // This commit is called by the vector function adapter.
    writer.commit(true);
  }

  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, e2ePrimitives) {
  testE2E<int8_t>("f_int6");
  testE2E<int16_t>("f_int16");
  testE2E<int32_t>("f_int32");
  testE2E<int64_t>("f_int64");
  testE2E<float>("f_float");
  testE2E<double>("f_double");
  testE2E<bool>("f_bool");
}

TEST_F(ArrayProxyTest, testTimeStamp) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(TIMESTAMP())));

  exec::VectorWriter<ArrayProxyT<Timestamp>> writer;
  writer.init(*result->as<ArrayVector>());
  writer.setOffset(0);
  auto& arrayProxy = writer.current();
  // General interface.
  auto& timeStamp = arrayProxy.add_item();
  timeStamp = Timestamp::fromMillis(1);
  arrayProxy.add_null();

  // STD like interface.
  arrayProxy.push_back(Timestamp::fromMillis(2));
  arrayProxy.push_back(std::nullopt);
  arrayProxy.resize(6);
  arrayProxy[4] = std::nullopt;
  arrayProxy[5] = Timestamp::fromMillis(3);

  writer.commit();

  auto expected = std::vector<std::vector<std::optional<Timestamp>>>{
      {Timestamp::fromMillis(1),
       std::nullopt,
       Timestamp::fromMillis(2),
       std::nullopt,
       std::nullopt,
       Timestamp::fromMillis(3)}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, testVarChar) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(VARCHAR())));

  exec::VectorWriter<ArrayProxyT<Varchar>> writer;
  writer.init(*result->as<ArrayVector>());
  writer.setOffset(0);
  auto& arrayProxy = writer.current();
  // General interface is allowed only for arrays of strings.
  {
    auto& string = arrayProxy.add_item();
    string.resize(2);
    string.data()[0] = 'h';
    string.data()[1] = 'i';
  }

  arrayProxy.add_null();

  {
    auto& string = arrayProxy.add_item();
    UDFOutputString::assign(string, "welcome");
  }

  {
    auto& string = arrayProxy.add_item();
    UDFOutputString::assign(
        string,
        "test a long string, a bit longer than that, longer, and longer");
  }
  writer.commit();
  auto expected = std::vector<std::vector<std::optional<StringView>>>{
      {"hi"_sv,
       std::nullopt,
       "welcome"_sv,
       "test a long string, a bit longer than that, longer, and longer"_sv}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, testVarBinary) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(VARBINARY())));

  exec::VectorWriter<ArrayProxyT<Varbinary>> writer;
  writer.init(*result->as<ArrayVector>());
  writer.setOffset(0);
  auto& arrayProxy = writer.current();
  // General interface is allowed only for arrays of strings.
  {
    auto& string = arrayProxy.add_item();
    string.resize(2);
    string.data()[0] = 'h';
    string.data()[1] = 'i';
  }

  arrayProxy.add_null();

  {
    auto& string = arrayProxy.add_item();
    UDFOutputString::assign(string, "welcome");
  }

  {
    auto& string = arrayProxy.add_item();
    UDFOutputString::assign(
        string,
        "test a long string, a bit longer than that, longer, and longer");
  }
  writer.commit();
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

TEST_F(ArrayProxyTest, nestedArray) {
  auto elementType =
      ArrayType(std::make_shared<ArrayType>(ArrayType(INTEGER())));
  auto result = prepareResult(std::make_shared<ArrayType>(elementType));

  exec::VectorWriter<ArrayProxyT<ArrayProxyT<int32_t>>> writer;
  writer.init(*result.get()->as<ArrayVector>());
  writer.setOffset(0);
  auto& arrayProxy = writer.current();
  // Only general interface is allowed for nested arrays.
  {
    auto& array = arrayProxy.add_item();
    array.resize(2);
    array[0] = 1;
    array[1] = 2;
  }

  arrayProxy.add_null();

  {
    auto& array = arrayProxy.add_item();
    array.resize(3);
    array[0] = 1;
    array[1] = std::nullopt;
    array[2] = 2;
  }

  writer.commit();
  using array_type = std::optional<std::vector<std::optional<int32_t>>>;
  array_type array1 = {{1, 2}};
  array_type array2 = std::nullopt;
  array_type array3 = {{1, std::nullopt, 2}};

  assertEqualVectors(
      result, makeNestedArrayVector<int32_t>({{array1, array2, array3}}));
}

// Creates a matrix of size n*n with numbers 1 to n^2-1 for every input n,
// and nulls in the diagonal.
template <typename T>
struct MakeMatrixFunc {
  template <typename TOut>
  bool call(TOut& out, const int64_t& n) {
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
    return true;
  }
};

TEST_F(ArrayProxyTest, nestedArrayE2E) {
  registerFunction<MakeMatrixFunc, ArrayProxyT<ArrayProxyT<int64_t>>, int64_t>(
      {"func"});

  auto result = evaluate(
      "func(c0)",
      makeRowVector(
          {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})}));

  // Build the expected output.
  using matrix_row = std::vector<std::optional<int64_t>>;
  using matrix_type = std::vector<std::optional<matrix_row>>;
  std::vector<matrix_type> expected;
  for (auto k = 1; k <= 10; k++) {
    auto& expectedMatrix = *expected.insert(expected.end(), matrix_type());
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

  assertEqualVectors(result, makeNestedArrayVector<int64_t>(expected));
}

TEST_F(ArrayProxyTest, copyFromEmptyArray) {
  auto [result, writer] = makeTestWriter();

  auto& proxy = writer->current();
  std::vector<int64_t> data = {};

  proxy.copy_from(data);
  writer->commit();
  writer->finish();

  assertEqualVectors(result, makeNullableArrayVector<int64_t>({{}}));
}

TEST_F(ArrayProxyTest, copyFromIntArray) {
  auto [result, writer] = makeTestWriter();

  auto& proxy = writer->current();
  std::vector<int64_t> data = {1, 2, 3, 4};

  proxy.copy_from(data);
  writer->commit();
  writer->finish();

  assertEqualVectors(result, makeNullableArrayVector<int64_t>({{1, 2, 3, 4}}));
}

TEST_F(ArrayProxyTest, copyFromStringArray) {
  auto result =
      prepareResult(std::make_shared<ArrayType>(ArrayType(VARCHAR())));

  exec::VectorWriter<ArrayProxyT<Varchar>> writer;
  writer.init(*result->as<ArrayVector>());
  writer.setOffset(0);

  auto& arrayProxy = writer.current();
  std::vector<std::string> data = {"hi", "welcome"};
  arrayProxy.copy_from(data);

  writer.commit();
  writer.finish();
  auto expected = std::vector<std::vector<std::optional<StringView>>>{
      {"hi"_sv, "welcome"_sv}};
  assertEqualVectors(result, makeNullableArrayVector(expected));
}

TEST_F(ArrayProxyTest, copyFromNestedArray) {
  auto elementType =
      ArrayType(std::make_shared<ArrayType>(ArrayType(BIGINT())));
  auto result = prepareResult(std::make_shared<ArrayType>(elementType));

  exec::VectorWriter<ArrayProxyT<ArrayProxyT<int64_t>>> writer;
  writer.init(*result.get()->as<ArrayVector>());
  writer.setOffset(0);

  std::vector<std::vector<int64_t>> data = {{}, {1, 2, 3, 4}, {1}};
  auto& arrayProxy = writer.current();
  arrayProxy.copy_from(data);

  writer.commit();
  writer.finish();

  using array_type = std::optional<std::vector<std::optional<int64_t>>>;
  array_type array1 = {{}};
  array_type array2 = {{1, 2, 3, 4}};
  array_type array3 = {{1}};

  assertEqualVectors(
      result, makeNestedArrayVector<int64_t>({{array1, array2, array3}}));
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
  bool call(TOut& out) {
    out.copy_from(makeCopyFromTestData());
    return true;
  }
};

TEST_F(ArrayProxyTest, copyFromE2EMapArray) {
  registerFunction<CopyFromFunc, ArrayProxyT<MapWriterT<int64_t, int64_t>>>(
      {"func"});

  auto result = evaluate("func()", makeRowVector({makeFlatVector<int64_t>(1)}));

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
} // namespace
} // namespace facebook::velox

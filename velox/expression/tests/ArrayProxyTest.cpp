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
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

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
    writer.writer->init(*writer.result.get()->as<ArrayVector>());
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
  writer.init(*result.get()->as<ArrayVector>());

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

TEST_F(ArrayProxyTest, e2e) {
  testE2E<int8_t>("f_int6");
  testE2E<int16_t>("f_int16");
  testE2E<int32_t>("f_int32");
  testE2E<int64_t>("f_int64");
  testE2E<float>("f_float");
  testE2E<double>("f_double");
}
} // namespace
} // namespace facebook::velox

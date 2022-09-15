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
#include <optional>
#include <tuple>

#include <velox/expression/VectorReaders.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/SelectivityVector.h>
#include "folly/container/F14Map.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
namespace facebook::velox {
namespace {

using namespace facebook::velox::test;

// Function that creates a map and covers all the map writer interface.
template <typename T>
struct Func {
  template <typename TOut>
  bool call(TOut& out, const int64_t& n) {
    for (int i = 0; i < n; i++) {
      switch (i % 6) {
        case 0: {
          auto [keyWriter, valueWriter] = out.add_item();
          keyWriter = i;
          valueWriter = i + 1;
          break;
        }
        case 1:
          out.add_null() = i;
          break;
        case 2:
          out.emplace(i, i + 2);
          break;
        case 3:
          out.emplace(i, std::nullopt);
          break;
        case 4:
          out.resize(out.size() + 1);
          out[out.size() - 1] = std::make_tuple(i, std::nullopt);
          break;
        case 5:
          out.resize(out.size() + 1);
          out[out.size() - 1] = std::make_tuple(i, i + 5);
          break;
      }
    }
    return true;
  }
};

class MapWriterTest : public functions::test::FunctionBaseTest {
 public:
  template <typename K, typename V>
  using map_pairs_t = std::vector<std::pair<K, std::optional<V>>>;

  VectorPtr prepareResult(const TypePtr& mapType, vector_size_t size = 1) {
    VectorPtr result;
    BaseVector::ensureWritable(
        SelectivityVector(size), mapType, this->execCtx_.pool(), result);
    return result;
  }

  template <typename T>
  void testE2E(const std::string& testFunctionName) {
    registerFunction<Func, Map<T, T>, int64_t>({testFunctionName});

    auto result = evaluate(
        fmt::format("{}(c0)", testFunctionName),
        makeRowVector(
            {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})}));

    std::vector<map_pairs_t<T, T>> expected;
    for (auto n = 1; n <= 10; n++) {
      expected.push_back({});
      auto& currentExpected = expected[expected.size() - 1];
      for (auto i = 0; i < n; i++) {
        switch (i % 6) {
          case 0:
            currentExpected.push_back({i, i + 1});
            break;
          case 1:
            currentExpected.push_back({i, std::nullopt});
            break;
          case 2:
            currentExpected.push_back({i, i + 2});
            break;
          case 3:
            currentExpected.push_back({i, std::nullopt});
            break;
          case 4:
            currentExpected.push_back({i, std::nullopt});
            break;
          case 5:
            currentExpected.push_back({i, i + 5});
            break;
        }
      }
    }

    assertEqualVectors(result, makeMapVector<T, T>(expected));
  }

  struct TestWriter {
    VectorPtr result;
    std::unique_ptr<exec::VectorWriter<Map<int64_t, int64_t>>> writer =
        std::make_unique<exec::VectorWriter<Map<int64_t, int64_t>>>();
  };

  TestWriter makeTestWriter() {
    TestWriter writer;

    writer.result =
        prepareResult(std::make_shared<MapType>(MapType(BIGINT(), BIGINT())));
    writer.writer->init(*writer.result->as<MapVector>());
    writer.writer->setOffset(0);
    return writer;
  }
};

TEST_F(MapWriterTest, addNull) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& mapWriter = vectorWriter->current();
  mapWriter.add_null() = 1;
  mapWriter.add_null() = 2;
  vectorWriter->commit();

  vectorWriter->finish();
  map_pairs_t<int64_t, int64_t> expected = {
      {1, std::nullopt}, {2, std::nullopt}};
  assertEqualVectors(result, makeMapVector<int64_t, int64_t>({expected}));
}

TEST_F(MapWriterTest, writeThenCommitNull) {
  auto [result, vectorWriter] = makeTestWriter();
  vectorWriter->ensureSize(2);

  {
    vectorWriter->setOffset(0);
    auto& mapWriter = vectorWriter->current();
    mapWriter.add_null() = 1;
    mapWriter.add_null() = 2;
    mapWriter.add_item() = std::make_tuple(100, 100);
    vectorWriter->commitNull();
  }

  {
    vectorWriter->setOffset(1);
    auto& mapWriter = vectorWriter->current();
    mapWriter.add_item() = std::make_tuple(200, 200);
    mapWriter.add_null() = 1;
    vectorWriter->commit();
  }
  vectorWriter->finish();

  map_pairs_t<int64_t, int64_t> expected = {{200, 200}, {1, std::nullopt}};
  auto expexctedVector = makeMapVector<int64_t, int64_t>({{}, expected});
  expexctedVector->setNull(0, true);
  assertEqualVectors(result, expexctedVector);
}

TEST_F(MapWriterTest, writeThenCommitNullNestedInRow) {
  using test_t = Row<Map<int64_t, int64_t>>;
  auto result = prepareResult(CppToType<test_t>::create(), 2);

  exec::VectorWriter<test_t> vectorWriter;
  vectorWriter.init(*result->as<RowVector>());
  {
    vectorWriter.setOffset(0);
    auto& mapWriter = vectorWriter.current().get_writer_at<0>();
    mapWriter.add_null() = 1;
    mapWriter.add_null() = 2;
    mapWriter.add_item() = std::make_tuple(100, 100);
    vectorWriter.commitNull();
  }

  {
    vectorWriter.setOffset(1);
    auto& mapWriter = vectorWriter.current().get_writer_at<0>();
    vectorWriter.commit();
  }
  vectorWriter.finish();
  DecodedVector decoded;
  SelectivityVector rows(2);
  decoded.decode(*result, rows);
  exec::VectorReader<test_t> reader(&decoded);
  auto rowView = reader[1];
  ASSERT_TRUE(rowView.at<0>().has_value());
  ASSERT_TRUE(rowView.at<0>().value().size() == 0);
}

TEST_F(MapWriterTest, addItem) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& mapWriter = vectorWriter->current();
  // Item 1.
  auto [key, value] = mapWriter.add_item();
  key = 1;
  value = 1;

  // Item 2.
  mapWriter.add_item() = std::make_tuple(1, 3);

  // Item 3.
  auto writers = mapWriter.add_item();
  std::get<0>(writers) = 11;
  std::get<1>(writers) = 12;

  vectorWriter->commit();
  vectorWriter->finish();

  map_pairs_t<int64_t, int64_t> expected = {{1, 1}, {1, 3}, {11, 12}};
  assertEqualVectors(result, makeMapVector<int64_t, int64_t>({expected}));
}

TEST_F(MapWriterTest, emplace) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& mapWriter = vectorWriter->current();

  mapWriter.emplace(1, 1);
  mapWriter.emplace(2, std::nullopt);
  mapWriter.emplace(3, std::optional<int64_t>{std::nullopt});
  mapWriter.emplace(4, std::optional<int64_t>{11});

  vectorWriter->commit();
  vectorWriter->finish();

  map_pairs_t<int64_t, int64_t> expected = {
      {1, 1}, {2, std::nullopt}, {3, std::nullopt}, {4, 11}};
  assertEqualVectors(result, makeMapVector<int64_t, int64_t>({expected}));
}

TEST_F(MapWriterTest, resizeAndSubscriptAccess) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& mapWriter = vectorWriter->current();

  mapWriter.resize(4);
  mapWriter[0] = std::make_tuple(1, 1);
  mapWriter[1] = std::make_tuple(2, std::nullopt);
  mapWriter[2] = std::make_tuple(3, std::optional<int64_t>{std::nullopt});
  mapWriter[3] = std::make_tuple(4, std::optional<int64_t>{11});

  mapWriter.resize(mapWriter.size() + 1);
  mapWriter[mapWriter.size() - 1] = std::make_tuple(6, std::nullopt);

  mapWriter.resize(mapWriter.size() + 1);
  mapWriter[mapWriter.size() - 1] = std::make_tuple(5, 6);

  vectorWriter->commit();
  vectorWriter->finish();

  map_pairs_t<int64_t, int64_t> expected = {
      {1, 1},
      {2, std::nullopt},
      {3, std::nullopt},
      {4, 11},
      {5, 6},
      {6, std::nullopt}};
  assertEqualVectors(result, makeMapVector<int64_t, int64_t>({expected}));
}

TEST_F(MapWriterTest, e2ePrimitives) {
  testE2E<int8_t>("map_writer_f_int8");
  testE2E<int16_t>("map_writer_f_int16");
  testE2E<int32_t>("map_writer_f_int32");
  testE2E<int64_t>("map_writer_f_int64");
  testE2E<float>("map_writer_f_float");
  testE2E<double>("map_writer_f_double");
  testE2E<bool>("map_writer_f_bool");
}

TEST_F(MapWriterTest, testTimeStamp) {
  auto result =
      prepareResult(std::make_shared<MapType>(MapType(BIGINT(), TIMESTAMP())));

  exec::VectorWriter<Map<int64_t, Timestamp>> writer;
  writer.init(*result->as<MapVector>());
  writer.setOffset(0);
  auto& mapWriter = writer.current();
  // General interface.
  mapWriter.add_item() = std::make_tuple(1, Timestamp::fromMillis(1));
  mapWriter.add_null() = 2;
  writer.commit();
  writer.finish();
  map_pairs_t<int64_t, Timestamp> expected = {
      {1, Timestamp::fromMillis(1)}, {2, std::nullopt}};
  assertEqualVectors(result, makeMapVector<int64_t, Timestamp>({expected}));
}

TEST_F(MapWriterTest, testVarChar) {
  auto result =
      prepareResult(std::make_shared<MapType>(MapType(VARCHAR(), VARCHAR())));

  exec::VectorWriter<Map<Varchar, Varchar>> writer;
  writer.init(*result->as<MapVector>());
  writer.setOffset(0);
  auto& mapWriter = writer.current();
  {
    auto [keyWriter, valueWriter] = mapWriter.add_item();
    keyWriter.resize(2);
    keyWriter.data()[0] = 'h';
    keyWriter.data()[1] = 'i';

    valueWriter += "welcome"_sv;
  }

  {
    auto& keyWriter = mapWriter.add_null();
    keyWriter += "null"_sv;
  }

  writer.commit();
  writer.finish();

  map_pairs_t<StringView, StringView> expected = {
      {"hi"_sv, "welcome"_sv}, {"null"_sv, std::nullopt}};
  assertEqualVectors(result, makeMapVector<StringView, StringView>({expected}));
}

TEST_F(MapWriterTest, testVarBinary) {
  auto result = prepareResult(
      std::make_shared<MapType>(MapType(VARBINARY(), VARBINARY())));

  exec::VectorWriter<Map<Varbinary, Varbinary>> writer;
  writer.init(*result->as<MapVector>());
  writer.setOffset(0);

  auto& mapWriter = writer.current();
  {
    auto [keyWriter, valueWriter] = mapWriter.add_item();
    keyWriter.resize(2);
    keyWriter.data()[0] = 'h';
    keyWriter.data()[1] = 'i';

    valueWriter += "welcome"_sv;
  }

  {
    auto& keyWriter = mapWriter.add_null();
    keyWriter += "null"_sv;
  }

  writer.commit();
  writer.finish();

  map_pairs_t<StringView, StringView> expected = {
      {"hi"_sv, "welcome"_sv}, {"null"_sv, std::nullopt}};

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(result->size());
  decoded.decode(*result, rows);
  exec::VectorReader<Map<Varbinary, Varbinary>> reader(&decoded);
  auto mapView = reader[0];

  ASSERT_EQ(mapView.size(), expected.size());

  auto i = 0;
  for (auto [key, value] : mapView) {
    ASSERT_EQ(key, expected[i].first);
    ASSERT_EQ(value, expected[i].second);
    i++;
  }
}

// A function that returns map<array<int>, map<int, int>> as output.
template <typename T>
struct MakeComplexMapFunction {
  template <typename TOut>
  bool call(TOut& out, const int64_t& n) {
    auto [arrayWriter, mapWriter] = out.add_item();
    arrayWriter.resize(n);
    for (auto i = 0; i < n; i++) {
      arrayWriter[i] = i;
    }
    for (auto i = 0; i < n; i++) {
      mapWriter.add_item() = std::make_tuple(i, i + 1);
    }
    return true;
  }
};

// Test a function that writes out map<array, map<>>.
TEST_F(MapWriterTest, nestedMap) {
  using out_t = Map<Array<int64_t>, Map<int64_t, int64_t>>;
  registerFunction<MakeComplexMapFunction, out_t, int64_t>({"complex_map"});

  auto result = evaluate(
      "complex_map(c0)",
      makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(result->size());
  decoded.decode(*result, rows);
  exec::VectorReader<Map<Array<int64_t>, Map<int64_t, int64_t>>> reader(
      &decoded);
  for (auto i = 0; i < rows.size(); i++) {
    auto outerMap = reader[i];
    for (const auto& [array, innerMap] : outerMap) {
      ASSERT_EQ(array.size(), i);
      // Check key.
      for (auto j = 0; j < i; j++) {
        ASSERT_EQ(array[j].value(), j);
      }

      // Check value.
      auto j = 0;
      for (const auto& [key, val] : innerMap.value()) {
        ASSERT_EQ(key, j);
        ASSERT_EQ(*val, j + 1);
        j++;
      }
    }
  }
}

std::unordered_map<int64_t, std::vector<int64_t>> makeCopyFromTestData() {
  std::unordered_map<int64_t, std::vector<int64_t>> data;
  for (int i = 0; i < 10; i++) {
    std::vector<int64_t> array;
    for (auto j = 0; j < i; j++) {
      array.push_back(j);
    }
    data.emplace(i, std::move(array));
  }
  return data;
}

template <typename T>
struct CopyFromTestFunc {
  template <typename TOut>
  bool call(TOut& out) {
    out.copy_from(makeCopyFromTestData());
    return true;
  }
};

TEST_F(MapWriterTest, copyFrom) {
  auto [result, vectorWriter] = makeTestWriter();

  auto& mapWriter = vectorWriter->current();
  // Item 1.
  std::unordered_map<int64_t, int64_t> data = {{1, 2}, {1, 3}, {11, 12}};
  mapWriter.copy_from(data);
  vectorWriter->commit();
  vectorWriter->finish();

  map_pairs_t<int64_t, int64_t> expected;
  for (auto item : data) {
    expected.push_back(item);
  }
  assertEqualVectors(result, makeMapVector<int64_t, int64_t>({expected}));
}

// Test copy_from e2e on Map<int64_t, Array<int64_t>>
TEST_F(MapWriterTest, copyFromE2E) {
  registerFunction<CopyFromTestFunc, Map<int64_t, Array<int64_t>>>(
      {"f_copy_from_e2e"});

  auto result = evaluate(
      "f_copy_from_e2e()", makeRowVector({makeFlatVector<int64_t>(1)}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Map<int64_t, Array<int64_t>>> reader(&decoded);

  auto referenceData = makeCopyFromTestData();
  auto mapView = reader[0];

  ASSERT_EQ(mapView.size(), referenceData.size());

  for (auto [key, value] : mapView) {
    auto arrayView = *value;
    auto& arrayRef = referenceData[key];
    ASSERT_EQ(arrayView.size(), arrayRef.size());
    for (int i = 0; i < arrayView.size(); i++) {
      ASSERT_EQ(arrayRef[i], arrayView[i].value());
    }
  }
}

template <typename T>
struct CopyFromNullFreeInputFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TOut, typename TIn>
  void callNullFree(TOut& out, const TIn& input) {
    out.copy_from(input);
  }
};

TEST_F(MapWriterTest, copyFromNullFreeMapView) {
  registerFunction<
      CopyFromNullFreeInputFunc,
      Map<int64_t, int64_t>,
      Map<int64_t, int64_t>>({"copy_from_null_free_map_view"});

  auto result = evaluate(
      "copy_from_null_free_map_view(map(array_constructor(1,2,3),array_constructor(4,5,6)))",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Map<int64_t, int64_t>> reader(&decoded);

  auto mapView = reader.readNullFree(0);

  ASSERT_EQ(
      mapView.materialize(),
      (folly::F14FastMap<int64_t, int64_t>{{1, 4}, {2, 5}, {3, 6}}));
}

template <typename T>
struct CopyFromNullableInputFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TOut, typename TIn>
  void call(TOut& out, const TIn& input) {
    out.copy_from(input);
  }
};

TEST_F(MapWriterTest, copyFromNullableMapView) {
  registerFunction<
      CopyFromNullableInputFunc,
      Map<int64_t, int64_t>,
      Map<int64_t, int64_t>>({"copy_from_nullable_map_view"});

  auto result = evaluate(
      "copy_from_nullable_map_view(map(array_constructor(1,2,3),array_constructor(4,null,6)))",
      makeRowVector({makeFlatVector<int64_t>(1)}));

  // Test results.
  DecodedVector decoded;
  SelectivityVector rows(1);
  decoded.decode(*result, rows);
  exec::VectorReader<Map<int64_t, int64_t>> reader(&decoded);

  auto mapView = reader[0];

  ASSERT_EQ(
      mapView.materialize(),
      (folly::F14FastMap<int64_t, std::optional<int64_t>>{
          {1, 4}, {2, std::nullopt}, {3, 6}}));
}

// Make sure nested vectors are resized to actual size after writing.
TEST_F(MapWriterTest, finishPostSize) {
  using out_t = Map<int64_t, Map<int64_t, int64_t>>;
  auto result = prepareResult(CppToType<out_t>::create());

  exec::VectorWriter<out_t> vectorWriter;
  vectorWriter.init(*result->as<MapVector>());
  vectorWriter.setOffset(0);

  auto& mapWriter = vectorWriter.current();
  folly::F14FastMap<int64_t, int64_t> element = {{1, 2}, {3, 4}};

  mapWriter.copy_from(
      folly::F14FastMap<int64_t, folly::F14FastMap<int64_t, int64_t>>{
          {1, element}, {2, element}, {3, element}});

  vectorWriter.commit();
  vectorWriter.finish();

  auto* outerMap = result->as<MapVector>();
  ASSERT_EQ(outerMap->mapKeys()->size(), 3);
  ASSERT_EQ(outerMap->mapValues()->size(), 3);

  auto* innerMap = outerMap->mapValues()->as<MapVector>();
  ASSERT_EQ(innerMap->mapKeys()->size(), 6);
  ASSERT_EQ(innerMap->mapValues()->size(), 6);
}

} // namespace
} // namespace facebook::velox

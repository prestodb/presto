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

#include <cstdint>

#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/tests/CastBaseTest.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;

namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();

template <typename T>
using TwoDimVector = std::vector<std::vector<std::optional<T>>>;

} // namespace

class JsonCastTest : public functions::test::CastBaseTest {
 protected:
  // Populates offsets and sizes buffers for making array and map vectors.
  // Every row has offsetEvery number of elements except the last row.
  void makeOffsetsAndSizes(
      int numOfElements,
      int offsetEvery,
      BufferPtr& offsets,
      BufferPtr& sizes) {
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();

    int i = 0;
    while (i < numOfElements) {
      rawOffsets[i / offsetEvery] = i;
      rawSizes[i / offsetEvery] =
          i + offsetEvery > numOfElements ? numOfElements - i : offsetEvery;

      i += offsetEvery;
    }
  }

  // Makes a flat vector wrapped in reversed indices.
  template <typename T>
  VectorPtr makeDictionaryVector(const std::vector<std::optional<T>>& data) {
    auto vector = makeNullableFlatVector<T>(data);
    auto reversedIndices = makeIndicesInReverse(data.size());
    return wrapInDictionary(reversedIndices, data.size(), vector);
  }

  // Makes an array vector whose elements vector is wrapped in a dictionary
  // that reverses all elements. Each row of the array vector contains arraySize
  // number of elements except the last row.
  template <typename T>
  ArrayVectorPtr makeArrayWithDictionaryElements(
      const std::vector<std::optional<T>>& elements,
      int arraySize) {
    int size = elements.size();
    int numOfArray = (size + arraySize - 1) / arraySize;
    auto dictElements = makeDictionaryVector(elements);

    BufferPtr offsets =
        AlignedBuffer::allocate<vector_size_t>(numOfArray, pool());
    BufferPtr sizes =
        AlignedBuffer::allocate<vector_size_t>(numOfArray, pool());
    makeOffsetsAndSizes(size, arraySize, offsets, sizes);

    return std::make_shared<ArrayVector>(
        pool(),
        ARRAY(CppToType<T>::create()),
        BufferPtr(nullptr),
        numOfArray,
        offsets,
        sizes,
        dictElements,
        0);
  }

  // Makes a map vector whose keys and values vectors are wrapped in a
  // dictionary that reverses all elements. Each row of the map vector contains
  // mapSize number of keys and values except the last row.
  template <typename TKey, typename TValue>
  MapVectorPtr makeMapWithDictionaryElements(
      const std::vector<std::optional<TKey>>& keys,
      const std::vector<std::optional<TValue>>& values,
      int mapSize) {
    VELOX_CHECK_EQ(
        keys.size(),
        values.size(),
        "keys and values must have the same number of elements.");

    int size = keys.size();
    int numOfMap = (size + mapSize - 1) / mapSize;
    auto dictKeys = makeDictionaryVector(keys);
    auto dictValues = makeDictionaryVector(values);

    BufferPtr offsets =
        AlignedBuffer::allocate<vector_size_t>(numOfMap, pool());
    BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(numOfMap, pool());
    makeOffsetsAndSizes(size, mapSize, offsets, sizes);

    return std::make_shared<MapVector>(
        pool(),
        MAP(CppToType<TKey>::create(), CppToType<TValue>::create()),
        BufferPtr(nullptr),
        numOfMap,
        offsets,
        sizes,
        dictKeys,
        dictValues,
        0);
  }

  // Makes a row vector whose children vectors are wrapped in a dictionary
  // that reverses all elements.
  template <typename T>
  RowVectorPtr makeRowWithDictionaryElements(
      const TwoDimVector<T>& elements,
      const TypePtr& rowType) {
    VELOX_CHECK_NE(elements.size(), 0, "At least one child must be provided.");

    int childrenSize = elements.size();
    int size = elements[0].size();

    std::vector<VectorPtr> dictChildren;
    for (int i = 0; i < childrenSize; ++i) {
      VELOX_CHECK_EQ(
          elements[i].size(),
          size,
          "All children vectors must have the same size.");
      dictChildren.push_back(makeDictionaryVector(elements[i]));
    }

    return std::make_shared<RowVector>(
        pool(), rowType, BufferPtr(nullptr), size, dictChildren);
  }
};

TEST_F(JsonCastTest, fromInteger) {
  testCast<int64_t, Json>(
      BIGINT(),
      JSON(),
      {1, -3, 0, INT64_MAX, INT64_MIN, std::nullopt},
      {"1"_sv,
       "-3"_sv,
       "0"_sv,
       "9223372036854775807"_sv,
       "-9223372036854775808"_sv,
       std::nullopt});
  testCast<int8_t, Json>(
      TINYINT(),
      JSON(),
      {1, -3, 0, INT8_MAX, INT8_MIN, std::nullopt},
      {"1"_sv, "-3"_sv, "0"_sv, "127"_sv, "-128"_sv, std::nullopt});
  testCast<int32_t, Json>(
      INTEGER(),
      JSON(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(JsonCastTest, fromVarchar) {
  testCast<StringView, Json>(
      VARCHAR(),
      JSON(),
      {"aaa"_sv, "bbb"_sv, "ccc"_sv},
      {R"("aaa")"_sv, R"("bbb")"_sv, R"("ccc")"_sv});
  testCast<StringView, Json>(
      VARCHAR(),
      JSON(),
      {""_sv,
       std::nullopt,
       "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\"\\ ."_sv},
      {"\"\""_sv,
       std::nullopt,
       R"("\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000b\f\r\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f\"\\ .")"_sv});
  testCast<StringView, Json>(
      VARCHAR(),
      JSON(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(JsonCastTest, fromBoolean) {
  testCast<bool, Json>(
      BOOLEAN(),
      JSON(),
      {true, false, false, std::nullopt},
      {"true"_sv, "false"_sv, "false"_sv, std::nullopt});
  testCast<bool, Json>(
      BOOLEAN(),
      JSON(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(JsonCastTest, fromDouble) {
  testCast<double, Json>(
      DOUBLE(),
      JSON(),
      {1.1, 2.0001, 10.0, 3.14e0, kNan, kInf, -kInf, std::nullopt},
      {"1.1"_sv,
       "2.0001"_sv,
       "10"_sv,
       "3.14"_sv,
       "NaN"_sv,
       "Infinity"_sv,
       "-Infinity"_sv,
       std::nullopt});
  testCast<double, Json>(
      DOUBLE(),
      JSON(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(JsonCastTest, fromDate) {
  testCast<Date, Json>(
      DATE(),
      JSON(),
      {0, 1000, -10000, std::nullopt},
      {"1970-01-01"_sv, "1972-09-27"_sv, "1942-08-16"_sv, std::nullopt});
  testCast<Date, Json>(
      DATE(),
      JSON(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(JsonCastTest, fromTimestamp) {
  testCast<Timestamp, Json>(
      TIMESTAMP(),
      JSON(),
      {Timestamp{0, 0},
       Timestamp{10000000, 0},
       Timestamp{-1, 9000},
       std::nullopt},
      {"1970-01-01T00:00:00.000000000"_sv,
       "1970-04-26T17:46:40.000000000"_sv,
       "1969-12-31T23:59:59.000009000"_sv,
       std::nullopt});
  testCast<Timestamp, Json>(
      TIMESTAMP(),
      JSON(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(JsonCastTest, fromArray) {
  TwoDimVector<StringView> array{
      {"red"_sv, "blue"_sv}, {std::nullopt, std::nullopt, "purple"_sv}, {}};
  std::vector<std::optional<Json>> expected{
      R"(["red","blue"])", R"([null,null,"purple"])", "[]"};

  auto arrayVector = makeNullableArrayVector<StringView>(array);
  auto expectedVector = makeNullableFlatVector<Json>(expected);

  testCast<Json>(ARRAY(VARCHAR()), JSON(), arrayVector, expectedVector);

  // Tests array whose elements are wrapped in a dictionary.
  std::vector<std::optional<int64_t>> elements{1, -2, 3, -4, 5, -6, 7};
  auto arrayOfDictElements = makeArrayWithDictionaryElements(elements, 2);
  auto arrayOfDictElementsExpected =
      makeNullableFlatVector<Json>({"[7,-6]", "[5,-4]", "[3,-2]", "[1]"});

  testCast<Json>(
      ARRAY(BIGINT()),
      JSON(),
      arrayOfDictElements,
      arrayOfDictElementsExpected);

  // Tests array vector with nulls at all rows.
  auto allNullArray = vectorMaker_.allNullArrayVector(5, BIGINT());
  auto allNullExpected = makeNullableFlatVector<Json>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testCast<Json>(ARRAY(BIGINT()), JSON(), allNullArray, allNullExpected);
}

TEST_F(JsonCastTest, fromMap) {
  using P = std::pair<StringView, std::optional<int64_t>>;

  std::vector<P> a{{"blue"_sv, 1}, {"red"_sv, 2}};
  std::vector<P> b{{"purple", std::nullopt}, {"orange"_sv, -2}};
  std::vector<std::vector<P>> map{a, b, {}};

  std::vector<std::optional<Json>> expected{
      R"({"blue":1,"red":2})", R"({"orange":-2,"purple":null})", "{}"};

  auto mapVector = makeMapVector<StringView, int64_t>(map);
  auto expectedVector = makeNullableFlatVector<Json>(expected);

  testCast<Json>(MAP(VARCHAR(), BIGINT()), JSON(), mapVector, expectedVector);

  // Tests map whose elements are wrapped in a dictionary.
  std::vector<std::optional<StringView>> keys{
      "a"_sv, "b"_sv, "c"_sv, "d"_sv, "e"_sv, "f"_sv, "g"_sv};
  std::vector<std::optional<double>> values{
      1.1e3, 2.2, 3.14e0, -4.4, std::nullopt, -6e-10, -7.7};
  auto mapOfDictElements = makeMapWithDictionaryElements(keys, values, 2);

  auto mapOfDictElementsExpected = makeNullableFlatVector<Json>(
      {R"({"f":-6E-10,"g":-7.7})",
       R"({"d":-4.4,"e":null})",
       R"({"b":2.2,"c":3.14})",
       R"({"a":1100})"});

  testCast<Json>(
      MAP(VARCHAR(), BIGINT()),
      JSON(),
      mapOfDictElements,
      mapOfDictElementsExpected);

  // Tests map vector with nulls at all rows.
  auto allNullMap = vectorMaker_.allNullMapVector(5, VARCHAR(), BIGINT());
  auto allNullExpected = makeNullableFlatVector<Json>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testCast<Json>(MAP(VARCHAR(), BIGINT()), JSON(), allNullMap, allNullExpected);
}

TEST_F(JsonCastTest, fromRow) {
  auto firstChild =
      makeNullableFlatVector<int64_t>({std::nullopt, 2, 3, std::nullopt, 5});
  auto secondChild = makeNullableFlatVector<StringView>(
      {"red", std::nullopt, "blue", std::nullopt, "yellow"});
  auto thirdChild = makeNullableFlatVector<double>(
      {1.1, 2.2, std::nullopt, std::nullopt, 5.5});

  std::vector<std::optional<Json>> expected{
      R"([null,"red",1.1])",
      R"([2,null,2.2])",
      R"([3,"blue",null])",
      R"([null,null,null])",
      R"([5,"yellow",5.5])"};

  auto rowVector = makeRowVector({firstChild, secondChild, thirdChild});
  auto expectedVector = makeNullableFlatVector<Json>(expected);

  testCast<Json>(
      ROW({BIGINT(), VARCHAR(), DOUBLE()}), JSON(), rowVector, expectedVector);

  // Tests row whose children are wrapped in dictionaries.
  auto rowOfDictElements = makeRowWithDictionaryElements<int64_t>(
      {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}, ROW({BIGINT(), BIGINT(), BIGINT()}));
  auto rowOfDictElementsExpected =
      makeNullableFlatVector<Json>({"[3,6,9]", "[2,5,8]", "[1,4,7]"});

  testCast<Json>(
      ROW({BIGINT(), BIGINT(), BIGINT()}),
      JSON(),
      rowOfDictElements,
      rowOfDictElementsExpected);

  // Tests row vector with nulls at all rows.
  auto allNullChild = vectorMaker_.allNullFlatVector<int64_t>(5);
  auto nulls = AlignedBuffer::allocate<bool>(5, pool());
  auto rawNulls = nulls->asMutable<uint64_t>();
  bits::fillBits(rawNulls, 0, 5, false);

  auto allNullRow = std::make_shared<RowVector>(
      pool(), ROW({BIGINT()}), nulls, 5, std::vector<VectorPtr>{allNullChild});
  auto allNullExpected = makeNullableFlatVector<Json>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testCast<Json>(ROW({BIGINT()}), JSON(), allNullRow, allNullExpected);
}

TEST_F(JsonCastTest, fromNested) {
  // Create map of array vector.
  auto keyVector = makeFlatVector<StringView>(
      {"blue"_sv, "red"_sv, "green"_sv, "yellow"_sv, "purple"_sv, "orange"_sv});
  auto valueVector = makeNullableArrayVector<int64_t>(
      {{1, 2},
       {std::nullopt, 4},
       {std::nullopt, std::nullopt},
       {7, 8},
       {9, std::nullopt},
       {11, 12}});

  auto offsets = AlignedBuffer::allocate<vector_size_t>(3, pool());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(3, pool());
  makeOffsetsAndSizes(6, 2, offsets, sizes);

  auto nulls = AlignedBuffer::allocate<bool>(3, pool());
  auto rawNulls = nulls->asMutable<uint64_t>();
  bits::setNull(rawNulls, 0, false);
  bits::setNull(rawNulls, 1, true);
  bits::setNull(rawNulls, 2, false);

  auto mapVector = std::make_shared<MapVector>(
      pool(),
      MAP(VARCHAR(), ARRAY(BIGINT())),
      nulls,
      3,
      offsets,
      sizes,
      keyVector,
      valueVector,
      0);

  // Create array of map vector
  using Pair = std::pair<StringView, std::optional<int64_t>>;

  std::vector<Pair> a{Pair{"blue"_sv, 1}, Pair{"red"_sv, 2}};
  std::vector<Pair> b{Pair{"green"_sv, std::nullopt}};
  std::vector<Pair> c{Pair{"yellow"_sv, 4}, Pair{"purple"_sv, 5}};
  std::vector<std::vector<std::vector<Pair>>> data{{a, b}, {b}, {c, a}};

  auto arrayVector = makeArrayOfMapVector<StringView, int64_t>(data);

  // Create row vector of array of map and map of array
  auto rowVector = makeRowVector({mapVector, arrayVector});

  std::vector<std::optional<Json>> expected{
      R"([{"blue":[1,2],"red":[null,4]},[{"blue":1,"red":2},{"green":null}]])",
      R"([null,[{"green":null}]])",
      R"([{"orange":[11,12],"purple":[9,null]},[{"purple":5,"yellow":4},{"blue":1,"red":2}]])"};
  auto expectedVector = makeNullableFlatVector<Json>(expected);

  testCast<Json>(
      ROW({MAP(VARCHAR(), ARRAY(BIGINT())), ARRAY(MAP(VARCHAR(), BIGINT()))}),
      JSON(),
      rowVector,
      expectedVector);
}

TEST_F(JsonCastTest, unsupportedTypes) {
  // Map keys must be varchar.
  auto invalidTypeMap = makeMapVector<int64_t, int64_t>({{}});
  auto invalidTypeMapExpected = makeNullableFlatVector<Json>({"{}"});
  EXPECT_THROW(
      evaluateCast<Json>(
          MAP(BIGINT(), BIGINT()),
          JSON(),
          makeRowVector({invalidTypeMap}),
          invalidTypeMapExpected),
      VeloxException);

  // All children of row must be of supported types.
  auto invalidTypeRow = makeRowVector({invalidTypeMap});
  auto invalidTypeRowExpected = makeNullableFlatVector<Json>({"[{}]"});
  EXPECT_THROW(
      evaluateCast<Json>(
          ROW({MAP(BIGINT(), BIGINT())}),
          JSON(),
          makeRowVector({invalidTypeRow}),
          invalidTypeRowExpected),
      VeloxException);

  // Map keys must not be null.
  auto keyVector = makeNullableFlatVector<StringView>({"red"_sv, std::nullopt});
  auto valueVector = makeNullableFlatVector<int64_t>({1, 2});

  auto offsets = AlignedBuffer::allocate<vector_size_t>(1, pool());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(1, pool());
  makeOffsetsAndSizes(2, 2, offsets, sizes);

  auto invalidKeyMap = std::make_shared<MapVector>(
      pool(),
      MAP(VARCHAR(), BIGINT()),
      BufferPtr(nullptr),
      1,
      offsets,
      sizes,
      keyVector,
      valueVector,
      0);
  auto invalidKeyMapExpected =
      makeNullableFlatVector<Json>({R"({"red":1,null:2})"});

  EXPECT_THROW(
      evaluateCast<Json>(
          MAP(VARCHAR(), BIGINT()),
          JSON(),
          makeRowVector({invalidKeyMap}),
          invalidKeyMapExpected),
      VeloxException);
}

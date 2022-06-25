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

#include "velox/common/base/BitUtil.h"
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

template <typename TKey, typename TValue>
using Pair = std::pair<TKey, std::optional<TValue>>;

} // namespace

class JsonCastTest : public functions::test::CastBaseTest {
 protected:
  template <typename T>
  void testCastFromArray(
      const TypePtr& fromType,
      const TwoDimVector<T>& input,
      const std::vector<std::optional<Json>>& expected) {
    auto arrayVector = makeNullableArrayVector<T>(input, fromType);
    auto expectedVector = makeNullableFlatVector<Json>(expected);

    testCast<Json>(fromType, JSON(), arrayVector, expectedVector);
  }

  template <typename TKey, typename TValue>
  void testCastFromMap(
      const TypePtr& fromType,
      const std::vector<std::vector<Pair<TKey, TValue>>>& input,
      const std::vector<std::optional<Json>>& expected) {
    auto mapVector = makeMapVector<TKey, TValue>(input, fromType);
    auto expectedVector = makeNullableFlatVector<Json>(expected);

    testCast<Json>(fromType, JSON(), mapVector, expectedVector);
  }

  template <typename TChild1, typename TChild2, typename TChild3>
  void testCastFromRow(
      const TypePtr& fromType,
      const std::vector<std::optional<TChild1>>& child1,
      const std::vector<std::optional<TChild2>>& child2,
      const std::vector<std::optional<TChild3>>& child3,
      const std::vector<std::optional<Json>>& expected) {
    auto firstChild =
        makeNullableFlatVector<TChild1>(child1, fromType->childAt(0));
    auto secondChild =
        makeNullableFlatVector<TChild2>(child2, fromType->childAt(1));
    auto thirdChild =
        makeNullableFlatVector<TChild3>(child3, fromType->childAt(2));

    auto rowVector = makeRowVector({firstChild, secondChild, thirdChild});
    auto expectedVector = makeNullableFlatVector<Json>(expected);

    testCast<Json>(fromType, JSON(), rowVector, expectedVector);
  }

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

  // Makes a flat vector wrapped in reversed indices. If isKey is false, also
  // makes the first row to be null.
  template <typename T>
  VectorPtr makeDictionaryVector(
      const std::vector<std::optional<T>>& data,
      const TypePtr& type = CppToType<T>::create(),
      bool isKey = false) {
    VectorPtr vector;
    if constexpr (std::is_same_v<T, UnknownValue>) {
      vector = makeFlatUnknownVector(data.size());
    } else {
      vector = makeNullableFlatVector<T>(data, type);
    }

    auto reversedIndices = makeIndicesInReverse(data.size());

    auto nulls = AlignedBuffer::allocate<bool>(data.size(), pool());
    auto rawNulls = nulls->template asMutable<uint64_t>();
    bits::fillBits(rawNulls, 0, data.size(), true);
    bits::setBit(rawNulls, 0, false);

    if (!isKey) {
      return BaseVector::wrapInDictionary(
          nulls, reversedIndices, data.size(), vector);
    } else {
      return BaseVector::wrapInDictionary(
          nullptr, reversedIndices, data.size(), vector);
    }
  }

  // Makes an array vector whose elements vector is wrapped in a dictionary
  // that reverses all elements and first element is null. Each row of the array
  // vector contains arraySize number of elements except the last row.
  template <typename T>
  ArrayVectorPtr makeArrayWithDictionaryElements(
      const std::vector<std::optional<T>>& elements,
      int arraySize,
      const TypePtr& type = ARRAY(CppToType<T>::create())) {
    int size = elements.size();
    int numOfArray = (size + arraySize - 1) / arraySize;
    auto dictElements = makeDictionaryVector(elements, type->childAt(0));

    BufferPtr offsets =
        AlignedBuffer::allocate<vector_size_t>(numOfArray, pool());
    BufferPtr sizes =
        AlignedBuffer::allocate<vector_size_t>(numOfArray, pool());
    makeOffsetsAndSizes(size, arraySize, offsets, sizes);

    return std::make_shared<ArrayVector>(
        pool(),
        type,
        BufferPtr(nullptr),
        numOfArray,
        offsets,
        sizes,
        dictElements,
        0);
  }

  // Makes a map vector whose keys and values vectors are wrapped in a
  // dictionary that reverses all elements and first value is null. Each row of
  // the map vector contains mapSize number of keys and values except the last
  // row.
  template <typename TKey, typename TValue>
  MapVectorPtr makeMapWithDictionaryElements(
      const std::vector<std::optional<TKey>>& keys,
      const std::vector<std::optional<TValue>>& values,
      int mapSize,
      const TypePtr& type =
          MAP(CppToType<TKey>::create(), CppToType<TValue>::create())) {
    VELOX_CHECK_EQ(
        keys.size(),
        values.size(),
        "keys and values must have the same number of elements.");

    int size = keys.size();
    int numOfMap = (size + mapSize - 1) / mapSize;
    auto dictKeys = makeDictionaryVector(keys, type->childAt(0), true);
    auto dictValues = makeDictionaryVector(values, type->childAt(1));

    BufferPtr offsets =
        AlignedBuffer::allocate<vector_size_t>(numOfMap, pool());
    BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(numOfMap, pool());
    makeOffsetsAndSizes(size, mapSize, offsets, sizes);

    return std::make_shared<MapVector>(
        pool(),
        type,
        BufferPtr(nullptr),
        numOfMap,
        offsets,
        sizes,
        dictKeys,
        dictValues,
        0);
  }

  // Makes a row vector whose children vectors are wrapped in a dictionary
  // that reverses all elements and elements at the first row are null.
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
      dictChildren.push_back(
          makeDictionaryVector(elements[i], rowType->childAt(i)));
    }

    return std::make_shared<RowVector>(
        pool(), rowType, BufferPtr(nullptr), size, dictChildren);
  }

  VectorPtr makeFlatUnknownVector(int size) {
    auto vector = std::dynamic_pointer_cast<FlatVector<UnknownValue>>(
        BaseVector::create(UNKNOWN(), size, pool()));
    for (int i = 0; i < size; ++i) {
      vector->setNull(i, true);
    }

    return vector;
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

TEST_F(JsonCastTest, fromUnknown) {
  auto input = makeFlatUnknownVector(3);
  auto expected =
      makeNullableFlatVector<Json>({std::nullopt, std::nullopt, std::nullopt});
  evaluateAndVerify<Json>(UNKNOWN(), JSON(), makeRowVector({input}), expected);
}

TEST_F(JsonCastTest, fromArray) {
  TwoDimVector<StringView> array{
      {"red"_sv, "blue"_sv}, {std::nullopt, std::nullopt, "purple"_sv}, {}};
  std::vector<std::optional<Json>> expected{
      R"(["red","blue"])", R"([null,null,"purple"])", "[]"};

  // Tests array of json elements.
  std::vector<std::optional<Json>> expectedJsonArray{
      "[red,blue]", "[null,null,purple]", "[]"};
  testCastFromArray(ARRAY(JSON()), array, expectedJsonArray);

  // Tests array whose elements are of unknown type.
  auto arrayOfUnknownElements = makeArrayWithDictionaryElements<UnknownValue>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      2,
      ARRAY(UNKNOWN()));
  auto arrayOfUnknownElementsExpected =
      makeNullableFlatVector<Json>({"[null,null]", "[null,null]"});
  testCast<Json>(
      ARRAY(UNKNOWN()),
      JSON(),
      arrayOfUnknownElements,
      arrayOfUnknownElementsExpected);

  // Tests array whose elements are wrapped in a dictionary.
  auto arrayOfDictElements =
      makeArrayWithDictionaryElements<int64_t>({1, -2, 3, -4, 5, -6, 7}, 2);
  auto arrayOfDictElementsExpected =
      makeNullableFlatVector<Json>({"[null,-6]", "[5,-4]", "[3,-2]", "[1]"});
  testCast<Json>(
      ARRAY(BIGINT()),
      JSON(),
      arrayOfDictElements,
      arrayOfDictElementsExpected);

  // Tests array whose elements are json and wrapped in a dictionary.
  auto jsonArrayOfDictElements = makeArrayWithDictionaryElements<Json>(
      {"a"_sv, "b"_sv, "c"_sv, "d"_sv, "e"_sv, "f"_sv, "g"_sv},
      2,
      ARRAY(JSON()));
  auto jsonArrayOfDictElementsExpected =
      makeNullableFlatVector<Json>({"[null,f]", "[e,d]", "[c,b]", "[a]"});
  testCast<Json>(
      ARRAY(JSON()),
      JSON(),
      jsonArrayOfDictElements,
      jsonArrayOfDictElementsExpected);

  // Tests array vector with nulls at all rows.
  auto allNullArray = vectorMaker_.allNullArrayVector(5, BIGINT());
  auto allNullExpected = makeNullableFlatVector<Json>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testCast<Json>(ARRAY(BIGINT()), JSON(), allNullArray, allNullExpected);
}

TEST_F(JsonCastTest, fromMap) {
  // Tests map with string keys.
  std::vector<std::vector<Pair<StringView, int64_t>>> mapStringKey{
      {{"blue"_sv, 1}, {"red"_sv, 2}},
      {{"purple", std::nullopt}, {"orange"_sv, -2}},
      {}};
  std::vector<std::optional<Json>> expectedStringKey{
      R"({"blue":1,"red":2})", R"({"orange":-2,"purple":null})", "{}"};
  testCastFromMap(MAP(VARCHAR(), BIGINT()), mapStringKey, expectedStringKey);

  // Tests map with integer keys.
  std::vector<std::vector<Pair<int16_t, int64_t>>> mapIntKey{
      {{3, std::nullopt}, {4, 2}}, {}};
  std::vector<std::optional<Json>> expectedIntKey{R"({"3":null,"4":2})", "{}"};
  testCastFromMap(MAP(SMALLINT(), BIGINT()), mapIntKey, expectedIntKey);

  // Tests map with floating-point keys.
  std::vector<std::vector<Pair<double, int64_t>>> mapDoubleKey{
      {{4.4, std::nullopt}, {3.3, 2}}, {}};
  std::vector<std::optional<Json>> expectedDoubleKey{
      R"({"3.3":2,"4.4":null})", "{}"};
  testCastFromMap(MAP(DOUBLE(), BIGINT()), mapDoubleKey, expectedDoubleKey);

  // Tests map with boolean keys.
  std::vector<std::vector<Pair<bool, int64_t>>> mapBoolKey{
      {{true, std::nullopt}, {false, 2}}, {}};
  std::vector<std::optional<Json>> expectedBoolKey{
      R"({"false":2,"true":null})", "{}"};
  testCastFromMap(MAP(BOOLEAN(), BIGINT()), mapBoolKey, expectedBoolKey);

  // Tests map whose values are of unknown type.
  std::vector<std::optional<StringView>> keys{
      "a"_sv, "b"_sv, "c"_sv, "d"_sv, "e"_sv, "f"_sv, "g"_sv};
  std::vector<std::optional<UnknownValue>> unknownValues{
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt};
  auto mapOfUnknownValues =
      makeMapWithDictionaryElements<StringView, UnknownValue>(
          keys, unknownValues, 2, MAP(VARCHAR(), UNKNOWN()));

  auto mapOfUnknownValuesExpected = makeNullableFlatVector<Json>(
      {R"({"f":null,"g":null})",
       R"({"d":null,"e":null})",
       R"({"b":null,"c":null})",
       R"({"a":null})"});

  testCast<Json>(
      MAP(VARCHAR(), UNKNOWN()),
      JSON(),
      mapOfUnknownValues,
      mapOfUnknownValuesExpected);

  // Tests map whose elements are wrapped in a dictionary.
  std::vector<std::optional<double>> values{
      1.1e3, 2.2, 3.14e0, -4.4, std::nullopt, -6e-10, -7.7};
  auto mapOfDictElements = makeMapWithDictionaryElements(keys, values, 2);

  auto mapOfDictElementsExpected = makeNullableFlatVector<Json>(
      {R"({"f":-6E-10,"g":null})",
       R"({"d":-4.4,"e":null})",
       R"({"b":2.2,"c":3.14})",
       R"({"a":1100})"});
  testCast<Json>(
      MAP(VARCHAR(), DOUBLE()),
      JSON(),
      mapOfDictElements,
      mapOfDictElementsExpected);

  // Tests map whose elements are json and wrapped in a dictionary.
  auto jsonMapOfDictElements =
      makeMapWithDictionaryElements(keys, values, 2, MAP(JSON(), DOUBLE()));
  auto jsonMapOfDictElementsExpected = makeNullableFlatVector<Json>(
      {"{f:-6E-10,g:null}", "{d:-4.4,e:null}", "{b:2.2,c:3.14}", "{a:1100}"});
  testCast<Json>(
      MAP(JSON(), DOUBLE()),
      JSON(),
      jsonMapOfDictElements,
      jsonMapOfDictElementsExpected);

  // Tests map vector with nulls at all rows.
  auto allNullMap = vectorMaker_.allNullMapVector(5, VARCHAR(), BIGINT());
  auto allNullExpected = makeNullableFlatVector<Json>(
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testCast<Json>(MAP(VARCHAR(), BIGINT()), JSON(), allNullMap, allNullExpected);
}

TEST_F(JsonCastTest, fromRow) {
  std::vector<std::optional<int64_t>> child1{
      std::nullopt, 2, 3, std::nullopt, 5};
  std::vector<std::optional<StringView>> child2{
      "red"_sv, std::nullopt, "blue"_sv, std::nullopt, "yellow"_sv};
  std::vector<std::optional<double>> child3{
      1.1, 2.2, std::nullopt, std::nullopt, 5.5};
  std::vector<std::optional<Json>> expected{
      R"([null,"red",1.1])",
      R"([2,null,2.2])",
      R"([3,"blue",null])",
      R"([null,null,null])",
      R"([5,"yellow",5.5])"};
  testCastFromRow<int64_t, StringView, double>(
      ROW({BIGINT(), VARCHAR(), DOUBLE()}), child1, child2, child3, expected);

  // Tests row with json child column.
  std::vector<std::optional<Json>> expectedJsonChild{
      R"([null,red,1.1])",
      R"([2,null,2.2])",
      R"([3,blue,null])",
      R"([null,null,null])",
      R"([5,yellow,5.5])"};
  testCastFromRow<int64_t, StringView, double>(
      ROW({BIGINT(), JSON(), DOUBLE()}),
      child1,
      child2,
      child3,
      expectedJsonChild);

  // Tests row whose children are of unknown type.
  auto rowOfUnknownChildren = makeRowWithDictionaryElements<UnknownValue>(
      {{std::nullopt, std::nullopt}, {std::nullopt, std::nullopt}},
      ROW({UNKNOWN(), UNKNOWN()}));
  auto rowOfUnknownChildrenExpected =
      makeNullableFlatVector<Json>({"[null,null]", "[null,null]"});

  testCast<Json>(
      ROW({UNKNOWN(), UNKNOWN()}),
      JSON(),
      rowOfUnknownChildren,
      rowOfUnknownChildrenExpected);

  // Tests row whose children are wrapped in dictionaries.
  auto rowOfDictElements = makeRowWithDictionaryElements<int64_t>(
      {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}, ROW({BIGINT(), BIGINT(), BIGINT()}));
  auto rowOfDictElementsExpected =
      makeNullableFlatVector<Json>({"[null,null,null]", "[2,5,8]", "[1,4,7]"});
  testCast<Json>(
      ROW({BIGINT(), BIGINT(), BIGINT()}),
      JSON(),
      rowOfDictElements,
      rowOfDictElementsExpected);

  // Tests row whose children are json and wrapped in dictionaries.
  auto jsonRowOfDictElements = makeRowWithDictionaryElements<Json>(
      {{"a1"_sv, "a2"_sv, "a3"_sv},
       {"b1"_sv, "b2"_sv, "b3"_sv},
       {"c1"_sv, "c2"_sv, "c3"_sv}},
      ROW({JSON(), JSON(), JSON()}));
  auto jsonRowOfDictElementsExpected = makeNullableFlatVector<Json>(
      {"[null,null,null]", "[a2,b2,c2]", "[a1,b1,c1]"});
  testCast<Json>(
      ROW({JSON(), JSON(), JSON()}),
      JSON(),
      jsonRowOfDictElements,
      jsonRowOfDictElementsExpected);

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
  auto keyVector = makeNullableFlatVector<StringView>(
      {"blue"_sv, "red"_sv, "green"_sv, "yellow"_sv, "purple"_sv, "orange"_sv},
      JSON());
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
      MAP(JSON(), ARRAY(BIGINT())),
      nulls,
      3,
      offsets,
      sizes,
      keyVector,
      valueVector,
      0);

  // Create array of map vector
  std::vector<Pair<StringView, int64_t>> a{{"blue"_sv, 1}, {"red"_sv, 2}};
  std::vector<Pair<StringView, int64_t>> b{{"green"_sv, std::nullopt}};
  std::vector<Pair<StringView, int64_t>> c{{"yellow"_sv, 4}, {"purple"_sv, 5}};
  std::vector<std::vector<std::vector<Pair<StringView, int64_t>>>> data{
      {a, b}, {b}, {c, a}};

  auto arrayVector = makeArrayOfMapVector<StringView, int64_t>(data);

  // Create row vector of array of map and map of array
  auto rowVector = makeRowVector({mapVector, arrayVector});

  std::vector<std::optional<Json>> expected{
      R"([{blue:[1,2],red:[null,4]},[{"blue":1,"red":2},{"green":null}]])",
      R"([null,[{"green":null}]])",
      R"([{orange:[11,12],purple:[9,null]},[{"purple":5,"yellow":4},{"blue":1,"red":2}]])"};
  auto expectedVector = makeNullableFlatVector<Json>(expected);

  testCast<Json>(
      ROW({MAP(VARCHAR(), ARRAY(BIGINT())), ARRAY(MAP(VARCHAR(), BIGINT()))}),
      JSON(),
      rowVector,
      expectedVector);
}

TEST_F(JsonCastTest, unsupportedTypes) {
  // Map keys cannot be timestamp.
  auto timestampKeyMap = makeMapVector<Timestamp, int64_t>({{}});
  EXPECT_THROW(
      evaluateCast<Json>(
          MAP(TIMESTAMP(), BIGINT()), JSON(), makeRowVector({timestampKeyMap})),
      VeloxException);

  // All children of row must be of supported types.
  auto invalidTypeRow = makeRowVector({timestampKeyMap});
  EXPECT_THROW(
      evaluateCast<Json>(
          ROW({MAP(TIMESTAMP(), BIGINT())}),
          JSON(),
          makeRowVector({invalidTypeRow})),
      VeloxException);

  // Map keys cannot be null.
  auto nullKeyVector =
      makeNullableFlatVector<StringView>({"red"_sv, std::nullopt});
  auto valueVector = makeNullableFlatVector<int64_t>({1, 2});

  auto offsets = AlignedBuffer::allocate<vector_size_t>(1, pool());
  auto sizes = AlignedBuffer::allocate<vector_size_t>(1, pool());
  makeOffsetsAndSizes(2, 2, offsets, sizes);

  auto nullKeyMap = std::make_shared<MapVector>(
      pool(),
      MAP(VARCHAR(), BIGINT()),
      BufferPtr(nullptr),
      1,
      offsets,
      sizes,
      nullKeyVector,
      valueVector,
      0);
  EXPECT_THROW(
      evaluateCast<Json>(
          MAP(VARCHAR(), BIGINT()), JSON(), makeRowVector({nullKeyMap})),
      VeloxException);

  // Map keys cannot be complex type.
  auto arrayKeyVector = makeNullableArrayVector<int64_t>({{1}, {2}});
  auto arrayKeyMap = std::make_shared<MapVector>(
      pool(),
      MAP(ARRAY(BIGINT()), BIGINT()),
      BufferPtr(nullptr),
      1,
      offsets,
      sizes,
      arrayKeyVector,
      valueVector,
      0);
  EXPECT_THROW(
      evaluateCast<Json>(
          MAP(ARRAY(BIGINT()), BIGINT()), JSON(), makeRowVector({arrayKeyMap})),
      VeloxException);

  // Map keys of json type must not be null.
  auto jsonKeyVector =
      makeNullableFlatVector<Json>({"red"_sv, std::nullopt}, JSON());
  auto invalidJsonKeyMap = std::make_shared<MapVector>(
      pool(),
      MAP(JSON(), BIGINT()),
      BufferPtr(nullptr),
      1,
      offsets,
      sizes,
      jsonKeyVector,
      valueVector,
      0);
  EXPECT_THROW(
      evaluateCast<Json>(
          MAP(JSON(), BIGINT()), JSON(), makeRowVector({invalidJsonKeyMap})),
      VeloxException);

  // Not allowing to cast from json to itself.
  EXPECT_THROW(
      evaluateCast<Json>(
          JSON(),
          JSON(),
          makeRowVector({makeNullableFlatVector<Json>(
              {"123"_sv, R"("abc")"_sv, ""_sv, std::nullopt}, JSON())})),
      VeloxException);
}

TEST_F(JsonCastTest, toVarchar) {
  testCast<Json, StringView>(
      JSON(),
      VARCHAR(),
      {R"("aaa")"_sv, R"("bbb")"_sv, R"("ccc")"_sv},
      {"aaa"_sv, "bbb"_sv, "ccc"_sv});
  testCast<Json, StringView>(
      JSON(),
      VARCHAR(),
      {"\"\""_sv,
       std::nullopt,
       R"("\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000b\f\r\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f\"\\ .")"_sv},
      {""_sv,
       std::nullopt,
       "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\"\\ ."_sv});
  testCast<Json, StringView>(
      JSON(),
      VARCHAR(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
  testCast<Json, StringView>(
      JSON(),
      VARCHAR(),
      {"123"_sv,
       "-12.3"_sv,
       "true"_sv,
       "false"_sv,
       "NaN"_sv,
       "Infinity"_sv,
       "-Infinity"_sv,
       "null"_sv},
      {"123"_sv,
       "-12.3"_sv,
       "true"_sv,
       "false"_sv,
       "NaN"_sv,
       "Infinity"_sv,
       "-Infinity"_sv,
       std::nullopt});
}

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
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;

class VectorEstimateFlatSizeTest : public testing::Test,
                                   public test::VectorTestBase {
 protected:
  using test::VectorTestBase::makeArrayVector;

  ArrayVectorPtr makeArrayVector(
      const TypePtr& type,
      vector_size_t size,
      BufferPtr offsets,
      BufferPtr lengths,
      const VectorPtr& elements) {
    return std::make_shared<ArrayVector>(
        pool(), type, nullptr, size, offsets, lengths, elements);
  }

  std::vector<std::string> longStrings_{
      {std::string(15, '.'), std::string(16, '-'), std::string(17, '+')}};
};

namespace {
int16_t int16At(vector_size_t row) {
  return row;
}

int32_t int32At(vector_size_t row) {
  return row;
}

int64_t int64At(vector_size_t row) {
  return row;
}

float floatAt(vector_size_t row) {
  return row;
}

double doubleAt(vector_size_t row) {
  return row;
}

bool boolAt(vector_size_t row) {
  return row % 7 == 0;
}

StringView shortStringAt(vector_size_t row) {
  return StringView(std::string(row % 3, '.'));
};
} // namespace

TEST_F(VectorEstimateFlatSizeTest, fixedWidthNoNulls) {
  // Fixed width vectors without nulls.
  VectorPtr flat = makeFlatVector<int16_t>(1'000, int16At);
  EXPECT_EQ(2976, flat->retainedSize());
  EXPECT_EQ(2976, flat->estimateFlatSize());

  flat = makeFlatVector<int32_t>(1'000, int32At);
  EXPECT_EQ(4000, flat->retainedSize());
  EXPECT_EQ(4000, flat->estimateFlatSize());

  flat = makeFlatVector<int64_t>(1'000, int64At);
  EXPECT_EQ(8096, flat->retainedSize());
  EXPECT_EQ(8096, flat->estimateFlatSize());

  flat = makeFlatVector<float>(1'000, floatAt);
  EXPECT_EQ(4000, flat->retainedSize());
  EXPECT_EQ(4000, flat->estimateFlatSize());

  flat = makeFlatVector<double>(1'000, doubleAt);
  EXPECT_EQ(8096, flat->retainedSize());
  EXPECT_EQ(8096, flat->estimateFlatSize());

  flat = makeFlatVector<bool>(1'000, boolAt);
  EXPECT_EQ(160, flat->retainedSize());
  EXPECT_EQ(160, flat->estimateFlatSize());
}

TEST_F(VectorEstimateFlatSizeTest, fixedWidthWithNulls) {
  // Fixed width vectors with nulls. Nulls buffer adds a few bytes.
  VectorPtr flat = makeFlatVector<int16_t>(1'000, int16At, nullEvery(5));
  EXPECT_EQ(3136, flat->retainedSize());
  EXPECT_EQ(3136, flat->estimateFlatSize());

  flat = makeFlatVector<int32_t>(1'000, int32At, nullEvery(5));
  EXPECT_EQ(4160, flat->retainedSize());
  EXPECT_EQ(4160, flat->estimateFlatSize());

  flat = makeFlatVector<int64_t>(1'000, int64At, nullEvery(5));
  EXPECT_EQ(8256, flat->retainedSize());
  EXPECT_EQ(8256, flat->estimateFlatSize());

  flat = makeFlatVector<float>(1'000, floatAt, nullEvery(5));
  EXPECT_EQ(4160, flat->retainedSize());
  EXPECT_EQ(4160, flat->estimateFlatSize());

  flat = makeFlatVector<double>(1'000, doubleAt, nullEvery(5));
  EXPECT_EQ(8256, flat->retainedSize());
  EXPECT_EQ(8256, flat->estimateFlatSize());

  flat = makeFlatVector<bool>(1'000, boolAt, nullEvery(5));
  EXPECT_EQ(320, flat->retainedSize());
  EXPECT_EQ(320, flat->estimateFlatSize());
}

TEST_F(VectorEstimateFlatSizeTest, dictionaryFixedWidthNoExtraNulls) {
  // Dictionary vector. Indices buffer adds a few bytes.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  auto dict = makeDict(makeFlatVector<int16_t>(1'000, int16At));
  EXPECT_EQ(3392, dict->retainedSize());
  EXPECT_EQ(297, dict->estimateFlatSize());
  EXPECT_EQ(288, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<int32_t>(1'000, int32At));
  EXPECT_EQ(4416, dict->retainedSize());
  EXPECT_EQ(400, dict->estimateFlatSize());
  EXPECT_EQ(416, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<int64_t>(1'000, int64At));
  EXPECT_EQ(8512, dict->retainedSize());
  EXPECT_EQ(809, dict->estimateFlatSize());
  EXPECT_EQ(928, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<float>(1'000, floatAt));
  EXPECT_EQ(4416, dict->retainedSize());
  EXPECT_EQ(400, dict->estimateFlatSize());
  EXPECT_EQ(416, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<double>(1'000, doubleAt));
  EXPECT_EQ(8512, dict->retainedSize());
  EXPECT_EQ(809, dict->estimateFlatSize());
  EXPECT_EQ(928, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<bool>(1'000, boolAt));
  EXPECT_EQ(576, dict->retainedSize());
  EXPECT_EQ(16, dict->estimateFlatSize());
  EXPECT_EQ(32, flatten(dict)->retainedSize());

  // 2 levels of dictionary encoding.
  auto makeDoubleDict = [&](auto base) {
    return wrapInDictionary(indices, 50, wrapInDictionary(indices, 100, base));
  };

  dict = makeDoubleDict(makeFlatVector<int16_t>(1'000, int16At));
  EXPECT_EQ(3808, dict->retainedSize());
  EXPECT_EQ(148, dict->estimateFlatSize());
  EXPECT_EQ(160, flatten(dict)->retainedSize());

  dict = makeDoubleDict(makeFlatVector<int32_t>(1'000, int32At));
  EXPECT_EQ(4832, dict->retainedSize());
  EXPECT_EQ(200, dict->estimateFlatSize());
  EXPECT_EQ(288, flatten(dict)->retainedSize());

  dict = makeDoubleDict(makeFlatVector<int64_t>(1'000, int64At));
  EXPECT_EQ(8928, dict->retainedSize());
  EXPECT_EQ(404, dict->estimateFlatSize());
  EXPECT_EQ(416, flatten(dict)->retainedSize());

  dict = makeDoubleDict(makeFlatVector<float>(1'000, floatAt));
  EXPECT_EQ(4832, dict->retainedSize());
  EXPECT_EQ(200, dict->estimateFlatSize());
  EXPECT_EQ(288, flatten(dict)->retainedSize());

  dict = makeDoubleDict(makeFlatVector<double>(1'000, doubleAt));
  EXPECT_EQ(8928, dict->retainedSize());
  EXPECT_EQ(404, dict->estimateFlatSize());
  EXPECT_EQ(416, flatten(dict)->retainedSize());

  dict = makeDoubleDict(makeFlatVector<bool>(1'000, boolAt));
  EXPECT_EQ(992, dict->retainedSize());
  EXPECT_EQ(8, dict->estimateFlatSize());
  EXPECT_EQ(32, flatten(dict)->retainedSize());
}

TEST_F(VectorEstimateFlatSizeTest, dictionaryFixedWidthExtraNulls) {
  // Dictionary vector with extra nulls.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto dictNulls = AlignedBuffer::allocate<bool>(100, pool());
  auto rawDictNulls = dictNulls->asMutable<uint64_t>();
  for (auto i = 0; i < 100; i++) {
    bits::setNull(rawDictNulls, i, i % 3 == 0);
  }

  auto makeDict = [&](auto base) {
    return BaseVector::wrapInDictionary(dictNulls, indices, 100, base);
  };

  auto dict = makeDict(makeFlatVector<int16_t>(1'000, int16At));
  EXPECT_EQ(3424, dict->retainedSize());
  EXPECT_EQ(297, dict->estimateFlatSize());
  EXPECT_EQ(320, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<int32_t>(1'000, int32At));
  EXPECT_EQ(4448, dict->retainedSize());
  EXPECT_EQ(400, dict->estimateFlatSize());
  EXPECT_EQ(448, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<int64_t>(1'000, int64At));
  EXPECT_EQ(8544, dict->retainedSize());
  EXPECT_EQ(809, dict->estimateFlatSize());
  EXPECT_EQ(960, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<float>(1'000, floatAt));
  EXPECT_EQ(4448, dict->retainedSize());
  EXPECT_EQ(400, dict->estimateFlatSize());
  EXPECT_EQ(448, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<double>(1'000, doubleAt));
  EXPECT_EQ(8544, dict->retainedSize());
  EXPECT_EQ(809, dict->estimateFlatSize());
  EXPECT_EQ(960, flatten(dict)->retainedSize());

  dict = makeDict(makeFlatVector<bool>(1'000, boolAt));
  EXPECT_EQ(608, dict->retainedSize());
  EXPECT_EQ(16, dict->estimateFlatSize());
  EXPECT_EQ(64, flatten(dict)->retainedSize());

  // Dictionary vector with all nulls over an empty flat vector.
  auto indicesAllZero = makeIndices(100, [](auto /*row*/) { return 0; });

  auto dictAllNulls = AlignedBuffer::allocate<bool>(100, pool());
  auto rawDictAllNulls = dictAllNulls->asMutable<uint64_t>();
  for (auto i = 0; i < 100; i++) {
    bits::setNull(rawDictAllNulls, i, true);
  }

  auto makeDictOverEmpty = [&](auto base) {
    return BaseVector::wrapInDictionary(
        dictAllNulls, indicesAllZero, 100, base);
  };

  dict = makeDictOverEmpty(makeFlatVector<int16_t>(0, int16At));
  EXPECT_EQ(448, dict->retainedSize());
  EXPECT_EQ(232, dict->estimateFlatSize());
  EXPECT_EQ(320, flatten(dict)->retainedSize());

  dict = makeDictOverEmpty(makeFlatVector<double>(0, doubleAt));
  EXPECT_EQ(448, dict->retainedSize());
  EXPECT_EQ(832, dict->estimateFlatSize());
  EXPECT_EQ(960, flatten(dict)->retainedSize());
}

TEST_F(VectorEstimateFlatSizeTest, flatStrings) {
  // Inlined strings.
  auto flat = makeFlatVector<StringView>(1'000, shortStringAt);
  EXPECT_EQ(16288, flat->retainedSize());
  EXPECT_EQ(16288, flat->estimateFlatSize());

  // Inlined strings with nulls.
  flat = makeFlatVector<StringView>(1'000, shortStringAt, nullEvery(5));
  EXPECT_EQ(16448, flat->retainedSize());
  EXPECT_EQ(16448, flat->estimateFlatSize());

  // Non-inlined strings.
  auto longStringAt = [&](auto row) {
    return StringView(longStrings_[row % 3]);
  };
  flat = makeFlatVector<StringView>(1'000, longStringAt);
  EXPECT_EQ(65344, flat->retainedSize());
  EXPECT_EQ(65343, flat->estimateFlatSize());

  flat = makeFlatVector<StringView>(1'000, longStringAt, nullEvery(5));
  EXPECT_EQ(65504, flat->retainedSize());
  EXPECT_EQ(65504, flat->estimateFlatSize());
}

TEST_F(VectorEstimateFlatSizeTest, dictionaryShortStrings) {
  // Inlined strings.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  auto dict = makeDict(makeFlatVector<StringView>(1'000, shortStringAt));
  EXPECT_EQ(16704, dict->retainedSize());
  EXPECT_EQ(1628, dict->estimateFlatSize());
  EXPECT_EQ(1952, flatten(dict)->retainedSize());

  // Inlined strings with nulls.
  dict =
      makeDict(makeFlatVector<StringView>(1'000, shortStringAt, nullEvery(5)));
  EXPECT_EQ(16864, dict->retainedSize());
  EXPECT_EQ(1644, dict->estimateFlatSize());
  EXPECT_EQ(1984, flatten(dict)->retainedSize());
}

TEST_F(VectorEstimateFlatSizeTest, dictionaryLongStrings) {
  // Non-inlined strings.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  auto longStringAt = [&](auto row) {
    return StringView(longStrings_[row % 3]);
  };

  auto dict = makeDict(makeFlatVector<StringView>(1'000, longStringAt));
  EXPECT_EQ(65760, dict->retainedSize());
  EXPECT_EQ(6534, dict->estimateFlatSize());
  // Flatten() method uses BaseVector::copy() which doesn't copy the strings,
  // but rather copies the shared pointer to the string buffers of the source
  // vector. Hence, the size of the "flattened" vector includes the size of the
  // original string buffers.
  EXPECT_EQ(51008, flatten(dict)->retainedSize());

  // Non-inlined strings with nulls.
  dict =
      makeDict(makeFlatVector<StringView>(1'000, longStringAt, nullEvery(5)));
  EXPECT_EQ(65920, dict->retainedSize());
  EXPECT_EQ(6550, dict->estimateFlatSize());
  // Flatten() method uses BaseVector::copy() which doesn't copy the strings,
  // but rather copies the shared pointer to the string buffers of the source
  // vector. Hence, the size of the "flattened" vector includes the size of the
  // original string buffers.
  EXPECT_EQ(51040, flatten(dict)->retainedSize());
}

TEST_F(VectorEstimateFlatSizeTest, arrayOfInts) {
  // Flat array.
  auto array = makeArrayVector<int32_t>(
      1'000,
      [](auto /* row */) { return 1; },
      [](auto row, auto index) { return row + index; });

  auto elements = array->elements();

  EXPECT_EQ(4000, elements->retainedSize());
  EXPECT_EQ(12000, array->retainedSize());
  EXPECT_EQ(12000, array->estimateFlatSize());

  // Dictionary-encoded array.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  EXPECT_EQ(12416, makeDict(array)->retainedSize());
  EXPECT_EQ(1200, makeDict(array)->estimateFlatSize());
  EXPECT_EQ(1248, flatten(makeDict(array))->estimateFlatSize());

  // Flat array with dictionary encoded elements.
  auto offsets = makeIndices(100, [](auto row) { return row; });
  auto lengths = makeIndices(100, [](auto /*row*/) { return 1; });

  array = makeArrayVector(
      ARRAY(INTEGER()), 100, offsets, lengths, makeDict(elements));
  EXPECT_EQ(5248, array->retainedSize());
  EXPECT_EQ(1232, array->estimateFlatSize());
  EXPECT_EQ(1248, flatten(array)->estimateFlatSize());
}

TEST_F(VectorEstimateFlatSizeTest, arrayOfShortStrings) {
  // Flat array.
  auto array = makeArrayVector<StringView>(
      1'000,
      [](auto /* row */) { return 1; },
      [](auto row, auto index) { return shortStringAt(row + index); });

  auto elements = array->elements();

  EXPECT_EQ(16288, elements->retainedSize());
  EXPECT_EQ(24288, array->retainedSize());
  EXPECT_EQ(24288, array->estimateFlatSize());

  // Dictionary-encoded array.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  EXPECT_EQ(24704, makeDict(array)->retainedSize());
  EXPECT_EQ(2428, makeDict(array)->estimateFlatSize());
  EXPECT_EQ(2784, flatten(makeDict(array))->estimateFlatSize());

  // Flat array with dictionary encoded elements.
  auto offsets = makeIndices(100, [](auto row) { return row; });
  auto lengths = makeIndices(100, [](auto /*row*/) { return 1; });

  array = makeArrayVector(
      ARRAY(VARCHAR()), 100, offsets, lengths, makeDict(elements));
  EXPECT_EQ(17536, array->retainedSize());
  EXPECT_EQ(2460, array->estimateFlatSize());
  EXPECT_EQ(2784, flatten(array)->estimateFlatSize());
}

TEST_F(VectorEstimateFlatSizeTest, arrayOfLongStrings) {
  // Flat array.
  auto longStringAt = [&](auto row, auto index) {
    return StringView(longStrings_[(row + index) % 3]);
  };
  auto array = makeArrayVector<StringView>(
      1'000, [](auto /* row */) { return 1; }, longStringAt);

  auto elements = array->elements();

  EXPECT_EQ(65344, elements->retainedSize());
  EXPECT_EQ(73344, array->retainedSize());
  EXPECT_EQ(73343, array->estimateFlatSize());

  // Dictionary-encoded array.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  EXPECT_EQ(73760, makeDict(array)->retainedSize());
  EXPECT_EQ(7334, makeDict(array)->estimateFlatSize());
  // Flattened vector includes the original string buffers.
  EXPECT_EQ(51840, flatten(makeDict(array))->estimateFlatSize());

  // Flat array with dictionary encoded elements.
  auto offsets = makeIndices(100, [](auto row) { return row; });
  auto lengths = makeIndices(100, [](auto /*row*/) { return 1; });

  array = makeArrayVector(
      ARRAY(VARCHAR()), 100, offsets, lengths, makeDict(elements));
  EXPECT_EQ(66592, array->retainedSize());
  EXPECT_EQ(7366, array->estimateFlatSize());
  // Flattened vector includes the original string buffers.
  EXPECT_EQ(51840, flatten(array)->estimateFlatSize());
}

TEST_F(VectorEstimateFlatSizeTest, mapOfInts) {
  // Flat map.
  auto map = makeMapVector<int32_t, double>(
      1'000,
      [](auto /* row */) { return 1; },
      [](auto row) { return row; },
      [](auto row) { return row + 0.1; });

  auto keys = map->mapKeys();
  auto values = map->mapValues();

  EXPECT_EQ(4000, keys->retainedSize());
  EXPECT_EQ(8096, values->retainedSize());
  EXPECT_EQ(20096, map->retainedSize());
  EXPECT_EQ(20096, map->estimateFlatSize());

  // Dictionary-encoded map.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  EXPECT_EQ(20512, makeDict(map)->retainedSize());
  EXPECT_EQ(2009, makeDict(map)->estimateFlatSize());
  EXPECT_EQ(2175, flatten(makeDict(map))->estimateFlatSize());

  // Flat map with dictionary encoded keys and values.
  auto offsets = makeIndices(100, [](auto row) { return row; });
  auto lengths = makeIndices(100, [](auto /*row*/) { return 1; });

  map = std::make_shared<MapVector>(
      pool(),
      MAP(INTEGER(), DOUBLE()),
      nullptr,
      100,
      offsets,
      lengths,
      makeDict(keys),
      makeDict(values));
  EXPECT_EQ(13760, map->retainedSize());
  EXPECT_EQ(2041, map->estimateFlatSize());
  EXPECT_EQ(2175, flatten(map)->estimateFlatSize());
}

TEST_F(VectorEstimateFlatSizeTest, structs) {
  // Flat struct.
  auto row = makeRowVector({
      makeFlatVector<int32_t>(1'000, int32At),
      makeFlatVector<double>(1'000, doubleAt),
      makeFlatVector<StringView>(1'000, shortStringAt),
  });

  EXPECT_EQ(4000, row->childAt(0)->retainedSize());
  EXPECT_EQ(8096, row->childAt(1)->retainedSize());
  EXPECT_EQ(16288, row->childAt(2)->retainedSize());
  EXPECT_EQ(28384, row->retainedSize());
  EXPECT_EQ(28384, row->estimateFlatSize());

  // Dictionary-encoded struct.
  auto indices = makeIndices(100, [](auto row) { return row * 2; });

  auto makeDict = [&](auto base) {
    return wrapInDictionary(indices, 100, base);
  };

  EXPECT_EQ(28800, makeDict(row)->retainedSize());
  EXPECT_EQ(2838, makeDict(row)->estimateFlatSize());
  EXPECT_EQ(3295, flatten(makeDict(row))->estimateFlatSize());

  // Flat struct with dictionary encoded fields.
  row = makeRowVector(
      {makeDict(row->childAt(0)),
       makeDict(row->childAt(1)),
       makeDict(row->childAt(2))});
  EXPECT_EQ(29632, row->retainedSize());
  EXPECT_EQ(2837, row->estimateFlatSize());
  EXPECT_EQ(3295, flatten(row)->estimateFlatSize());
}

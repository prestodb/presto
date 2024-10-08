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

#include "velox/common/base/VeloxException.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using facebook::velox::test::VectorMaker;

class VectorMakerTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  VectorMaker maker_{pool_.get()};
};

TEST_F(VectorMakerTest, flatVector) {
  std::vector<int64_t> data = {0, 1, 2, 3, 1024, -123456, -99, -999, -1};
  auto flatVector = maker_.flatVector(data);

  EXPECT_EQ(data.size(), flatVector->size());
  EXPECT_FALSE(flatVector->mayHaveNulls());
  EXPECT_EQ(0, flatVector->getNullCount().value());
  EXPECT_FALSE(flatVector->isSorted().value());
  EXPECT_EQ(9, flatVector->getDistinctValueCount().value());
  EXPECT_EQ(-123456, flatVector->getMin().value());
  EXPECT_EQ(1024, flatVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    EXPECT_EQ(data[i], flatVector->valueAt(i));
  }
}

TEST_F(VectorMakerTest, nullableFlatVector) {
  std::vector<std::optional<int64_t>> data = {
      0, 1, std::nullopt, 3, 1024, -123456, -99, -999, std::nullopt, -1};

  auto flatVector = maker_.flatVectorNullable<int64_t>(data);

  EXPECT_EQ(data.size(), flatVector->size());
  EXPECT_TRUE(flatVector->mayHaveNulls());
  EXPECT_EQ(2, flatVector->getNullCount().value());
  EXPECT_FALSE(flatVector->isSorted().value());
  EXPECT_EQ(8, flatVector->getDistinctValueCount().value());
  EXPECT_EQ(-123456, flatVector->getMin().value());
  EXPECT_EQ(1024, flatVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] == std::nullopt) {
      EXPECT_TRUE(flatVector->isNullAt(i));
    } else {
      EXPECT_FALSE(flatVector->isNullAt(i));
      EXPECT_EQ(*data[i], flatVector->valueAt(i));
    }
  }
}

TEST_F(VectorMakerTest, flatVectorString) {
  std::vector<std::string> data = {
      "hello",
      "world",
      "afe@41 135$2@%",
      "",
      "end",
  };

  auto flatVector = maker_.flatVector(data);

  EXPECT_EQ(data.size(), flatVector->size());
  EXPECT_FALSE(flatVector->mayHaveNulls());
  EXPECT_EQ(0, flatVector->getNullCount().value());
  EXPECT_FALSE(flatVector->isSorted().value());
  EXPECT_EQ(5, flatVector->getDistinctValueCount().value());
  EXPECT_EQ(""_sv, flatVector->getMin().value());
  EXPECT_EQ(StringView("world"), flatVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    EXPECT_EQ(data[i], std::string(flatVector->valueAt(i)));
  }
}

TEST_F(VectorMakerTest, flatVectorStringTypes) {
  auto validate = [&](const FlatVectorPtr<StringView>& input) {
    ASSERT_NE(nullptr, input);
    EXPECT_EQ("hello"_sv, input->valueAt(0));
    EXPECT_EQ("world"_sv, input->valueAt(1));
  };

  // char*
  validate(maker_.flatVector({"hello", "world"}));

  // std::string
  validate(maker_.flatVector({std::string("hello"), std::string("world")}));

  // StringView
  validate(maker_.flatVector({"hello"_sv, "world"_sv}));

  // std::string_view
  validate(maker_.flatVector(
      {std::string_view("hello"), std::string_view("world")}));
}

TEST_F(VectorMakerTest, flatVectorStringNullableTypes) {
  auto validate = [&](const FlatVectorPtr<StringView>& input) {
    ASSERT_NE(nullptr, input);
    EXPECT_EQ("hello"_sv, input->valueAt(0));
    EXPECT_TRUE(input->isNullAt(1));
    EXPECT_EQ("world"_sv, input->valueAt(2));
  };

  // Compilers can't infer dependent template types, so we either need to
  // explicitly specify the template type, of fully declare the vector type:

  // char*
  validate(
      maker_.flatVectorNullable<const char*>({"hello", std::nullopt, "world"}));

  // std::string
  validate(maker_.flatVectorNullable(std::vector<std::optional<std::string>>(
      {"hello", std::nullopt, "world"})));

  // StringView
  validate(
      maker_.flatVectorNullable<StringView>({"hello", std::nullopt, "world"}));

  // std::string_view
  validate(
      maker_.flatVectorNullable(std::vector<std::optional<std::string_view>>(
          {"hello", std::nullopt, "world"})));
}

TEST_F(VectorMakerTest, nullableFlatVectorString) {
  std::vector<std::optional<std::string>> data = {
      "hello",
      "world",
      std::nullopt,
      "",
      "end",
  };

  auto flatVector = maker_.flatVectorNullable(data);

  EXPECT_EQ(data.size(), flatVector->size());
  EXPECT_TRUE(flatVector->mayHaveNulls());
  EXPECT_EQ(1, flatVector->getNullCount().value());
  EXPECT_FALSE(flatVector->isSorted().value());
  EXPECT_EQ(4, flatVector->getDistinctValueCount().value());
  EXPECT_EQ(""_sv, flatVector->getMin().value());
  EXPECT_EQ("world"_sv, flatVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] == std::nullopt) {
      EXPECT_TRUE(flatVector->isNullAt(i));
    } else {
      EXPECT_FALSE(flatVector->isNullAt(i));
      EXPECT_EQ(*data[i], std::string(flatVector->valueAt(i)));
    }
  }
}

TEST_F(VectorMakerTest, nullableFlatVectorBool) {
  std::vector<std::optional<bool>> data = {
      true,
      false,
      std::nullopt,
      false,
      true,
  };

  auto flatVector = maker_.flatVectorNullable(data);

  EXPECT_EQ(data.size(), flatVector->size());
  EXPECT_TRUE(flatVector->mayHaveNulls());
  EXPECT_EQ(1, flatVector->getNullCount().value());
  EXPECT_FALSE(flatVector->isSorted().value());
  EXPECT_EQ(2, flatVector->getDistinctValueCount().value());
  EXPECT_EQ(false, flatVector->getMin().value());
  EXPECT_EQ(true, flatVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] == std::nullopt) {
      EXPECT_TRUE(flatVector->isNullAt(i));
    } else {
      EXPECT_FALSE(flatVector->isNullAt(i));
      EXPECT_EQ(*data[i], flatVector->valueAt(i));
    }
  }
}

TEST_F(VectorMakerTest, arrayVector) {
  std::vector<std::vector<int64_t>> data = {
      {0, 0},
      {1, 2, 3},
      {1024, -123, -99, -999},
      {},
      {-1},
  };

  auto arrayVector = maker_.arrayVector<int64_t>(data);

  EXPECT_FALSE(arrayVector->mayHaveNulls());

  // Validate array sizes and offsets.
  EXPECT_EQ(5, arrayVector->size());

  EXPECT_EQ(2, arrayVector->sizeAt(0));
  EXPECT_EQ(3, arrayVector->sizeAt(1));
  EXPECT_EQ(4, arrayVector->sizeAt(2));
  EXPECT_EQ(0, arrayVector->sizeAt(3));
  EXPECT_EQ(1, arrayVector->sizeAt(4));

  EXPECT_EQ(0, arrayVector->offsetAt(0));
  EXPECT_EQ(2, arrayVector->offsetAt(1));
  EXPECT_EQ(5, arrayVector->offsetAt(2));
  EXPECT_EQ(9, arrayVector->offsetAt(3));
  EXPECT_EQ(9, arrayVector->offsetAt(4));

  // Validate actual vector elements.
  auto* elementsVector = arrayVector->elements()->asFlatVector<int64_t>();

  EXPECT_FALSE(elementsVector->mayHaveNulls());

  vector_size_t idx = 0;
  for (const auto& item : data) {
    for (auto i : item) {
      EXPECT_EQ(i, elementsVector->valueAt(idx++));
    }
  }
}

TEST_F(VectorMakerTest, arrayVectorString) {
  auto arrayVector = maker_.arrayVector<std::string>({
      {"a", "abc", "Somewhat long", "test"},
      {"b", "Another long string"},
  });

  EXPECT_FALSE(arrayVector->mayHaveNulls());

  // Validate array sizes and offsets.
  EXPECT_EQ(2, arrayVector->size());

  EXPECT_EQ(4, arrayVector->sizeAt(0));
  EXPECT_EQ(2, arrayVector->sizeAt(1));

  EXPECT_EQ(0, arrayVector->offsetAt(0));
  EXPECT_EQ(4, arrayVector->offsetAt(1));

  // Validate actual vector elements.
  auto* elementsVector = arrayVector->elements()->asFlatVector<StringView>();

  EXPECT_FALSE(elementsVector->mayHaveNulls());

  EXPECT_EQ("a", elementsVector->valueAt(0).str());
  EXPECT_EQ("abc", elementsVector->valueAt(1).str());
  EXPECT_EQ("Somewhat long", elementsVector->valueAt(2).str());
  EXPECT_EQ("test", elementsVector->valueAt(3).str());

  EXPECT_EQ("b", elementsVector->valueAt(4).str());
  EXPECT_EQ("Another long string", elementsVector->valueAt(5).str());
}

TEST_F(VectorMakerTest, nullableArrayVector) {
  auto O = [](std::vector<std::optional<int64_t>> data) {
    return std::make_optional(data);
  };

  std::vector<std::optional<std::vector<std::optional<int64_t>>>> data = {
      O({std::nullopt}),
      O({1, 2, 3}),
      O({1024, std::nullopt, -99, -999}),
      O({}),
      O({std::nullopt, -1}),
  };

  auto arrayVector = maker_.arrayVectorNullable<int64_t>(data);

  // Validate array sizes and offsets.
  EXPECT_EQ(5, arrayVector->size());

  EXPECT_EQ(1, arrayVector->sizeAt(0));
  EXPECT_EQ(3, arrayVector->sizeAt(1));
  EXPECT_EQ(4, arrayVector->sizeAt(2));
  EXPECT_EQ(0, arrayVector->sizeAt(3));
  EXPECT_EQ(2, arrayVector->sizeAt(4));

  EXPECT_EQ(0, arrayVector->offsetAt(0));
  EXPECT_EQ(1, arrayVector->offsetAt(1));
  EXPECT_EQ(4, arrayVector->offsetAt(2));
  EXPECT_EQ(8, arrayVector->offsetAt(3));
  EXPECT_EQ(8, arrayVector->offsetAt(4));

  // Validate actual vector elements.
  auto* elementsVector = arrayVector->elements()->asFlatVector<int64_t>();

  EXPECT_TRUE(elementsVector->mayHaveNulls());

  vector_size_t idx = 0;
  for (const auto& item : data) {
    for (auto i : item.value()) {
      if (i == std::nullopt) {
        EXPECT_TRUE(elementsVector->isNullAt(idx));
      } else {
        EXPECT_FALSE(elementsVector->isNullAt(idx));
        EXPECT_EQ(i, elementsVector->valueAt(idx));
      }
      ++idx;
    }
  }
}

TEST_F(VectorMakerTest, arrayVectorWithNulls) {
  std::vector<std::vector<int64_t>> data = {
      {0, 0},
      {},
      {1024, -123, -99, -999},
      {},
      {-1},
  };
  std::vector<bool> nulls = {false, true, false, false, false};

  //  auto arrayVector = maker_.arrayVector<int64_t>(data);
  auto arrayVector = maker_.arrayVector<int64_t>(
      data.size(),
      [data](vector_size_t i) -> vector_size_t {
        return data[i].size();
      }, // sizeAt
      [data](vector_size_t i, vector_size_t j) -> int64_t {
        return data[i][j];
      }, // valueAt
      [nulls](vector_size_t i) -> bool { return nulls[i]; } // nullAt
  );

  EXPECT_TRUE(arrayVector->mayHaveNulls());

  // Validate array sizes and offsets.
  EXPECT_EQ(5, arrayVector->size());

  EXPECT_EQ(2, arrayVector->sizeAt(0));
  EXPECT_EQ(0, arrayVector->sizeAt(1));
  EXPECT_EQ(4, arrayVector->sizeAt(2));
  EXPECT_EQ(0, arrayVector->sizeAt(3));
  EXPECT_EQ(1, arrayVector->sizeAt(4));

  EXPECT_EQ(0, arrayVector->offsetAt(0));
  EXPECT_EQ(0, arrayVector->offsetAt(1));
  EXPECT_EQ(2, arrayVector->offsetAt(2));
  EXPECT_EQ(6, arrayVector->offsetAt(3));
  EXPECT_EQ(6, arrayVector->offsetAt(4));

  // Validate array null entries
  EXPECT_TRUE(arrayVector->mayHaveNulls());
  EXPECT_EQ(1, arrayVector->getNullCount().value());
  for (int i = 0; i < nulls.size(); i++) {
    EXPECT_EQ(arrayVector->isNullAt(i), nulls[i]);
  }

  // Validate actual vector elements.
  auto* elementsVector = arrayVector->elements()->asFlatVector<int64_t>();
  EXPECT_FALSE(elementsVector->mayHaveNulls());

  vector_size_t idx = 0;
  for (const auto& item : data) {
    for (auto i : item) {
      EXPECT_EQ(i, elementsVector->valueAt(idx++));
    }
  }
}

TEST_F(VectorMakerTest, arrayVectorFromJson) {
  auto arrayVector = maker_.arrayVectorFromJson<int32_t>({
      "null",
      "[]",
      "[1, 2, 3]",
      "[1, 2, null, 4]",
      "[null, null]",
  });

  EXPECT_EQ(*arrayVector->type(), *ARRAY(INTEGER()));
  EXPECT_TRUE(arrayVector->mayHaveNulls());

  EXPECT_EQ(5, arrayVector->size());

  auto* elements = arrayVector->elements()->as<FlatVector<int32_t>>();
  EXPECT_TRUE(elements->mayHaveNulls());

  // Null array.
  EXPECT_TRUE(arrayVector->isNullAt(0));

  // Empty array.
  EXPECT_FALSE(arrayVector->isNullAt(1));
  EXPECT_EQ(0, arrayVector->sizeAt(1));

  // [1, 2, 3].
  EXPECT_FALSE(arrayVector->isNullAt(2));
  EXPECT_EQ(3, arrayVector->sizeAt(2));
  EXPECT_EQ(0, arrayVector->offsetAt(2));

  for (auto i = 0; i < 3; ++i) {
    EXPECT_FALSE(elements->isNullAt(i));
    EXPECT_EQ(i + 1, elements->valueAt(i));
  }

  // [1, 2, null, 4].
  EXPECT_FALSE(arrayVector->isNullAt(3));
  EXPECT_EQ(4, arrayVector->sizeAt(3));
  EXPECT_EQ(3, arrayVector->offsetAt(3));

  for (auto i = 0; i < 4; ++i) {
    auto index = 3 + i;
    if (i == 2) {
      EXPECT_TRUE(elements->isNullAt(index));
    } else {
      EXPECT_FALSE(elements->isNullAt(index));
      EXPECT_EQ(i + 1, elements->valueAt(index));
    }
  }

  // [null, null].
  EXPECT_FALSE(arrayVector->isNullAt(4));
  EXPECT_EQ(2, arrayVector->sizeAt(4));
  EXPECT_EQ(7, arrayVector->offsetAt(4));

  for (auto i = 0; i < 2; ++i) {
    EXPECT_TRUE(elements->isNullAt(7 + i));
  }

  auto dates = maker_.arrayVectorFromJson<int32_t>(
      {
          "null",
          "[]",
          "[1, 2, 3]",
          "[1, 2, null, 4]",
          "[null, null]",
      },
      ARRAY(DATE()));

  EXPECT_EQ(*dates->type(), *ARRAY(DATE()));
  EXPECT_TRUE(dates->mayHaveNulls());
  EXPECT_EQ(5, dates->size());

  for (auto i = 0; i < 5; ++i) {
    EXPECT_TRUE(dates->equalValueAt(arrayVector.get(), i, i));
  }
}

TEST_F(VectorMakerTest, nestedArrayVectorFromJson) {
  auto arrayVector = maker_.nestedArrayVectorFromJson<int32_t>(
      {"[[1, 2], [2, 3, 4], [null, 7]]",
       "[[1, 3, 7, 9], []]",
       "[]",
       "[null]",
       "[[1, 2, null], [], null]",
       "null",
       "[[1,2,3],[],[4,5]]"});

  VectorPtr expectedVector = maker_.arrayVectorNullable<int32_t>({
      {{1, 2}},
      {{2, 3, 4}},
      {{std::nullopt, 7}},
      {{1, 3, 7, 9}},
      {{}},
      {{std::nullopt}},
      {{1, 2, std::nullopt}},
      {{}},
      std::nullopt,
      {{1, 2, 3}},
      {{}},
      {{4, 5}},
  });

  VectorPtr elementsOfArrayVector = arrayVector->elements();

  EXPECT_EQ(*arrayVector->elements()->type(), *ARRAY(INTEGER()));
  EXPECT_TRUE(arrayVector->elements()->mayHaveNulls());

  auto rowOfArrays = arrayVector->elements();

  // 7 rows in array of arrays
  EXPECT_EQ(7, arrayVector->size());

  EXPECT_EQ(3, arrayVector->sizeAt(0));
  EXPECT_EQ(2, arrayVector->sizeAt(1));

  // Empty inner array
  EXPECT_EQ(0, arrayVector->sizeAt(2));
  EXPECT_FALSE(arrayVector->isNullAt(2));

  // inner array with null
  EXPECT_EQ(1, arrayVector->sizeAt(3));
  EXPECT_FALSE(arrayVector->isNullAt(3));

  EXPECT_EQ(3, arrayVector->sizeAt(4));

  EXPECT_EQ(0, arrayVector->sizeAt(5));
  EXPECT_TRUE(arrayVector->isNullAt(5));

  EXPECT_EQ(3, arrayVector->sizeAt(6));

  test::assertEqualVectors(expectedVector, elementsOfArrayVector);
}

TEST_F(VectorMakerTest, arrayOfRowVector) {
  std::vector<std::vector<variant>> data = {
      {
          variant::row({1, "red"}),
          variant::row({2, "blue"}),
          variant::row({3, "green"}),
      },
      {},
      {
          variant::row({4, "green"}),
          variant::row({-5, "purple"}),
      },
  };

  auto arrayVector = maker_.arrayOfRowVector(ROW({INTEGER(), VARCHAR()}), data);

  // The arrays themselves can't be null.
  EXPECT_FALSE(arrayVector->mayHaveNulls());

  // Validate array sizes and offsets.
  EXPECT_EQ(3, arrayVector->size());

  EXPECT_EQ(3, arrayVector->sizeAt(0));
  EXPECT_EQ(0, arrayVector->sizeAt(1));
  EXPECT_EQ(2, arrayVector->sizeAt(2));

  EXPECT_EQ(0, arrayVector->offsetAt(0));
  EXPECT_EQ(3, arrayVector->offsetAt(1));
  EXPECT_EQ(3, arrayVector->offsetAt(2));

  // Validate actual vector elements.
  auto* elementsVector = arrayVector->elements()->as<RowVector>();

  EXPECT_FALSE(elementsVector->mayHaveNulls());
  EXPECT_EQ(5, elementsVector->size());

  auto numVector = elementsVector->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ(5, numVector->size());

  EXPECT_EQ(1, numVector->valueAt(0));
  EXPECT_EQ(2, numVector->valueAt(1));
  EXPECT_EQ(3, numVector->valueAt(2));
  EXPECT_EQ(4, numVector->valueAt(3));
  EXPECT_EQ(-5, numVector->valueAt(4));

  auto colorVector = elementsVector->childAt(1)->asFlatVector<StringView>();
  EXPECT_EQ(5, colorVector->size());

  EXPECT_EQ("red", colorVector->valueAt(0).str());
  EXPECT_EQ("blue", colorVector->valueAt(1).str());
  EXPECT_EQ("green", colorVector->valueAt(2).str());
  EXPECT_EQ("green", colorVector->valueAt(3).str());
  EXPECT_EQ("purple", colorVector->valueAt(4).str());
}

TEST_F(VectorMakerTest, arrayOfRowVectorFromTuples) {
  std::vector<std::vector<std::optional<std::tuple<int32_t, std::string>>>>
      data = {
          {{{1, "red"}}, {{2, "blue"}}, {{3, "green"}}},
          {},
          {std::nullopt},
          {{{4, "green"}}, {{-5, "purple"}}},
      };

  auto arrayVector = maker_.arrayOfRowVector(data, ROW({INTEGER(), VARCHAR()}));

  std::vector<vector_size_t> offsets{0, 3, 3, 4};
  auto elements = maker_.rowVector({
      maker_.flatVector<int32_t>({1, 2, 3, 0, 4, -5}),
      maker_.flatVector<std::string>(
          {"red", "blue", "green", "n/a", "green", "purple"}),
  });
  elements->setNull(3, true);
  auto expected = maker_.arrayVector(offsets, elements);

  ASSERT_EQ(expected->size(), arrayVector->size());
  // check equivalent because arrayVector's row type doesn't have name for each
  // column ('', '' ..) whereas expected's row type have names ('c0', 'c1' ..)
  ASSERT_TRUE((*expected->type()).equivalent((*arrayVector->type())));
  for (auto i = 0; i < expected->size(); i++) {
    ASSERT_TRUE(expected->equalValueAt(arrayVector.get(), i, i));
  }
}

TEST_F(VectorMakerTest, arrayVectorUsingBaseVector) {
  auto elementsVector = maker_.flatVector<int64_t>({1, 2, 3, 4, 5, 6});

  // Create an array vector with 2 elements per array .
  auto arrayVector = maker_.arrayVector({0, 2, 4}, elementsVector);

  EXPECT_EQ(arrayVector->size(), 3);
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(arrayVector->sizeAt(i), 2);
    EXPECT_EQ(arrayVector->isNullAt(i), false);
  }

  auto rawArrayValues = arrayVector->elements()->values()->as<int64_t>();
  auto baseValues = elementsVector->values()->as<int64_t>();
  EXPECT_EQ(
      memcmp(
          rawArrayValues, baseValues, elementsVector->size() * sizeof(int64_t)),
      0);

  // Create array vector with last array as null.
  auto arrayVectorWithNull =
      maker_.arrayVector({0, 2, 4, 6}, elementsVector, {3});
  EXPECT_EQ(arrayVectorWithNull->isNullAt(3), true);
  EXPECT_EQ(arrayVectorWithNull->sizeAt(3), 0);
}

TEST_F(VectorMakerTest, mapVectorUsingKeyValueVectorsNoNulls) {
  auto keys = maker_.flatVector<int32_t>({1, 2, 3, 4, 5, 6});
  auto values = maker_.flatVector<int64_t>({7, 8, 9, 10, 11, 12});

  // Create a map vector with 2 entries per map.
  auto mapVector = maker_.mapVector({0, 2, 4}, keys, values);

  EXPECT_EQ(mapVector->size(), 3);
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(mapVector->sizeAt(i), 2);
    EXPECT_EQ(mapVector->isNullAt(i), false);
  }

  auto rawMapKeys = mapVector->mapKeys()->values()->as<int32_t>();
  auto baseKeys = keys->values()->as<int32_t>();
  EXPECT_EQ(memcmp(rawMapKeys, baseKeys, keys->size() * sizeof(int32_t)), 0);

  auto rawMapValues = mapVector->mapValues()->values()->as<int64_t>();
  auto baseValues = values->values()->as<int64_t>();
  EXPECT_EQ(
      memcmp(rawMapValues, baseValues, values->size() * sizeof(int64_t)), 0);
}

TEST_F(VectorMakerTest, mapVectorUsingKeyValueVectorsSomeNulls) {
  auto keys = maker_.flatVector<int32_t>({1, 2, 3, 4, 5, 6});
  auto values = maker_.flatVector<int64_t>({7, 8, 9, 10, 11, 12});

  // Create map vector with last map as null.
  auto mapVectorWithLastNull =
      maker_.mapVector({0, 2, 4, 6}, keys, values, {3});
  EXPECT_EQ(mapVectorWithLastNull->isNullAt(3), true);
  EXPECT_EQ(mapVectorWithLastNull->sizeAt(3), 0);

  // Create map vector with middle map as null.
  auto mapVectorWithMiddleNull =
      maker_.mapVector({0, 2, 2, 4, 6}, keys, values, {1});
  EXPECT_EQ(mapVectorWithMiddleNull->isNullAt(1), true);
  EXPECT_EQ(mapVectorWithMiddleNull->sizeAt(1), 0);
}

TEST_F(VectorMakerTest, mapVectorUsingKeyValueVectorsAllNulls) {
  auto keys = maker_.flatVector<int32_t>({});
  auto values = maker_.flatVector<int64_t>({});

  // Create map vector with last map as null.
  auto mapVector = maker_.mapVector({0, 0, 0}, keys, values, {0, 1, 2});

  EXPECT_EQ(mapVector->size(), 3);
  for (int i = 0; i < 3; i++) {
    EXPECT_EQ(mapVector->sizeAt(i), 0);
    EXPECT_EQ(mapVector->isNullAt(i), true);
  }
}

TEST_F(VectorMakerTest, mapVectorUsingKeyValueVectorsUnevenKeysValues) {
  auto keys = maker_.flatVector<int32_t>({1, 2, 3, 4, 5, 6});
  // Create map vector with uneven keys and values, should fail.
  auto values = maker_.flatVector<int64_t>({7, 8, 9});
  EXPECT_THROW(maker_.mapVector({0, 2, 4}, keys, values), VeloxRuntimeError);
}

TEST_F(VectorMakerTest, mapVectorUsingKeyValueVectorsNullsInvalidIndices) {
  auto keys = maker_.flatVector<int32_t>({0, 1, 2, 3, 4, 5});
  auto values = maker_.flatVector<int64_t>({6, 7, 8, 9, 10, 11});

  // The middle map is NULL, but according to the offsets it has size 2, this
  // should fail.
  EXPECT_THROW(
      maker_.mapVector({0, 2, 4}, keys, values, {1}), VeloxRuntimeError);
}

TEST_F(VectorMakerTest, mapVectorStringString) {
  auto mapVector = maker_.mapVector<std::string, std::string>({
      {{"a", "1"}, {"b", "2"}},
      {{"a", "This is a test"}, {"b", "This is another test"}, {"c", "test"}},
  });

  EXPECT_FALSE(mapVector->mayHaveNulls());

  // Validate map sizes and offsets.
  EXPECT_EQ(2, mapVector->size());

  EXPECT_EQ(2, mapVector->sizeAt(0));
  EXPECT_EQ(3, mapVector->sizeAt(1));

  EXPECT_EQ(0, mapVector->offsetAt(0));
  EXPECT_EQ(2, mapVector->offsetAt(1));

  // Validate map keys and values.
  auto keys = mapVector->mapKeys()->asFlatVector<StringView>();
  auto values = mapVector->mapValues()->asFlatVector<StringView>();

  EXPECT_FALSE(keys->mayHaveNulls());

  EXPECT_EQ("a", keys->valueAt(0));
  EXPECT_EQ("1", values->valueAt(0));

  EXPECT_EQ("b", keys->valueAt(1));
  EXPECT_EQ("2", values->valueAt(1));

  EXPECT_EQ("a", keys->valueAt(2));
  EXPECT_EQ("This is a test", values->valueAt(2));

  EXPECT_EQ("b", keys->valueAt(3));
  EXPECT_EQ("This is another test", values->valueAt(3));

  EXPECT_EQ("c", keys->valueAt(4));
  EXPECT_EQ("test", values->valueAt(4));
}

TEST_F(VectorMakerTest, mapVectorFromJson) {
  auto mapVector = maker_.mapVectorFromJson<int32_t, double>({
      "null",
      "{}",
      "{1: 10.1, 2: 20.2, 3: 30.3}",
      "{1: 10.1, 2: 20.2, 3: null, 4: 40.4}",
      "{1: null, 2: null}",
  });

  MapVector::canonicalize(mapVector);

  EXPECT_EQ(5, mapVector->size());
  EXPECT_EQ(*MAP(INTEGER(), DOUBLE()), *mapVector->type());
  EXPECT_TRUE(mapVector->mayHaveNulls());

  auto* keys = mapVector->mapKeys()->as<SimpleVector<int32_t>>();
  auto* values = mapVector->mapValues()->as<SimpleVector<double>>();

  // Null map.
  EXPECT_TRUE(mapVector->isNullAt(0));

  // Empty map.
  EXPECT_FALSE(mapVector->isNullAt(1));
  EXPECT_EQ(0, mapVector->sizeAt(1));

  // {1: 10.1, 2: 20.2, 3: 30.3}.
  EXPECT_FALSE(mapVector->isNullAt(2));
  EXPECT_EQ(3, mapVector->sizeAt(2));
  EXPECT_EQ(0, mapVector->offsetAt(2));

  for (auto i = 0; i < 3; ++i) {
    EXPECT_FALSE(keys->isNullAt(i));
    EXPECT_EQ(i + 1, keys->valueAt(i));

    EXPECT_FALSE(values->isNullAt(i));
    EXPECT_DOUBLE_EQ((i + 1) * 10.1, values->valueAt(i));
  }

  // {1: 10.1, 2: 20.2, 3: null, 4: 40.4}.
  EXPECT_FALSE(mapVector->isNullAt(3));
  EXPECT_EQ(4, mapVector->sizeAt(3));
  EXPECT_EQ(3, mapVector->offsetAt(3));

  for (auto i = 0; i < 4; ++i) {
    auto index = i + 3;

    EXPECT_FALSE(keys->isNullAt(index));
    EXPECT_EQ(i + 1, keys->valueAt(index));

    if (i == 2) {
      EXPECT_TRUE(values->isNullAt(index));
    } else {
      EXPECT_FALSE(values->isNullAt(index));
      EXPECT_DOUBLE_EQ((i + 1) * 10.1, values->valueAt(index));
    }
  }

  // {1: null, 2: null}.
  EXPECT_FALSE(mapVector->isNullAt(4));
  EXPECT_EQ(2, mapVector->sizeAt(4));
  EXPECT_EQ(7, mapVector->offsetAt(4));

  for (auto i = 0; i < 2; ++i) {
    auto index = i + 7;

    EXPECT_FALSE(keys->isNullAt(index));
    EXPECT_EQ(i + 1, keys->valueAt(index));

    EXPECT_TRUE(values->isNullAt(index));
  }
}

TEST_F(VectorMakerTest, biasVector) {
  std::vector<std::optional<int64_t>> data = {10, 13, std::nullopt, 15, 12, 11};
  auto biasVector = maker_.biasVector(data);

  EXPECT_EQ(data.size(), biasVector->size());
  EXPECT_TRUE(biasVector->mayHaveNulls());
  EXPECT_EQ(1, biasVector->getNullCount().value());
  EXPECT_FALSE(biasVector->isSorted().value());
  EXPECT_EQ(10, biasVector->getMin().value());
  EXPECT_EQ(15, biasVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] == std::nullopt) {
      EXPECT_TRUE(biasVector->isNullAt(i));
    } else {
      EXPECT_FALSE(biasVector->isNullAt(i));
      EXPECT_EQ(*data[i], biasVector->valueAt(i));
    }
  }
}

TEST_F(VectorMakerTest, sequenceVector) {
  std::vector<std::optional<int64_t>> data = {
      10, 10, 11, 11, std::nullopt, std::nullopt, 15, std::nullopt, 12, 12, 12};
  auto sequenceVector = maker_.sequenceVector(data);

  EXPECT_EQ(data.size(), sequenceVector->size());
  EXPECT_TRUE(sequenceVector->mayHaveNulls());
  EXPECT_EQ(6, sequenceVector->numSequences());
  EXPECT_EQ(3, sequenceVector->getNullCount().value());
  EXPECT_FALSE(sequenceVector->isSorted().value());
  EXPECT_EQ(4, sequenceVector->getDistinctValueCount().value());
  EXPECT_EQ(10, sequenceVector->getMin().value());
  EXPECT_EQ(15, sequenceVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] == std::nullopt) {
      EXPECT_TRUE(sequenceVector->isNullAt(i));
    } else {
      EXPECT_FALSE(sequenceVector->isNullAt(i));
      EXPECT_EQ(*data[i], sequenceVector->valueAt(i));
    }
  }
}

TEST_F(VectorMakerTest, sequenceVectorString) {
  std::vector<std::optional<StringView>> data = {
      "a"_sv,
      "a"_sv,
      "a"_sv,
      std::nullopt,
      std::nullopt,
      "b"_sv,
      std::nullopt,
      "c"_sv,
      "c"_sv,
  };
  auto sequenceVector = maker_.sequenceVector(data);

  EXPECT_EQ(data.size(), sequenceVector->size());
  EXPECT_TRUE(sequenceVector->mayHaveNulls());
  EXPECT_EQ(5, sequenceVector->numSequences());
  EXPECT_EQ(3, sequenceVector->getNullCount().value());
  EXPECT_FALSE(sequenceVector->isSorted().value());
  EXPECT_EQ(3, sequenceVector->getDistinctValueCount().value());
  EXPECT_EQ("a"_sv, sequenceVector->getMin().value());
  EXPECT_EQ("c"_sv, sequenceVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] == std::nullopt) {
      EXPECT_TRUE(sequenceVector->isNullAt(i));
    } else {
      EXPECT_FALSE(sequenceVector->isNullAt(i));
      EXPECT_EQ(*data[i], sequenceVector->valueAt(i));
    }
  }
}

TEST_F(VectorMakerTest, constantVector) {
  std::vector<std::optional<int64_t>> data = {99, 99, 99};
  auto constantVector = maker_.constantVector(data);

  EXPECT_FALSE(constantVector->mayHaveNulls());
  EXPECT_EQ(data.size(), constantVector->size());

  for (vector_size_t i = 0; i < data.size(); i++) {
    EXPECT_FALSE(constantVector->isNullAt(i));
    EXPECT_EQ(*data.front(), constantVector->valueAt(i));
  }

  // Null vector.
  data = {std::nullopt, std::nullopt};
  constantVector = maker_.constantVector(data);

  EXPECT_TRUE(constantVector->mayHaveNulls());
  EXPECT_EQ(data.size(), constantVector->size());

  for (vector_size_t i = 0; i < data.size(); i++) {
    EXPECT_TRUE(constantVector->isNullAt(i));
  }
}

TEST_F(VectorMakerTest, constantRowVector) {
  auto vector = maker_.constantRow(
      ROW({INTEGER(), VARCHAR()}), variant::row({5, "orange"}), 123);

  EXPECT_EQ(vector->encoding(), VectorEncoding::Simple::CONSTANT);
  EXPECT_EQ(123, vector->size());

  auto constantVector = vector->as<ConstantVector<ComplexType>>();
  EXPECT_EQ(0, constantVector->index());

  auto rowVector = constantVector->valueVector()->as<RowVector>();
  ASSERT_EQ(1, rowVector->size());

  auto numVector = rowVector->childAt(0)->asFlatVector<int32_t>();
  ASSERT_EQ(1, numVector->size());
  ASSERT_EQ(5, numVector->valueAt(0));

  auto colorVector = rowVector->childAt(1)->asFlatVector<StringView>();
  ASSERT_EQ(1, colorVector->size());
  ASSERT_EQ("orange", colorVector->valueAt(0).str());
}

TEST_F(VectorMakerTest, constantVectorErrors) {
  // Error variations.
  EXPECT_THROW(maker_.constantVector<int64_t>({}), VeloxRuntimeError);
  EXPECT_THROW(maker_.constantVector<int64_t>({1, 2}), VeloxRuntimeError);
  EXPECT_THROW(
      maker_.constantVector<int64_t>({1, 2, 3, 4, 5}), VeloxRuntimeError);

  EXPECT_THROW(
      maker_.constantVector<int64_t>({std::nullopt, 1}), VeloxRuntimeError);
  EXPECT_THROW(
      maker_.constantVector<int64_t>({1, std::nullopt}), VeloxRuntimeError);
  EXPECT_THROW(
      maker_.constantVector<int64_t>({1, std::nullopt, 1}), VeloxRuntimeError);
}

TEST_F(VectorMakerTest, dictionaryVector) {
  std::vector<std::optional<int64_t>> data = {
      99, 99, 11, 10, 99, std::nullopt, 10, std::nullopt};
  auto dictionaryVector = maker_.dictionaryVector(data);

  EXPECT_EQ(data.size(), dictionaryVector->size());
  EXPECT_TRUE(dictionaryVector->mayHaveNulls());
  EXPECT_EQ(2, dictionaryVector->getNullCount().value());
  EXPECT_FALSE(dictionaryVector->isSorted().value());
  EXPECT_EQ(3, dictionaryVector->getDistinctValueCount().value());
  EXPECT_EQ(10, dictionaryVector->getMin().value());
  EXPECT_EQ(99, dictionaryVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] == std::nullopt) {
      EXPECT_TRUE(dictionaryVector->isNullAt(i));
    } else {
      EXPECT_FALSE(dictionaryVector->isNullAt(i));
      EXPECT_EQ(*data[i], dictionaryVector->valueAt(i));
    }
  }
}

TEST_F(VectorMakerTest, isSorted) {
  // Empty and single element.
  EXPECT_TRUE(maker_.flatVector(std::vector<int64_t>())->isSorted().value());
  EXPECT_TRUE(maker_.flatVector(std::vector<int64_t>(10))->isSorted().value());

  // More variations and data types.
  EXPECT_TRUE(maker_.flatVector({-1, 0, 1, 2})->isSorted().value());
  EXPECT_FALSE(maker_.flatVector({-1, 0, 2, 1})->isSorted().value());

  EXPECT_TRUE(maker_.flatVector({-1.9, 0.0, 9.1, 10.09})->isSorted().value());
  EXPECT_FALSE(maker_.flatVector({-1.9, 0.0, -9.1, 10.09})->isSorted().value());

  EXPECT_TRUE(
      maker_.flatVector({false, false, true, true})->isSorted().value());
  EXPECT_FALSE(
      maker_.flatVector({false, false, true, false})->isSorted().value());

  // Nullable.
  EXPECT_FALSE(maker_.flatVectorNullable<int64_t>({-1, std::nullopt, 1, 2})
                   ->isSorted()
                   .value());
  EXPECT_TRUE(
      maker_.flatVectorNullable<int64_t>({-1, 0, 1, 2})->isSorted().value());
  EXPECT_TRUE(maker_.flatVectorNullable<int64_t>({std::nullopt, -1, 0, 1, 2})
                  ->isSorted()
                  .value());
  EXPECT_FALSE(maker_.flatVectorNullable<int64_t>({-1, 0, 1, 2, std::nullopt})
                   ->isSorted()
                   .value());

  // Biased.
  EXPECT_TRUE(maker_.biasVector<int64_t>({std::nullopt, 10, 13, 13, 14, 15})
                  ->isSorted()
                  .value());
  EXPECT_FALSE(maker_.biasVector<int64_t>({10, 13, std::nullopt, 15, 12, 11})
                   ->isSorted()
                   .value());

  // Sequence.
  EXPECT_TRUE(
      maker_.sequenceVector<int64_t>({std::nullopt, 10, 10, 11, 15, 15, 15})
          ->isSorted()
          .value());
  EXPECT_FALSE(maker_.sequenceVector<int64_t>({10, 10, std::nullopt, 9, 9, 9})
                   ->isSorted()
                   .value());
  // Dictionary.
  EXPECT_TRUE(
      maker_.dictionaryVector<int64_t>({std::nullopt, 10, 10, 11, 99, 99})
          ->isSorted()
          .value());
  EXPECT_FALSE(
      maker_.dictionaryVector<int64_t>({10, 9, std::nullopt, 10, 10, 10})
          ->isSorted()
          .value());
}

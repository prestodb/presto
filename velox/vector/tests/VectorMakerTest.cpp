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

#include "velox/common/memory/Memory.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using facebook::velox::test::VectorMaker;

class VectorMakerTest : public ::testing::Test {
 protected:
  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorMaker maker_{pool_.get()};
};

TEST_F(VectorMakerTest, flatVector) {
  std::vector<int64_t> data = {0, 1, 2, 3, 1024, -123456, -99, -999, -1};

  auto flatVector = maker_.flatVector<int64_t>(data);

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
  EXPECT_EQ(StringView(""), flatVector->getMin().value());
  EXPECT_EQ(StringView("world"), flatVector->getMax().value());

  for (vector_size_t i = 0; i < data.size(); i++) {
    EXPECT_EQ(data[i], std::string(flatVector->valueAt(i)));
  }
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
  EXPECT_EQ(StringView(""), flatVector->getMin().value());
  EXPECT_EQ(StringView("world"), flatVector->getMax().value());

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
  auto elementsVector = arrayVector->elements()->asFlatVector<int64_t>();

  EXPECT_FALSE(elementsVector->mayHaveNulls());

  vector_size_t idx = 0;
  for (const auto& item : data) {
    for (auto i : item) {
      EXPECT_EQ(i, elementsVector->valueAt(idx++));
    }
  }
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
  auto elementsVector = arrayVector->elements()->asFlatVector<int64_t>();

  EXPECT_TRUE(elementsVector->mayHaveNulls());
  EXPECT_EQ(3, elementsVector->getNullCount().value());

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
  auto elementsVector = arrayVector->elements()->as<RowVector>();

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
      StringView{"a"},
      StringView{"a"},
      StringView{"a"},
      std::nullopt,
      std::nullopt,
      StringView{"b"},
      std::nullopt,
      StringView{"c"},
      StringView{"c"},
  };
  auto sequenceVector = maker_.sequenceVector(data);

  EXPECT_EQ(data.size(), sequenceVector->size());
  EXPECT_TRUE(sequenceVector->mayHaveNulls());
  EXPECT_EQ(5, sequenceVector->numSequences());
  EXPECT_EQ(3, sequenceVector->getNullCount().value());
  EXPECT_FALSE(sequenceVector->isSorted().value());
  EXPECT_EQ(3, sequenceVector->getDistinctValueCount().value());
  EXPECT_EQ(StringView("a"), sequenceVector->getMin().value());
  EXPECT_EQ(StringView("c"), sequenceVector->getMax().value());

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
  EXPECT_TRUE(
      maker_.flatVector<int64_t>(std::vector<int64_t>())->isSorted().value());
  EXPECT_TRUE(
      maker_.flatVector<int64_t>(std::vector<int64_t>(10))->isSorted().value());

  // More variations and data types.
  EXPECT_TRUE(maker_.flatVector<int64_t>({-1, 0, 1, 2})->isSorted().value());
  EXPECT_FALSE(maker_.flatVector<int64_t>({-1, 0, 2, 1})->isSorted().value());

  EXPECT_TRUE(
      maker_.flatVector<double>({-1.9, 0, 9.1, 10.09})->isSorted().value());
  EXPECT_FALSE(
      maker_.flatVector<double>({-1.9, 0, -9.1, 10.09})->isSorted().value());

  EXPECT_TRUE(
      maker_.flatVector<bool>({false, false, true, true})->isSorted().value());
  EXPECT_FALSE(
      maker_.flatVector<bool>({false, false, true, false})->isSorted().value());

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

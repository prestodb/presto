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

#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::test {

class MayHaveNullsRecursiveTest : public testing::Test {
 protected:
  std::unique_ptr<velox::memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorMaker vectorMaker_{pool_.get()};

  enum class TestOptions {
    WITH_NULL_CONTAINERS,
    WITH_NULL_VALUES,
  };

  std::vector<std::optional<int32_t>> generateIntInput() {
    return generateIntInput(false);
  }

  std::vector<std::optional<int32_t>> generateIntInputWithNulls() {
    return generateIntInput(true);
  }

  ArrayVectorPtr generateArrayVector(
      const std::unordered_set<TestOptions>& testOptions = {}) {
    std::function<bool(vector_size_t)> noNulls = nullptr;

    return vectorMaker_.arrayVector<int32_t>(
        10,
        [](vector_size_t row) { return row; },
        [](vector_size_t idx) { return idx; },
        testOptions.count(TestOptions::WITH_NULL_CONTAINERS) > 0
            ? [](vector_size_t row) { return row % 5 == 0; }
            : noNulls,
        testOptions.count(TestOptions::WITH_NULL_VALUES) > 0
            ? [](vector_size_t idx) { return idx % 7 == 0; }
            : noNulls);
  }

  MapVectorPtr generateMapVector(
      const std::unordered_set<TestOptions>& testOptions = {}) {
    std::function<bool(vector_size_t)> noNulls = nullptr;

    return vectorMaker_.mapVector<int32_t, int32_t>(
        10,
        [](vector_size_t row) { return row; },
        [](vector_size_t idx) { return idx; },
        [](vector_size_t idx) { return idx % 10; },
        testOptions.count(TestOptions::WITH_NULL_CONTAINERS) > 0
            ? [&](vector_size_t row) { return row % 5 == 0; }
            : noNulls,
        testOptions.count(TestOptions::WITH_NULL_VALUES) > 0
            ? [&](vector_size_t row) { return row % 7 == 0; }
            : noNulls);
  }

 private:
  std::vector<std::optional<int32_t>> generateIntInput(bool withNulls) {
    std::vector<std::optional<int32_t>> input;
    for (int32_t i = 0; i < 10; ++i) {
      if (withNulls && i % 5 == 0) {
        input.push_back(std::nullopt);
      } else {
        input.push_back(i);
      }
    }

    return input;
  }
};

TEST_F(MayHaveNullsRecursiveTest, biasHasNulls) {
  auto biasVector = vectorMaker_.biasVector(generateIntInputWithNulls());

  ASSERT_TRUE(biasVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, biasNullFree) {
  auto biasVector = vectorMaker_.biasVector(generateIntInput());

  ASSERT_FALSE(biasVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, arrayHasNulls) {
  auto arrayVector = generateArrayVector({TestOptions::WITH_NULL_CONTAINERS});

  ASSERT_TRUE(arrayVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, arrayHasNullElements) {
  auto arrayVector = generateArrayVector({TestOptions::WITH_NULL_VALUES});

  ASSERT_TRUE(arrayVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, arrayNullFree) {
  auto arrayVector = generateArrayVector();

  ASSERT_FALSE(arrayVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, mapHasNulls) {
  auto mapVector = generateMapVector({TestOptions::WITH_NULL_CONTAINERS});

  ASSERT_TRUE(mapVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, mapHasNullValues) {
  auto mapVector = generateMapVector({TestOptions::WITH_NULL_VALUES});

  ASSERT_TRUE(mapVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, mapNullFree) {
  auto mapVector = generateMapVector();

  ASSERT_FALSE(mapVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, rowHasNulls) {
  auto rowVector = vectorMaker_.rowVector(
      {vectorMaker_.flatVectorNullable(generateIntInput()),
       generateArrayVector(),
       generateMapVector()});

  rowVector->setNull(3, true);

  ASSERT_TRUE(rowVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, rowChildrenHaveNulls) {
  auto rowVector = vectorMaker_.rowVector(
      {vectorMaker_.flatVectorNullable(generateIntInputWithNulls()),
       generateArrayVector(),
       generateMapVector()});

  ASSERT_TRUE(rowVector->mayHaveNullsRecursive());

  rowVector = vectorMaker_.rowVector(
      {vectorMaker_.flatVectorNullable(generateIntInput()),
       generateArrayVector({TestOptions::WITH_NULL_VALUES}),
       generateMapVector()});

  ASSERT_TRUE(rowVector->mayHaveNullsRecursive());

  rowVector = vectorMaker_.rowVector(
      {vectorMaker_.flatVectorNullable(generateIntInput()),
       generateArrayVector(),
       generateMapVector({TestOptions::WITH_NULL_CONTAINERS})});

  ASSERT_TRUE(rowVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, rowNullFree) {
  auto rowVector = vectorMaker_.rowVector(
      {vectorMaker_.flatVectorNullable(generateIntInput()),
       generateArrayVector(),
       generateMapVector()});

  ASSERT_FALSE(rowVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, constantNull) {
  auto constantVector = vectorMaker_.constantVector<int32_t>({std::nullopt});

  ASSERT_TRUE(constantVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, constantNullFree) {
  auto constantVector = vectorMaker_.constantVector<int32_t>({1});

  ASSERT_FALSE(constantVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, dictionaryHasNulls) {
  auto dictionaryVector =
      vectorMaker_.dictionaryVector(generateIntInputWithNulls());

  ASSERT_TRUE(dictionaryVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, dictionaryNullFree) {
  auto dictionaryVector = vectorMaker_.dictionaryVector(generateIntInput());

  ASSERT_FALSE(dictionaryVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, flatHasNulls) {
  auto flatVector =
      vectorMaker_.flatVectorNullable(generateIntInputWithNulls());

  ASSERT_TRUE(flatVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, flatNullFree) {
  auto flatVector = vectorMaker_.flatVectorNullable(generateIntInput());

  ASSERT_FALSE(flatVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, lazyHasNulls) {
  auto lazyVector = vectorMaker_.lazyFlatVector<int32_t>(
      10,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 3 == 0; });

  ASSERT_TRUE(lazyVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, lazyNullFree) {
  auto lazyVector = vectorMaker_.lazyFlatVector<int32_t>(
      10, [](vector_size_t row) { return row; });

  ASSERT_FALSE(lazyVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, sequenceHasNulls) {
  auto sequenceVector =
      vectorMaker_.sequenceVector(generateIntInputWithNulls());

  ASSERT_TRUE(sequenceVector->mayHaveNullsRecursive());
}

TEST_F(MayHaveNullsRecursiveTest, sequenceNullFree) {
  auto sequenceVector = vectorMaker_.sequenceVector(generateIntInput());

  ASSERT_FALSE(sequenceVector->mayHaveNullsRecursive());
}
} // namespace facebook::velox::test

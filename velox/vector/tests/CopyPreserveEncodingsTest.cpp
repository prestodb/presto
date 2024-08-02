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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

class CopyPreserveEncodingsTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  VectorMaker vectorMaker_{pool_.get()};

  enum class TestOptions {
    WITH_NULLS,
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
        testOptions.count(TestOptions::WITH_NULLS) > 0
            ? [](vector_size_t row) { return row % 5 == 0; }
            : noNulls,
        testOptions.count(TestOptions::WITH_NULLS) > 0
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
        testOptions.count(TestOptions::WITH_NULLS) > 0
            ? [&](vector_size_t row) { return row % 5 == 0; }
            : noNulls,
        testOptions.count(TestOptions::WITH_NULLS) > 0
            ? [&](vector_size_t row) { return row % 7 == 0; }
            : noNulls);
  }

  template <TypeKind kind>
  void validateCopyPreserveEncodingsSimpleVector(
      const VectorPtr& base,
      const VectorPtr& copy) {
    using T = typename TypeTraits<kind>::NativeType;

    switch (base->encoding()) {
      case VectorEncoding::Simple::BIASED: {
        auto* baseBiased = base->as<BiasVector<T>>();
        auto* copyBiased = copy->as<BiasVector<T>>();
        ASSERT_NE(baseBiased->values(), copyBiased->values());
        break;
      }
      case VectorEncoding::Simple::CONSTANT: {
        auto* baseConstant = base->as<ConstantVector<T>>();
        auto* copyConstant = copy->as<ConstantVector<T>>();
        if (baseConstant->valueVector() != nullptr) {
          validateCopyPreserveEncodings(
              baseConstant->valueVector(), copyConstant->valueVector());
        }
        break;
      }
      case VectorEncoding::Simple::DICTIONARY: {
        auto* baseDictionary = base->as<DictionaryVector<T>>();
        auto* copyDictionary = copy->as<DictionaryVector<T>>();
        validateCopyPreserveEncodings(
            baseDictionary->valueVector(), copyDictionary->valueVector());
        ASSERT_NE(baseDictionary->indices(), copyDictionary->indices());
        break;
      }
      case VectorEncoding::Simple::FLAT: {
        auto* baseFlat = base->as<FlatVector<T>>();
        auto* copyFlat = copy->as<FlatVector<T>>();
        ASSERT_NE(baseFlat->values(), copyFlat->values());
        break;
      }
      case VectorEncoding::Simple::SEQUENCE: {
        auto* baseSequence = base->as<SequenceVector<T>>();
        auto* copySequence = copy->as<SequenceVector<T>>();
        validateCopyPreserveEncodings(
            baseSequence->valueVector(), copySequence->valueVector());
        ASSERT_NE(
            baseSequence->getSequenceLengths(),
            copySequence->getSequenceLengths());
        break;
      }
      default:
        VELOX_FAIL(
            "{} is not a supported SimpleVector encoding", base->encoding());
    }
  }

  void validateCopyPreserveEncodings(
      const VectorPtr& base,
      const VectorPtr& copy) {
    ASSERT_NE(base, copy);
    ASSERT_EQ(base->encoding(), copy->encoding());

    if (base->nulls() == nullptr) {
      ASSERT_EQ(copy->nulls(), nullptr);
    } else {
      ASSERT_NE(base->nulls(), copy->nulls());
    }

    switch (base->encoding()) {
      case VectorEncoding::Simple::ARRAY: {
        auto* baseArray = base->as<ArrayVector>();
        auto* copyArray = copy->as<ArrayVector>();
        ASSERT_NE(baseArray->offsets(), copyArray->offsets());
        ASSERT_NE(baseArray->sizes(), copyArray->sizes());
        validateCopyPreserveEncodings(
            baseArray->elements(), copyArray->elements());
        break;
      }
      case VectorEncoding::Simple::ROW: {
        auto* baseRow = base->as<RowVector>();
        auto* copyRow = copy->as<RowVector>();
        ASSERT_EQ(baseRow->childrenSize(), copyRow->childrenSize());
        for (int i = 0; i < baseRow->childrenSize(); i++) {
          validateCopyPreserveEncodings(
              baseRow->childAt(i), copyRow->childAt(i));
        }
        break;
      }
      case VectorEncoding::Simple::MAP: {
        auto* baseMap = base->as<MapVector>();
        auto* copyMap = copy->as<MapVector>();
        ASSERT_NE(baseMap->offsets(), copyMap->offsets());
        ASSERT_NE(baseMap->sizes(), copyMap->sizes());
        validateCopyPreserveEncodings(baseMap->mapKeys(), copyMap->mapKeys());
        validateCopyPreserveEncodings(
            baseMap->mapValues(), copyMap->mapValues());
        break;
      }
      case VectorEncoding::Simple::BIASED:
      case VectorEncoding::Simple::CONSTANT:
      case VectorEncoding::Simple::DICTIONARY:
      case VectorEncoding::Simple::FLAT:
      case VectorEncoding::Simple::SEQUENCE:
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            validateCopyPreserveEncodingsSimpleVector,
            base->typeKind(),
            base,
            copy);
        break;
      default:
        VELOX_FAIL("{} is not a supported encoding", base->encoding());
    }
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

TEST_F(CopyPreserveEncodingsTest, biasHasNulls) {
  auto biasVector = vectorMaker_.biasVector(generateIntInputWithNulls());
  auto copy = biasVector->copyPreserveEncodings();

  assertEqualVectors(biasVector, copy);
  validateCopyPreserveEncodings(biasVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, biasNoNulls) {
  auto biasVector = vectorMaker_.biasVector(generateIntInput());
  auto copy = biasVector->copyPreserveEncodings();

  assertEqualVectors(biasVector, copy);
  validateCopyPreserveEncodings(biasVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, arrayHasNulls) {
  auto arrayVector = generateArrayVector({TestOptions::WITH_NULLS});
  auto copy = arrayVector->copyPreserveEncodings();

  assertEqualVectors(arrayVector, copy);
  validateCopyPreserveEncodings(arrayVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, arrayNoNulls) {
  auto arrayVector = generateArrayVector();
  auto copy = arrayVector->copyPreserveEncodings();

  assertEqualVectors(arrayVector, copy);
  validateCopyPreserveEncodings(arrayVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, mapHasNulls) {
  auto mapVector = generateMapVector({TestOptions::WITH_NULLS});
  auto copy = mapVector->copyPreserveEncodings();

  assertEqualVectors(mapVector, copy);
  validateCopyPreserveEncodings(mapVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, mapNoNulls) {
  auto mapVector = generateMapVector();
  auto copy = mapVector->copyPreserveEncodings();

  assertEqualVectors(mapVector, copy);
  validateCopyPreserveEncodings(mapVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, rowHasNulls) {
  auto rowVector = vectorMaker_.rowVector(
      {vectorMaker_.flatVectorNullable(generateIntInput()),
       generateArrayVector(),
       generateMapVector()});

  rowVector->setNull(3, true);
  auto copy = rowVector->copyPreserveEncodings();

  assertEqualVectors(rowVector, copy);
  validateCopyPreserveEncodings(rowVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, rowNoNulls) {
  auto rowVector = vectorMaker_.rowVector(
      {vectorMaker_.flatVectorNullable(generateIntInput()),
       generateArrayVector(),
       generateMapVector()});
  auto copy = rowVector->copyPreserveEncodings();

  assertEqualVectors(rowVector, copy);
  validateCopyPreserveEncodings(rowVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, constantNull) {
  auto constantVector = vectorMaker_.constantVector<int32_t>({std::nullopt});
  auto copy = constantVector->copyPreserveEncodings();

  assertEqualVectors(constantVector, copy);
  validateCopyPreserveEncodings(constantVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, constantNoNulls) {
  auto constantVector = vectorMaker_.constantVector<int32_t>({1});
  auto copy = constantVector->copyPreserveEncodings();

  assertEqualVectors(constantVector, copy);
  validateCopyPreserveEncodings(constantVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, dictionaryHasNulls) {
  auto dictionaryVector =
      vectorMaker_.dictionaryVector(generateIntInputWithNulls());
  auto copy = dictionaryVector->copyPreserveEncodings();

  assertEqualVectors(dictionaryVector, copy);
  validateCopyPreserveEncodings(dictionaryVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, dictionaryNoNulls) {
  auto dictionaryVector = vectorMaker_.dictionaryVector(generateIntInput());
  auto copy = dictionaryVector->copyPreserveEncodings();

  assertEqualVectors(dictionaryVector, copy);
  validateCopyPreserveEncodings(dictionaryVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, flatHasNulls) {
  auto flatVector =
      vectorMaker_.flatVectorNullable(generateIntInputWithNulls());
  auto copy = flatVector->copyPreserveEncodings();

  assertEqualVectors(flatVector, copy);
  validateCopyPreserveEncodings(flatVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, flatNoNulls) {
  auto flatVector = vectorMaker_.flatVectorNullable(generateIntInput());
  auto copy = flatVector->copyPreserveEncodings();

  assertEqualVectors(flatVector, copy);
  validateCopyPreserveEncodings(flatVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, lazyNoNulls) {
  auto lazyVector = vectorMaker_.lazyFlatVector<int32_t>(
      10, [](vector_size_t row) { return row; });

  VELOX_ASSERT_THROW(
      lazyVector->copyPreserveEncodings(),
      "copyPreserveEncodings not defined for LazyVector");
}

TEST_F(CopyPreserveEncodingsTest, sequenceHasNulls) {
  auto sequenceVector =
      vectorMaker_.sequenceVector(generateIntInputWithNulls());
  auto copy = sequenceVector->copyPreserveEncodings();

  assertEqualVectors(sequenceVector, copy);
  validateCopyPreserveEncodings(sequenceVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, sequenceNoNulls) {
  auto sequenceVector = vectorMaker_.sequenceVector(generateIntInput());
  auto copy = sequenceVector->copyPreserveEncodings();

  assertEqualVectors(sequenceVector, copy);
  validateCopyPreserveEncodings(sequenceVector, copy);
}

TEST_F(CopyPreserveEncodingsTest, newMemoryPool) {
  auto dictionaryVector = vectorMaker_.dictionaryVector(generateIntInput());

  auto sourcePool = dictionaryVector->pool();
  auto targetPool = memory::memoryManager()->addLeafPool();
  auto preCopySrcMemory = sourcePool->usedBytes();
  ASSERT_EQ(0, targetPool->usedBytes());

  auto copy = dictionaryVector->copyPreserveEncodings(targetPool.get());
  assertEqualVectors(dictionaryVector, copy);
  validateCopyPreserveEncodings(dictionaryVector, copy);

  EXPECT_EQ(preCopySrcMemory, sourcePool->usedBytes());
  EXPECT_EQ(preCopySrcMemory, targetPool->usedBytes());
}
} // namespace facebook::velox::test

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

#include <algorithm>

#include <fmt/core.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <folly/Demangle.h>
#include <folly/FileUtil.h>
#include <folly/dynamic.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/BuilderTypeUtils.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/tests/SimpleVectorTestHelper.h"
#include "velox/vector/tests/VectorTestUtils.h"

namespace facebook::velox::test {

using namespace facebook::velox;

namespace {
template <typename T>
std::string type_name() {
  return folly::demangle(typeid(T)).c_str();
}

template <typename T>
bool isValidBiasOrDictionary(const VectorEncoding::Simple& vectorType) {
  if ((vectorType == VectorEncoding::Simple::BIASED && !admitsBias<T>()) ||
      (vectorType == VectorEncoding::Simple::DICTIONARY &&
       !admitsDictionary<T>())) {
    return false;
  }

  return true;
}

void assertIsAscii(
    const SimpleVectorPtr<StringView>& vector,
    const SelectivityVector& rows,
    bool expected) {
  auto ascii = vector->isAscii(rows);
  ASSERT_TRUE(ascii.has_value());
  ASSERT_EQ(ascii.value(), expected);
};

} // namespace

class SimpleVectorNonParameterizedTest : public SimpleVectorTest {
 protected:
  ExpectedData<StringView> stringData_ =
      {"àá"_sv, "abc"_sv, "xyz"_sv, "mno"_sv, "wv"_sv, "abc"_sv, "xyz"_sv};
};

TEST_F(SimpleVectorNonParameterizedTest, ConstantVectorTest) {
  ExpectedData<int64_t> expected(10, 123456);
  auto vector =
      maker_.encodedVector(VectorEncoding::Simple::CONSTANT, expected);
  assertVectorAndProperties(expected, vector);
}

TEST_F(SimpleVectorNonParameterizedTest, ConstantVectorTestIsNull) {
  ExpectedData<int64_t> expected(10, std::nullopt);
  auto vector =
      maker_.encodedVector(VectorEncoding::Simple::CONSTANT, expected);
  assertVectorAndProperties(expected, vector);
}

// Single Entry Tests
TEST_F(SimpleVectorNonParameterizedTest, SingleEntryIntegerVector) {
  for (auto t : kFullValueTypes) {
    LOG(INFO) << "Running:" << t;
    ExpectedData<int64_t> expected = {{123456}};
    auto vector = maker_.encodedVector(t, expected);
    assertVectorAndProperties(expected, vector);
  }
}

TEST_F(SimpleVectorNonParameterizedTest, SingleNullEntryIntegerVector) {
  for (auto t : kFullValueTypes) {
    LOG(INFO) << "Running:" << t;
    ExpectedData<int64_t> expected = {{std::nullopt}};
    auto vector = maker_.encodedVector(t, expected);
    assertVectorAndProperties(expected, vector);
  }
}

TEST_F(SimpleVectorNonParameterizedTest, roundTripNullFirstThenSortedAsc) {
  for (auto t : kNonConstantTypes) {
    LOG(INFO) << "Running:" << t;
    ExpectedData<int64_t> expected = {
        std::nullopt, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
    auto vector = maker_.encodedVector(t, expected);
    assertVectorAndProperties(expected, vector);
  }
}

TEST_F(SimpleVectorNonParameterizedTest, roundTripSortedDescThenNullLastData) {
  for (auto t : kNonConstantTypes) {
    LOG(INFO) << "Running:" << t;
    ExpectedData<int64_t> expected = {
        1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, std::nullopt};
    auto vector = maker_.encodedVector(t, expected);
    assertVectorAndProperties(expected, vector);
  }
}

TEST_F(SimpleVectorNonParameterizedTest, BasicRoundTrip) {
  auto expected =
      genTestDataWithSequences<int64_t>(1000, 100, true, true, 100, 10);
  auto vector = maker_.flatVectorNullable<int64_t>(expected.data());
  assertVectorAndProperties<int64_t>(expected.data(), vector);
}

TEST_F(SimpleVectorNonParameterizedTest, ThreadSafeAsciiCompute) {
  std::vector<std::thread> tasks;
  SelectivityVector rows(10000);
  VectorPtr vectorBase;
  BaseVector::ensureWritable(rows, VARCHAR(), pool_.get(), vectorBase);
  auto vector = vectorBase->as<FlatVector<StringView>>();
  for (int i = 0; i < 10000; i++) {
    vector->set(i, "dsfdsfsdfdsfds"_sv);
  }

  for (int i = 0; i < 100; i++) {
    tasks.emplace_back([&]() {
      for (int j = 0; j < 100; j++) {
        vector->invalidateIsAscii();
        vector->computeAndSetIsAscii(rows);
        for (int k = 0; k < vector->size(); k++) {
          ASSERT_TRUE(vector->isAscii(k));
        }
      }
    });
  }
  for (auto& thread : tasks) {
    thread.join();
  }
}

// Only signed types are supported.
using SimpleTypes = ::testing::
    Types<int8_t, int16_t, int32_t, int64_t, double, StringView, bool>;

template <typename T>
class SimpleVectorTypedTest : public SimpleVectorTest {
 protected:
  // This method tests vectors's valueAt(i) and isNullAt(i)
  // methods.
  void runSimpleAccessTest() {
    for (auto& vectorType : kAllTypes) {
      LOG(INFO) << "VectorType: " << vectorType;
      if (!isValidBiasOrDictionary<T>(vectorType)) {
        continue;
      }

      auto generatedData = generateData(vectorType);
      const auto& data = generatedData.data();
      auto vector = maker_.encodedVector(vectorType, data);

      ASSERT_EQ(data.size(), vector->size());

      for (size_t i = 0; i < vector->size(); ++i) {
        const bool expectNull = (data[i] == std::nullopt);
        ASSERT_EQ(vector->isNullAt(i), expectNull)
            << "IsNullAt doesn't match at index " << i
            << " for vectorType: " << vectorType;

        // if value is not null validate the value
        if (!expectNull) {
          ASSERT_EQ(vector->valueAt(i), *data[i])
              << "Values don't match at index " << i
              << " for vectorType: " << vectorType;
        }
      }
    }
  }

  size_t getCardinality(const VectorEncoding::Simple& vecType, size_t size) {
    switch (vecType) {
      case VectorEncoding::Simple::CONSTANT:
        return 1;
      case VectorEncoding::Simple::DICTIONARY:
      case VectorEncoding::Simple::SEQUENCE:
        return 50;
      default:
        return size;
    }
  }

  VectorGeneratedData<T> generateData(
      const VectorEncoding::Simple& vectorType) {
    const size_t size = 500;
    size_t cardinality = getCardinality(vectorType, size);
    return genTestDataWithSequences<T>(
        size /*count*/,
        cardinality /*cardinality*/,
        false /*sorted*/,
        true /*null*/,
        vectorType == VectorEncoding::Simple::SEQUENCE ? 50
                                                       : 0 /*sequenceCount*/,
        vectorType == VectorEncoding::Simple::SEQUENCE ? 10 : 0
        /*sequenceLength*/,
        vectorType != VectorEncoding::Simple::BIASED /*useFullTypeRange*/,
        1001 /*seed*/);
  }
};

namespace {
std::vector<VectorEncoding::Simple> kAsciiEncodings = {
    VectorEncoding::Simple::DICTIONARY,
    VectorEncoding::Simple::FLAT,
    VectorEncoding::Simple::SEQUENCE};
}

// StringView Ascii Tests
TEST_F(SimpleVectorNonParameterizedTest, computeAscii) {
  for (auto encoding : kAsciiEncodings) {
    LOG(INFO) << "Running:" << encoding;

    auto vector = maker_.encodedVector(encoding, stringData_);
    SelectivityVector all(stringData_.size());
    vector->computeAndSetIsAscii(all);

    ASSERT_TRUE(vector->isAscii(all).has_value());
    ASSERT_FALSE(vector->isAscii(all).value());

    // Exclude the first row, the only row with non-ASCII characters.
    SelectivityVector asciiSubset(stringData_.size());
    asciiSubset.setValid(0, false);
    asciiSubset.updateBounds();

    vector->invalidateIsAscii();
    vector->computeAndSetIsAscii(asciiSubset);

    ASSERT_TRUE(vector->isAscii(asciiSubset).has_value());
    ASSERT_TRUE(vector->isAscii(asciiSubset).value());
  }
}

TEST_F(SimpleVectorNonParameterizedTest, isAscii) {
  for (auto encoding : kAsciiEncodings) {
    LOG(INFO) << "Running:" << encoding;

    auto vector = maker_.encodedVector(encoding, stringData_);
    SelectivityVector all(stringData_.size());
    vector->computeAndSetIsAscii(all);

    bool ascii = vector->isAscii(all).value();
    ASSERT_FALSE(ascii);

    // Clear asciiness and compute asciiness only for 2nd row
    // Then ask for asciiness for some other row.
    vector->invalidateIsAscii();
    SelectivityVector first(stringData_.size(), false);
    first.setValid(1, true);
    first.updateBounds();
    vector->computeAndSetIsAscii(first);

    SelectivityVector other(stringData_.size(), false);
    other.setValid(2, true);
    other.updateBounds();

    ASSERT_FALSE(vector->isAscii(other).has_value());
    ASSERT_TRUE(vector->isAscii(first).value());

    // Give it a selectivity vector with larger bounds than supported.
    vector->invalidateIsAscii();
    vector->computeAndSetIsAscii(all);

    SelectivityVector larger(stringData_.size() + 1, false);
    larger.setValid(stringData_.size(), true);
    larger.updateBounds();
    ASSERT_FALSE(vector->isAscii(larger).has_value());

    // Selectivity Vector which is empty.
    SelectivityVector empty(stringData_.size(), false);
    EXPECT_THROW(vector->isAscii(empty).has_value(), VeloxException);
  }
}

TEST_F(SimpleVectorNonParameterizedTest, isAsciiIndex) {
  for (auto encoding : kAsciiEncodings) {
    LOG(INFO) << "Running:" << encoding;

    auto vector = maker_.encodedVector(encoding, stringData_);
    SelectivityVector all(stringData_.size());
    vector->computeAndSetIsAscii(all);

    assertIsAscii(vector, all, false);

    ASSERT_FALSE(vector->isAscii(stringData_.size() + 1).has_value());
    EXPECT_THROW(vector->isAscii(-1).has_value(), VeloxException);
  }
}

TEST_F(SimpleVectorNonParameterizedTest, isAsciiSourceRows) {
  for (auto encoding : kAsciiEncodings) {
    LOG(INFO) << "Running:" << encoding;

    auto vector = maker_.encodedVector(encoding, stringData_);
    SelectivityVector all(stringData_.size());
    vector->computeAndSetIsAscii(all);

    assertIsAscii(vector, all, false);

    vector_size_t sourceMappings[] = {0, 2, 1, 3, 5, 6, 4};

    auto ascii = vector->isAscii(all, sourceMappings);
    ASSERT_TRUE(ascii.has_value());
    ASSERT_FALSE(ascii.value());

    vector->setAllIsAscii(true);

    ascii = vector->isAscii(all, sourceMappings);
    ASSERT_TRUE(ascii.has_value());
    ASSERT_TRUE(ascii.value());

    // Ensure we return nullopt if we arent a subset.
    SelectivityVector some(all.size(), false);
    some.setValid(0, true);
    some.updateBounds();
    vector->invalidateIsAscii();
    vector->setIsAscii(true, some);
    ascii = vector->isAscii(all, sourceMappings);
    ASSERT_FALSE(ascii.has_value());
  }
}

TEST_F(SimpleVectorNonParameterizedTest, invalidateIsAscii) {
  for (auto encoding : kAsciiEncodings) {
    LOG(INFO) << "Running:" << encoding;

    auto vector = maker_.encodedVector(encoding, stringData_);
    SelectivityVector all(stringData_.size());
    vector->computeAndSetIsAscii(all);

    assertIsAscii(vector, all, false);

    vector->invalidateIsAscii();
    ASSERT_FALSE(vector->isAscii(all).has_value());
  }
}

TEST_F(SimpleVectorNonParameterizedTest, setAscii) {
  for (auto encoding : kAsciiEncodings) {
    LOG(INFO) << "Running:" << encoding;
    auto vector = maker_.encodedVector(encoding, stringData_);

    SelectivityVector all(stringData_.size());
    vector->computeAndSetIsAscii(all);
    assertIsAscii(vector, all, false);

    vector->invalidateIsAscii();
    vector->setIsAscii(true, all);
    assertIsAscii(vector, all, true);

    // Subset of all.
    SelectivityVector some(stringData_.size(), false);
    some.setValid(1, true);
    some.updateBounds();
    vector->setIsAscii(true, some);

    assertIsAscii(vector, all, true);

    some.clearAll();
    some.setValid(0, false);
    some.updateBounds();
    vector->setIsAscii(false, some);

    assertIsAscii(vector, all, false);
  }
}

VELOX_TYPED_TEST_SUITE(SimpleVectorTypedTest, SimpleTypes);

TYPED_TEST(SimpleVectorTypedTest, vectorAccessTest) {
  this->runSimpleAccessTest();
}

template <typename T>
class SimpleVectorBinaryTypedTest : public SimpleVectorTest {
 protected:
  using VisitorFunc = std::function<void(SimpleVector<T>*, SimpleVector<T>*)>;

  void runTest(const VisitorFunc& visitor) {
    int32_t count = 5;
    for (auto& typeA : kAllTypes) {
      LOG(INFO) << "TypeA: " << typeA;
      if (!isValidBiasOrDictionary<T>(typeA)) {
        continue;
      }

      const bool constA = (typeA == VectorEncoding::Simple::CONSTANT);
      int32_t cardinalityA = constA ? 1 : count;

      auto data = genTestDataWithSequences<T>(
          count,
          cardinalityA,
          false /* isSorted */,
          true /* includeNulls */,
          0 /* sequenceCount */,
          0 /* sequenceLength */,
          false /* useFullTypeRange */,
          2006 /* seed */);
      auto vectorA = maker_.encodedVector(typeA, data.data());

      for (auto& typeB : kAllTypes) {
        LOG(INFO) << "TypeB: " << typeB;
        if (!isValidBiasOrDictionary<T>(typeB) ||
            (typeB == VectorEncoding::Simple::BIASED && constA)) {
          continue;
        }

        const bool constB = (typeB == VectorEncoding::Simple::CONSTANT);
        std::vector<std::optional<T>> dataB;
        dataB.reserve(vectorA->size());

        for (size_t i = 0; i < count; ++i) {
          // construct b as a constant array same as A or as the reverse of A
          auto pos = constB ? 0 : count - i - 1;
          if (vectorA->isNullAt(pos)) {
            dataB.push_back(std::nullopt);
          } else {
            dataB.push_back(vectorA->valueAt(pos));
          }
        }
        auto vectorB = maker_.encodedVector(typeB, dataB);
        visitor(vectorA.get(), vectorB.get());
      }
    }
  }
};
VELOX_TYPED_TEST_SUITE(SimpleVectorBinaryTypedTest, SimpleTypes);

TYPED_TEST(SimpleVectorBinaryTypedTest, equalValueAt) {
  LOG(INFO) << "equalValueAt: " << type_name<TypeParam>();
  auto test = [](SimpleVector<TypeParam>* vectorA,
                 size_t indexA,
                 SimpleVector<TypeParam>* vectorB,
                 size_t indexB) {
    return (vectorA->isNullAt(indexA) && vectorB->isNullAt(indexB)) ||
        (!vectorA->isNullAt(indexA) && !vectorB->isNullAt(indexB) &&
         vectorA->valueAt(indexA) == vectorB->valueAt(indexB));
  };

  auto equalsTest = [&test](
                        SimpleVector<TypeParam>* vectorA,
                        SimpleVector<TypeParam>* vectorB) {
    // walk both forward
    for (size_t i = 0; i < vectorA->size(); ++i) {
      if (test(vectorA, i, vectorB, i)) {
        EXPECT_TRUE(vectorA->equalValueAt(vectorB, i, i));
        EXPECT_TRUE(vectorB->equalValueAt(vectorA, i, i));
      } else {
        EXPECT_FALSE(vectorA->equalValueAt(vectorB, i, i));
        EXPECT_FALSE(vectorB->equalValueAt(vectorA, i, i));
      }
    }

    // walk in reverse of each other
    for (size_t i = 0; i < vectorA->size(); ++i) {
      size_t posB = vectorA->size() - i - 1;
      if (test(vectorA, i, vectorB, posB)) {
        EXPECT_TRUE(vectorA->equalValueAt(vectorB, i, posB));
        EXPECT_TRUE(vectorB->equalValueAt(vectorA, posB, i));
      } else {
        EXPECT_FALSE(vectorA->equalValueAt(vectorB, i, posB));
        EXPECT_FALSE(vectorB->equalValueAt(vectorA, posB, i));
      }
    }
  };

  this->runTest(equalsTest);
}

template <typename T>
class SimpleVectorUnaryTypedTest : public SimpleVectorTest {
 protected:
  using VisitorFunc = std::function<void(SimpleVector<T>*)>;

  void runTest(const VisitorFunc& visitor) {
    int32_t count = 5;
    for (auto& type : kAllTypes) {
      LOG(INFO) << "Type: " << type;
      if ((type == VectorEncoding::Simple::BIASED && !admitsBias<T>()) ||
          (type == VectorEncoding::Simple::DICTIONARY &&
           !admitsDictionary<T>())) {
        continue;
      }

      const bool constant = (type == VectorEncoding::Simple::CONSTANT);
      int32_t cardinality = constant ? 1 : count;

      auto data = genTestDataWithSequences<T>(
          count,
          cardinality,
          false /* isSorted */,
          true /* includeNulls */,
          0 /* sequenceCount */,
          0 /* sequenceLength */,
          false /* useFullTypeRange */,
          2007 /* seed */);
      auto vector = maker_.encodedVector(type, data.data());
      visitor(vector.get());
    }
  }
};
VELOX_TYPED_TEST_SUITE(SimpleVectorUnaryTypedTest, SimpleTypes);

TYPED_TEST(SimpleVectorUnaryTypedTest, hashAll) {
  LOG(INFO) << "hashAll: " << type_name<TypeParam>();
  folly::hasher<TypeParam> hasher;
  auto hashTest = [&hasher](SimpleVector<TypeParam>* vector) {
    auto hashes = vector->hashAll();
    for (size_t i = 0; i < vector->size(); ++i) {
      auto expected = vector->isNullAt(i) ? BaseVector::kNullHash
                                          : hasher(vector->valueAt(i));
      EXPECT_EQ(hashes->valueAt(i), expected);
    }
  };
  this->runTest(hashTest);
}

template <typename T>
class SimpleVectorCompareTest : public SimpleVectorTest {
 protected:
  void runTest(const CompareFlags& flags) {
    ExpectedData<T> input = {
        max(), std::nullopt, nan(), lowest(), inf(), -1, 1, 0, max(), lowest()};
    auto vector = maker_.encodedVector(VectorEncoding::Simple::FLAT, input);
    std::vector<int> indices(input.size());
    for (int i = 0; i < input.size(); ++i) {
      indices[i] = i;
    }
    std::sort(indices.begin(), indices.end(), [&](int a, int b) {
      return vector->compare(vector.get(), a, b, flags) < 0;
    });
    ExpectedData<T> expected = {
        std::nullopt, lowest(), lowest(), -1, 0, 1, max(), max(), inf(), nan()};
    if (!flags.ascending) {
      std::reverse(expected.begin() + 1, expected.end());
    }
    if (!flags.nullsFirst) {
      // Move null to end of vector.
      expected.erase(expected.begin());
      expected.push_back(std::nullopt);
    }
    ExpectedData<T> actual;
    for (int i = 0; i < input.size(); ++i) {
      std::optional<T> v = std::nullopt;
      if (not vector->isNullAt(indices[i])) {
        v = vector->valueAt(indices[i]);
      }
      actual.push_back(v);
    }
    assertVector(
        expected, maker_.encodedVector(VectorEncoding::Simple::FLAT, actual));
  }

  T max() {
    return std::numeric_limits<T>::max();
  }

  T lowest() {
    return std::numeric_limits<T>::lowest();
  }

  T inf() {
    if constexpr (std::is_floating_point<T>::value) {
      return std::numeric_limits<T>::infinity();
    }
    return std::numeric_limits<T>::max();
  }

  T nan() {
    if constexpr (std::is_floating_point<T>::value) {
      return std::numeric_limits<T>::quiet_NaN();
    }
    return std::numeric_limits<T>::max();
  }
};

using NumericTypes =
    ::testing::Types<int8_t, int16_t, int32_t, int64_t, float, double>;
VELOX_TYPED_TEST_SUITE(SimpleVectorCompareTest, NumericTypes);

TYPED_TEST(SimpleVectorCompareTest, compareAscNullsFirst) {
  this->runTest({/*nullsFirst=*/true, /*ascending=*/true});
}

TYPED_TEST(SimpleVectorCompareTest, compareAscNullsLast) {
  this->runTest({/*nullsFirst=*/false, /*ascending=*/true});
}

TYPED_TEST(SimpleVectorCompareTest, compareDescNullsFirst) {
  this->runTest({/*nullsFirst=*/true, /*ascending=*/false});
}

TYPED_TEST(SimpleVectorCompareTest, compareDescNullsLast) {
  this->runTest({/*nullsFirst=*/false, /*ascending=*/false});
}

template <typename T, int32_t offset>
inline T simd256_extract_value(xsimd::batch<T> simdValue) {
  if constexpr (std::is_same_v<T, bool>) {
    static_assert(offset < 256);
    auto byte = xsimd::batch<uint8_t>(simdValue).get(offset / 8);
    return byte & (1 << (offset % 8));
  } else if constexpr (std::is_integral_v<T>) {
    static_assert(offset < xsimd::batch<T>::size);
    return simdValue.get(offset);
  } else {
    VELOX_UNSUPPORTED(
        "Invalid simd type - offset {} - type {}",
        offset,
        folly::demangle(typeid(T)));
  }
}

// We use this class to determine offset i in compilation time to empower us
// to utilize simd256_extract_value<T, i>(simdBuffer).
template <typename T, int i>
struct AssertSimdElement {
  static void eq(
      const std::vector<std::optional<T>>& expected,
      xsimd::batch<T> simdBuffer,
      size_t base) {
    static_assert(i >= 0);
    if (base + i < expected.size()) {
      auto& val = expected[base + i];

      if (val != std::nullopt) {
        auto actualSimdValue = simd256_extract_value<T, i>(simdBuffer);
        EXPECT_EQ(*val, actualSimdValue);
      }
    }
    if constexpr (i - 1 >= 0) {
      AssertSimdElement<T, i - 1>::eq(expected, simdBuffer, base);
    }
  }
};

template <VectorEncoding::Simple encode, typename T>
struct CanSimd {
  constexpr explicit operator bool() const {
    if constexpr (encode == VectorEncoding::Simple::BIASED) {
      return BiasVector<T>::can_simd;
    } else if constexpr (encode == VectorEncoding::Simple::CONSTANT) {
      return ConstantVector<T>::can_simd;
    } else if constexpr (encode == VectorEncoding::Simple::DICTIONARY) {
      return DictionaryVector<T>::can_simd;
    } else if constexpr (encode == VectorEncoding::Simple::FLAT) {
      return FlatVector<T>::can_simd;
    } else {
      static_assert(encode == VectorEncoding::Simple::SEQUENCE);
      return SequenceVector<T>::can_simd;
    }
  }
};

template <typename T>
xsimd::batch<T> loadSIMDValueBufferAt(
    const SimpleVector<T>* outVector,
    size_t byteOffset) {
  switch (outVector->encoding()) {
    case VectorEncoding::Simple::BIASED:
      return dynamic_cast<const BiasVector<T>*>(outVector)
          ->loadSIMDValueBufferAt(byteOffset);
    case VectorEncoding::Simple::CONSTANT:
      return dynamic_cast<const ConstantVector<T>*>(outVector)
          ->loadSIMDValueBufferAt(byteOffset);
    case VectorEncoding::Simple::DICTIONARY:
      return dynamic_cast<const DictionaryVector<T>*>(outVector)
          ->loadSIMDValueBufferAt(byteOffset);
    case VectorEncoding::Simple::FLAT:
      return dynamic_cast<const FlatVector<T>*>(outVector)
          ->loadSIMDValueBufferAt(byteOffset);
    case VectorEncoding::Simple::SEQUENCE:
      return dynamic_cast<const SequenceVector<T>*>(outVector)
          ->loadSIMDValueBufferAt(byteOffset);
    default:
      VELOX_FAIL("Invalid vector encoding - {}", outVector->encoding());
  }
}

template <typename T>
class SimpleVectorSimdTypedTest : public SimpleVectorTest {
 protected:
  template <typename VectorEncoding::Simple encode>
  void runTest() {
    if constexpr (std::is_integral_v<T> && CanSimd<encode, T>()) {
      LOG(INFO) << "VectorType: " << encode << ", SimdType: " << type_name<T>();
      // Try more numbers to cover several edge cases of all vector's
      // loadSIMDValueBufferAt() function
      std::vector<int32_t> countsToTest{
          1,
          2,
          3,
          4,
          7,
          8,
          9,
          15,
          16,
          100,
          255,
          256,
          // testing with up to (CPU_register_width (256) + 1) elements
          // for SIMD vector upper bound check
          257,
          // testing with a large upper bound, which previously threw signed
          // integer overflow at SequenceVector::loadSIMDValueBufferAt()
          2000,
          // another large upper bound, which previously threw
          // out-of-bound error at DictionaryVector::loadSIMDValueBufferAt()
          2047,
          // another large upper bound, which previously passed unit test
          2048};

      for (int32_t count : countsToTest) {
        if (encode == VectorEncoding::Simple::BIASED && count < 3) {
          continue;
        }
        // Make the size of cardinality smaller than length so that a few kinds
        // of vectors can perform their own encodings. e.g. Sequence and
        // dictionary vectors.
        int32_t cardinality =
            encode == VectorEncoding::Simple::CONSTANT ? 1 : count;
        auto expected = genTestDataWithSequences<T>(
            count,
            cardinality,
            false /* isSorted */,
            true /* includeNulls */,
            encode == VectorEncoding::Simple::SEQUENCE ? 40
                                                       : 0 /* sequenceCount */,
            encode == VectorEncoding::Simple::SEQUENCE ? 50
                                                       : 0 /* sequenceLength */,
            encode != VectorEncoding::Simple::BIASED /* useFullTypeRange */,
            2008 /* seed */);

        auto vector = maker_.encodedVector(encode, expected.data());
        constexpr auto width = xsimd::batch<T>::size;

        // TODO T71293360: determine SIMD behavior when index + SIMD register
        // width result exceeds the vector length
        for (size_t base = 0; base + width < vector->size(); base += width) {
          auto simdBuffer = loadSIMDValueBufferAt<T>(
              static_cast<SimpleVector<T>*>(vector.get()),
              // Though sizeof(bool) = 1, while a bool only occupies 1 bit in
              // SIMD.
              std::is_same_v<T, bool> ? base / 8 : base * sizeof(T));
          AssertSimdElement<T, width - 1>::eq(
              expected.data(), simdBuffer, base);
        }
      }
    }
  }
};

VELOX_TYPED_TEST_SUITE(SimpleVectorSimdTypedTest, SimpleTypes);

TYPED_TEST(SimpleVectorSimdTypedTest, biasVectorTest) {
  this->template runTest<VectorEncoding::Simple::BIASED>();
}

TYPED_TEST(SimpleVectorSimdTypedTest, constantVectorTest) {
  this->template runTest<VectorEncoding::Simple::CONSTANT>();
}

TYPED_TEST(SimpleVectorSimdTypedTest, dictionaryVectorTest) {
  this->template runTest<VectorEncoding::Simple::DICTIONARY>();
}

TYPED_TEST(SimpleVectorSimdTypedTest, flatVectorTest) {
  this->template runTest<VectorEncoding::Simple::FLAT>();
}

TYPED_TEST(SimpleVectorSimdTypedTest, sequenceVectorTest) {
  this->template runTest<VectorEncoding::Simple::SEQUENCE>();
}

// See instructions in SimpleVectorTestHelper.h. By implementing this, a bunch
// of SimpleVector test cases will be run.
template <typename T>
void SimpleVectorTest::runTest(
    ExpectedData<T> expected,
    VectorEncoding::Simple vectorType) {
  LOG(INFO) << "Running " << vectorType;
  auto vector = maker_.encodedVector(vectorType, expected);
  assertVectorAndProperties(expected, vector);
}

} // namespace facebook::velox::test

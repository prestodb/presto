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

#include "velox/vector/DecodedVector.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include "velox/type/Variant.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/tests/VectorMaker.h"
#include "velox/vector/tests/VectorTestUtils.h"

namespace facebook::velox::test {

class DecodedVectorTest : public testing::Test {
 protected:
  DecodedVectorTest() : allSelected_(10010), halfSelected_(10010) {
    allSelected_.setAll();
    halfSelected_.setAll();
    for (int32_t i = 1; i < halfSelected_.size(); i += 2) {
      halfSelected_.setValid(i, false);
    }
  }

  template <typename T>
  void assertDecodedVector(
      const std::vector<std::optional<T>>& expected,
      const SelectivityVector& selection,
      SimpleVector<T>* outVector,
      bool dbgPrintVec) {
    DecodedVector decoded(*outVector, selection);
    auto decodedResult = decoded.as<T>();
    auto end = selection.end();
    ASSERT_EQ(expected.size(), end);

    for (int32_t index = selection.begin(); index < end; ++index) {
      if (!selection.isValid(index)) {
        continue;
      }
      bool actualIsNull = outVector->isNullAt(index);
      auto actualValue = outVector->valueAt(index);
      ASSERT_EQ(actualIsNull, decodedResult.isNullAt(index));
      if (!actualIsNull) {
        ASSERT_EQ(actualValue, decodedResult[index]);
      }
      const bool isNull = (expected[index] == std::nullopt);
      if (dbgPrintVec) {
        LOG(INFO) << "[" << index << "]:"
                  << (isNull ? "NULL" : folly::to<std::string>(actualValue));
      }
      ASSERT_EQ(isNull, actualIsNull);
      if (!isNull) {
        ASSERT_EQ(*expected[index], actualValue);
      }
    }
  }

  template <typename T>
  void assertDecodedVector(
      const std::vector<std::optional<T>>& expected,
      BaseVector* outBaseVector,
      bool dbgPrintVec) {
    auto* outVector = reinterpret_cast<SimpleVector<T>*>(outBaseVector);
    assertDecodedVector(expected, allSelected_, outVector, dbgPrintVec);
    assertDecodedVector(expected, halfSelected_, outVector, dbgPrintVec);
  }

  template <typename T>
  void testFlat(vector_size_t cardinality = 10010) {
    auto cardData = genTestData<T>(cardinality, /* includeNulls */ true);
    const auto& data = cardData.data();
    EXPECT_EQ(cardinality, data.size());

    auto flatVector = vectorMaker_->flatVectorNullable(data);
    assertDecodedVector(data, flatVector.get(), false);
  }

  template <typename T>
  void testConstant(const T& value) {
    auto constantVector = BaseVector::createConstant(value, 100, pool_.get());
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_FALSE(decoded.isIdentityMapping());
    for (int32_t i = 0; i < 100; i++) {
      EXPECT_FALSE(decoded.isNullAt(i));
      EXPECT_EQ(decoded.valueAt<T>(i), value);
    }
  }

  void testConstant(const VectorPtr& base, vector_size_t index) {
    auto constantVector = std::make_shared<ConstantVector<ComplexType>>(
        pool_.get(), 100, index, base);
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_FALSE(decoded.isIdentityMapping());
    EXPECT_EQ(base->encoding(), decoded.base()->encoding());
    bool isNull = base->isNullAt(index);
    if (isNull) {
      for (int32_t i = 0; i < 100; i++) {
        EXPECT_TRUE(decoded.isNullAt(i)) << "at " << i;
      }
    } else {
      for (int32_t i = 0; i < 100; i++) {
        EXPECT_FALSE(decoded.isNullAt(i));
        EXPECT_TRUE(base->equalValueAt(decoded.base(), index, decoded.index(i)))
            << "at " << i << ": " << base->toString(index) << " vs. "
            << decoded.base()->toString(decoded.index(i));
      }
    }
  }

  template <typename T>
  void testConstantOpaque(const std::shared_ptr<T>& value) {
    int uses = value.use_count();
    auto constantVector =
        BaseVector::createConstant(variant::opaque(value), 100, pool_.get());
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_FALSE(decoded.isIdentityMapping());
    for (int32_t i = 0; i < 100; i++) {
      EXPECT_FALSE(decoded.isNullAt(i));
      EXPECT_EQ(decoded.valueAt<std::shared_ptr<T>>(i), value);
    }
    // despite all the decoding we only keep one copy
    EXPECT_EQ(uses + 1, value.use_count());
    EXPECT_EQ(decoded.base()->type(), OpaqueType::create<T>());
  }

  void testConstantNull(TypeKind typeKind) {
    auto constantVector =
        BaseVector::createConstant(variant(typeKind), 100, pool_.get());
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_FALSE(decoded.isIdentityMapping());
    for (int32_t i = 0; i < 100; i++) {
      EXPECT_TRUE(decoded.isNullAt(i));
    }
  }

  void testDictionaryOverConstant(const VectorPtr& base, vector_size_t index) {
    constexpr vector_size_t size = 1000;
    auto constantVector = std::make_shared<ConstantVector<ComplexType>>(
        pool_.get(), size, index, base);

    // Add nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    // Every even element is null. Every odd element is a constant.
    BufferPtr indices = evenIndices(size);
    auto dictionarySize = size / 2;
    auto dictionaryVector = BaseVector::wrapInDictionary(
        nulls, indices, dictionarySize, constantVector);

    SelectivityVector selection(dictionarySize);
    DecodedVector decoded(*dictionaryVector, selection, false);
    ASSERT_FALSE(decoded.isIdentityMapping());
    if (base->isNullAt(index)) {
      // All elements are nulls.
      ASSERT_TRUE(decoded.isConstantMapping());
      for (auto i = 0; i < dictionarySize; i++) {
        ASSERT_TRUE(decoded.isNullAt(i)) << "at " << i;
      }
    } else {
      ASSERT_FALSE(decoded.isConstantMapping());
      for (auto i = 0; i < dictionarySize; i++) {
        if (i % 2 == 0) {
          ASSERT_TRUE(decoded.isNullAt(i)) << "at " << i;
        } else {
          ASSERT_TRUE(
              base->equalValueAt(decoded.base(), index, decoded.index(i)))
              << "at " << i;
        }
      }
    }
  }

  template <typename T>
  void testDictionaryOverConstant(const T& value) {
    constexpr vector_size_t size = 1000;
    auto constantVector = BaseVector::createConstant(value, size, pool_.get());

    // add nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    // Every even element is null. Every odd element is a constant.
    BufferPtr indices = evenIndices(size);
    auto dictionarySize = size / 2;
    auto dictionaryVector = BaseVector::wrapInDictionary(
        nulls, indices, dictionarySize, constantVector);

    SelectivityVector selection(dictionarySize);
    DecodedVector decoded(*dictionaryVector, selection, false);
    ASSERT_FALSE(decoded.isIdentityMapping());
    ASSERT_FALSE(decoded.isConstantMapping());
    for (auto i = 0; i < dictionarySize; i++) {
      if (i % 2 == 0) {
        ASSERT_TRUE(decoded.isNullAt(i)) << "at " << i;
      } else {
        ASSERT_EQ(value, decoded.valueAt<T>(i)) << "at " << i;
      }
    }
  }

  void testDictionaryOverNullConstant() {
    constexpr vector_size_t size = 1000;
    auto constantNullVector = BaseVector::createConstant(
        variant(TypeKind::BIGINT), size, pool_.get());

    // Add more nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    BufferPtr indices = evenIndices(size);
    auto dictionarySize = size / 2;
    auto dictionaryVector = BaseVector::wrapInDictionary(
        nulls, indices, dictionarySize, constantNullVector);

    SelectivityVector selection(dictionarySize);
    DecodedVector decoded(*dictionaryVector, selection, false);
    ASSERT_FALSE(decoded.isIdentityMapping());
    ASSERT_TRUE(decoded.isConstantMapping());
    for (auto i = 0; i < dictionarySize; i++) {
      ASSERT_TRUE(decoded.isNullAt(i)) << "at " << i;
    }
  }

  BufferPtr indicesBuffer(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /*row*/)> indexAt) {
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (int i = 0; i < size; i++) {
      rawIndices[i] = indexAt(i);
    }
    return indices;
  }

  BufferPtr nullsBuffer(
      vector_size_t size,
      std::function<bool(vector_size_t /*row*/)> isNullAt) {
    BufferPtr nulls =
        AlignedBuffer::allocate<uint64_t>(bits::nwords(size), pool_.get());
    for (auto i = 0; i < size; i++) {
      bits::setNull(nulls->asMutable<uint64_t>(), i, isNullAt(i));
    }
    return nulls;
  }

  BufferPtr evenIndices(vector_size_t size) {
    return indicesBuffer(size, [](auto row) { return row * 2; });
  }

  BufferPtr evenNulls(vector_size_t size) {
    return nullsBuffer(size, VectorMaker::nullEvery(2));
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*index*/)> valueAt) {
    auto vector = std::dynamic_pointer_cast<FlatVector<T>>(
        BaseVector::create(CppToType<T>::create(), size, pool_.get()));
    for (int32_t i = 0; i < size; ++i) {
      vector->set(i, valueAt(i));
    }
    return vector;
  }

  template <typename T>
  void testDictionary(
      vector_size_t size,
      std::function<T(vector_size_t /*index*/)> valueAt) {
    auto flatVector = makeFlatVector<T>(size, valueAt);
    BufferPtr indices = evenIndices(size);

    auto dictionarySize = size / 2;

    auto dictionaryVector = std::dynamic_pointer_cast<DictionaryVector<T>>(
        BaseVector::wrapInDictionary(
            BufferPtr(nullptr), indices, dictionarySize, flatVector));

    SelectivityVector selection(dictionarySize);
    DecodedVector decoded(*dictionaryVector, selection);
    EXPECT_FALSE(decoded.isConstantMapping());
    EXPECT_FALSE(decoded.isIdentityMapping());
    for (int32_t i = 0; i < dictionaryVector->size(); i++) {
      EXPECT_FALSE(decoded.isNullAt(i)) << "at " << i;
      EXPECT_EQ(decoded.valueAt<T>(i), dictionaryVector->valueAt(i))
          << "at " << i;
    }
  }

  SelectivityVector allSelected_;
  SelectivityVector halfSelected_;
  std::unique_ptr<velox::memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<test::VectorMaker> vectorMaker_{
      std::make_unique<test::VectorMaker>(pool_.get())};
};

template <>
void DecodedVectorTest::testConstant<StringView>(const StringView& value) {
  auto val = value.getString();
  auto constantVector =
      BaseVector::createConstant(folly::StringPiece{val}, 100, pool_.get());
  SelectivityVector selection(100);
  DecodedVector decoded(*constantVector, selection);
  EXPECT_TRUE(decoded.isConstantMapping());
  EXPECT_FALSE(decoded.isIdentityMapping());
  for (int32_t i = 0; i < 100; i++) {
    EXPECT_FALSE(decoded.isNullAt(i));
    EXPECT_EQ(decoded.valueAt<StringView>(i).getString(), val);
  }
}

TEST_F(DecodedVectorTest, testFlat) {
  testFlat<int8_t>();
  testFlat<int16_t>();
  testFlat<int32_t>();
  testFlat<int64_t>();
  testFlat<bool>();

  // TODO: ValueGenerator doesn't support floats.
  // testFlat<float>();
  testFlat<double>();

  testFlat<StringView>();
}

namespace {
struct NonPOD {
  static int alive;

  int x;

  NonPOD(int x = 123) : x(x) {
    ++alive;
  }

  ~NonPOD() {
    --alive;
  }

  bool operator==(const NonPOD& other) const {
    return x == other.x;
  }
};

int NonPOD::alive = 0;
} // namespace

TEST_F(DecodedVectorTest, constant) {
  testConstant<bool>(true);
  testConstant<bool>(false);
  testConstant<int8_t>(7);
  testConstant<int16_t>(12);
  testConstant<int32_t>(123);
  testConstant<int64_t>(12'345);
  testConstant<float>(1.23);
  testConstant<double>(12.345);
  testConstant<StringView>(StringView("test"));
  NonPOD::alive = 0;
  testConstantOpaque(std::make_shared<NonPOD>());
  EXPECT_EQ(NonPOD::alive, 0);
}

TEST_F(DecodedVectorTest, constantNull) {
  testConstantNull(TypeKind::BOOLEAN);
  testConstantNull(TypeKind::TINYINT);
  testConstantNull(TypeKind::SMALLINT);
  testConstantNull(TypeKind::INTEGER);
  testConstantNull(TypeKind::BIGINT);
  testConstantNull(TypeKind::REAL);
  testConstantNull(TypeKind::DOUBLE);
  testConstantNull(TypeKind::VARCHAR);
}

TEST_F(DecodedVectorTest, constantComplexType) {
  auto arrayVector = vectorMaker_->arrayVector<int64_t>(
      10,
      [](auto /*row*/) { return 5; },
      [](auto row, auto index) { return row + index; },
      VectorMaker::nullEvery(7, 5));
  testConstant(arrayVector, 0);
  testConstant(arrayVector, 3);
  testConstant(arrayVector, 5); // null

  auto mapVector = vectorMaker_->mapVector<int64_t, double>(
      10,
      [](auto /*row*/) { return 5; },
      [](auto row, auto index) { return row + index; },
      [](auto row, auto index) { return (row + index) * 0.1; },
      VectorMaker::nullEvery(7, 5));
  testConstant(mapVector, 0);
  testConstant(mapVector, 3);
  testConstant(mapVector, 5); // null
}

TEST_F(DecodedVectorTest, boolDictionary) {
  testDictionary<bool>(1000, [](vector_size_t i) { return i % 3 == 0; });
  testDictionary<int8_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<int16_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<int32_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<int64_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<float>(1000, [](vector_size_t i) { return i * 0.1; });
  testDictionary<double>(1000, [](vector_size_t i) { return i * 0.1; });
  testDictionary<std::shared_ptr<void>>(
      1000, [](vector_size_t i) { return std::make_shared<int>(i % 5); });
}

TEST_F(DecodedVectorTest, dictionaryOverLazy) {
  constexpr vector_size_t size = 1000;
  auto lazyVector = vectorMaker_->lazyFlatVector<int32_t>(
      size, [](vector_size_t i) { return i % 5; });

  BufferPtr indices = evenIndices(size);
  auto dictionarySize = size / 2;
  auto dictionaryVector = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, dictionarySize, lazyVector);

  // Ensure we set the baseVector_ in the DecodedVector, when decoding
  // DICT(LAZY) with loadLazy=false.
  SelectivityVector selection(dictionarySize);
  DecodedVector decoded(*dictionaryVector, selection, false);
  auto base = decoded.base();
  EXPECT_NE(base, nullptr);

  // Ensure we don't load the lazy vector under dictionary.
  auto lazyVector2 = dictionaryVector->valueVector()->as<LazyVector>();
  EXPECT_FALSE(lazyVector2->isLoaded());
}

TEST_F(DecodedVectorTest, dictionaryOverConstant) {
  testDictionaryOverConstant(10);
  testDictionaryOverConstant(12.3);
  testDictionaryOverConstant(false);

  testDictionaryOverNullConstant();

  auto arrayVector = vectorMaker_->arrayVector<int64_t>(
      10,
      [](auto /*row*/) { return 5; },
      [](auto row, auto index) { return row + index; },
      VectorMaker::nullEvery(7, 5));
  testDictionaryOverConstant(arrayVector, 0);
  testDictionaryOverConstant(arrayVector, 3);
  testDictionaryOverConstant(arrayVector, 5); // null
}

TEST_F(DecodedVectorTest, wrapOnDictionaryEncoding) {
  const int kSize = 12;
  auto intVector =
      vectorMaker_->flatVector<int32_t>(kSize, [](auto row) { return row; });
  auto rowVector = vectorMaker_->rowVector({intVector});

  // Test dictionary with depth one
  auto indicesOne =
      indicesBuffer(kSize, [](auto row) { return kSize - row - 1; });
  auto nullsOne = nullsBuffer(kSize, [](auto row) { return row < 2; });
  auto dictionaryVector =
      BaseVector::wrapInDictionary(nullsOne, indicesOne, kSize, rowVector);
  SelectivityVector allRows(kSize);
  DecodedVector decoded(*dictionaryVector, allRows);
  auto wrappedVector = decoded.wrap(intVector, *dictionaryVector, allRows);
  for (auto i = 0; i < kSize; i++) {
    if (i < 2) {
      ASSERT_TRUE(wrappedVector->isNullAt(i));
    } else {
      ASSERT_TRUE(
          wrappedVector->equalValueAt(intVector.get(), i, decoded.index(i)));
    }
  }

  // Test dictionary with depth two
  auto nullsTwo =
      nullsBuffer(kSize, [](auto row) { return row >= 2 && row < 4; });
  auto indicesTwo = indicesBuffer(kSize, [](vector_size_t i) { return i; });
  auto dictionaryOverDictionaryVector = BaseVector::wrapInDictionary(
      nullsTwo, indicesTwo, kSize, dictionaryVector);
  decoded.decode(*dictionaryOverDictionaryVector, allRows);
  wrappedVector =
      decoded.wrap(intVector, *dictionaryOverDictionaryVector, allRows);
  for (auto i = 0; i < kSize; i++) {
    if (i < 4) {
      ASSERT_TRUE(wrappedVector->isNullAt(i));
    } else {
      ASSERT_TRUE(
          wrappedVector->equalValueAt(intVector.get(), i, decoded.index(i)));
    }
  }

  // Test dictionrary with depth two and no nulls
  auto noNullDictionaryVector =
      BaseVector::wrapInDictionary(nullptr, indicesOne, kSize, rowVector);
  auto noNullDictionaryOverDictionaryVector = BaseVector::wrapInDictionary(
      nullptr, indicesTwo, kSize, noNullDictionaryVector);
  decoded.decode(*noNullDictionaryOverDictionaryVector, allRows);
  wrappedVector =
      decoded.wrap(intVector, *noNullDictionaryOverDictionaryVector, allRows);
  for (auto i = 0; i < kSize; i++) {
    ASSERT_TRUE(
        wrappedVector->equalValueAt(intVector.get(), i, decoded.index(i)));
    ASSERT_FALSE(wrappedVector->isNullAt(i));
  }
}

TEST_F(DecodedVectorTest, wrapOnConstantEncoding) {
  const int kSize = 12;
  auto intVector =
      vectorMaker_->flatVector<int32_t>(kSize, [](auto row) { return row; });
  auto rowVector = vectorMaker_->rowVector({intVector});
  auto constantVector = BaseVector::wrapInConstant(kSize, 1, rowVector);
  SelectivityVector allRows(kSize);
  DecodedVector decoded(*constantVector, allRows);
  auto wrappedVector = decoded.wrap(intVector, *constantVector, allRows);
  for (auto i = 0; i < kSize; i++) {
    ASSERT_TRUE(
        wrappedVector->equalValueAt(intVector.get(), i, decoded.index(i)));
  }
}

} // namespace facebook::velox::test

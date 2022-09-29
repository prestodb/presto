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
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

class DecodedVectorTest : public testing::Test, public VectorTestBase {
 protected:
  DecodedVectorTest() : allSelected_(10010), halfSelected_(10010) {
    allSelected_.setAll();
    halfSelected_.setAll();
    for (int32_t i = 1; i < halfSelected_.size(); i += 2) {
      halfSelected_.setValid(i, false);
    }
  }

  void assertNoNulls(DecodedVector& decodedVector) {
    ASSERT_TRUE(decodedVector.nulls() == nullptr);
    for (auto i = 0; i < decodedVector.size(); ++i) {
      ASSERT_FALSE(decodedVector.isNullAt(i));
    }
  }

  void assertNulls(const VectorPtr& vector, DecodedVector& decodedVector) {
    SCOPED_TRACE(vector->toString(true));
    ASSERT_TRUE(decodedVector.nulls() != nullptr);
    for (auto i = 0; i < decodedVector.size(); ++i) {
      ASSERT_EQ(decodedVector.isNullAt(i), vector->isNullAt(i));
      ASSERT_EQ(bits::isBitNull(decodedVector.nulls(), i), vector->isNullAt(i));
    }
  }

  template <typename T>
  void assertDecodedVector(
      const std::vector<std::optional<T>>& expected,
      const SelectivityVector& selection,
      SimpleVector<T>* outVector,
      bool dbgPrintVec) {
    DecodedVector decoded(*outVector, selection);
    auto end = selection.end();
    ASSERT_EQ(expected.size(), end);

    for (int32_t index = selection.begin(); index < end; ++index) {
      if (!selection.isValid(index)) {
        continue;
      }
      bool actualIsNull = outVector->isNullAt(index);
      auto actualValue = outVector->valueAt(index);
      ASSERT_EQ(actualIsNull, decoded.isNullAt(index));
      if (!actualIsNull) {
        auto decodedValue = decoded.template valueAt<T>(index);
        ASSERT_EQ(actualValue, decodedValue);
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

    auto flatVector = makeNullableFlatVector(data);
    assertDecodedVector(data, flatVector.get(), false);
  }

  template <typename T>
  void testConstant(const T& value) {
    auto constantVector = BaseVector::createConstant(value, 100, pool_.get());
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_TRUE(!decoded.isIdentityMapping());
    EXPECT_TRUE(decoded.nulls() == nullptr);
    for (int32_t i = 0; i < 100; i++) {
      EXPECT_FALSE(decoded.isNullAt(i));
      EXPECT_EQ(decoded.valueAt<T>(i), value);
    }
  }

  void testConstant(const VectorPtr& base, vector_size_t index) {
    SCOPED_TRACE(base->toString());
    auto constantVector = std::make_shared<ConstantVector<ComplexType>>(
        pool_.get(), 100, index, base);
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_TRUE(!decoded.isIdentityMapping());
    EXPECT_EQ(base->encoding(), decoded.base()->encoding());
    bool isNull = base->isNullAt(index);
    if (isNull) {
      EXPECT_TRUE(decoded.nulls() != nullptr);
      for (int32_t i = 0; i < 100; i++) {
        EXPECT_TRUE(decoded.isNullAt(i)) << "at " << i;
        EXPECT_TRUE(bits::isBitNull(decoded.nulls(), i)) << "at " << i;
      }
    } else {
      EXPECT_TRUE(decoded.nulls() == nullptr);
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

  void testConstantNull(const TypePtr& type) {
    SCOPED_TRACE(type->toString());
    auto constantVector =
        BaseVector::createNullConstant(type, 100, pool_.get());
    EXPECT_EQ(constantVector->isScalar(), type->isPrimitiveType());
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    EXPECT_EQ(*decoded.base()->type(), *type);
    // Decoded vector doesn't ensure base() has "FLAT" encoding for primitive
    // types.
    if (!type->isPrimitiveType()) {
      EXPECT_EQ(
          decoded.base()->encoding(),
          BaseVector::create(type, 1, pool_.get())->encoding());
    }
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_TRUE(!decoded.isIdentityMapping());
    ASSERT_TRUE(decoded.nulls() != nullptr);
    for (int32_t i = 0; i < 100; i++) {
      EXPECT_TRUE(decoded.isNullAt(i));
      EXPECT_TRUE(bits::isBitNull(decoded.nulls(), i));
    }
  }

  void testDictionaryOverConstant(const VectorPtr& base, vector_size_t index) {
    constexpr vector_size_t size = 1000;
    auto constantVector = std::make_shared<ConstantVector<ComplexType>>(
        pool_.get(), size, index, base);

    // Add nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    // Every even element is null. Every odd element is a constant.
    BufferPtr indices = makeEvenIndices(size);
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

    // Add nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    // Every even element is null. Every odd element is a constant.
    BufferPtr indices = makeEvenIndices(size);
    auto dictionarySize = size / 2;
    auto dictionaryVector = BaseVector::wrapInDictionary(
        nulls, indices, dictionarySize, constantVector);

    SelectivityVector selection(dictionarySize);
    DecodedVector decoded(*dictionaryVector, selection, false);
    ASSERT_FALSE(decoded.isIdentityMapping());
    ASSERT_FALSE(decoded.isConstantMapping());
    ASSERT_TRUE(decoded.nulls() != nullptr);
    for (auto i = 0; i < dictionarySize; i++) {
      if (i % 2 == 0) {
        ASSERT_TRUE(decoded.isNullAt(i)) << "at " << i;
      } else {
        ASSERT_EQ(value, decoded.valueAt<T>(i)) << "at " << i;
      }
      ASSERT_EQ(decoded.isNullAt(i), dictionaryVector->isNullAt(i));
      ASSERT_EQ(
          bits::isBitNull(decoded.nulls(), i), dictionaryVector->isNullAt(i));
    }
  }

  void testDictionaryOverNullConstant() {
    constexpr vector_size_t size = 1000;
    auto constantNullVector = BaseVector::createConstant(
        variant(TypeKind::BIGINT), size, pool_.get());

    // Add more nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    BufferPtr indices = makeEvenIndices(size);
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

  BufferPtr evenNulls(vector_size_t size) {
    return makeNulls(size, VectorMaker::nullEvery(2));
  }

  template <typename T>
  void testDictionary(
      vector_size_t size,
      std::function<T(vector_size_t /*index*/)> valueAt) {
    auto flatVector = makeFlatVector<T>(size, valueAt);
    BufferPtr indices = makeEvenIndices(size);

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

TEST_F(DecodedVectorTest, flat) {
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
  testConstantNull(BOOLEAN());
  testConstantNull(TINYINT());
  testConstantNull(SMALLINT());
  testConstantNull(INTEGER());
  testConstantNull(BIGINT());
  testConstantNull(REAL());
  testConstantNull(DOUBLE());
  testConstantNull(VARCHAR());
  testConstantNull(VARBINARY());
  testConstantNull(TIMESTAMP());
  testConstantNull(DATE());
  testConstantNull(INTERVAL_DAY_TIME());
  testConstantNull(ARRAY(INTEGER()));
  testConstantNull(MAP(INTEGER(), INTEGER()));
  testConstantNull(ROW({INTEGER()}));
}

TEST_F(DecodedVectorTest, constantComplexType) {
  auto arrayVector = makeArrayVector<int64_t>(
      10,
      [](auto /*row*/) { return 5; },
      [](auto row, auto index) { return row + index; },
      VectorMaker::nullEvery(7, 5));
  testConstant(arrayVector, 0);
  testConstant(arrayVector, 3);
  testConstant(arrayVector, 5); // null

  auto mapVector = vectorMaker_.mapVector<int64_t, double>(
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
  auto lazyVector = vectorMaker_.lazyFlatVector<int32_t>(
      size, [](vector_size_t i) { return i % 5; });

  BufferPtr indices = makeEvenIndices(size);
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

  auto arrayVector = makeArrayVector<int64_t>(
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
  auto intVector = makeFlatVector<int32_t>(kSize, [](auto row) { return row; });
  auto rowVector = makeRowVector({intVector});

  // Test dictionary with depth one
  auto indicesOne =
      makeIndices(kSize, [](auto row) { return kSize - row - 1; });
  auto nullsOne = makeNulls(kSize, [](auto row) { return row < 2; });
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
      makeNulls(kSize, [](auto row) { return row >= 2 && row < 4; });
  auto indicesTwo = makeIndices(kSize, [](vector_size_t i) { return i; });
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
  // non-null
  auto intVector = makeFlatVector<int32_t>(kSize, [](auto row) { return row; });
  auto rowVector = makeRowVector({intVector});
  auto constantVector = BaseVector::wrapInConstant(kSize, 1, rowVector);
  SelectivityVector allRows(kSize);
  DecodedVector decoded(*constantVector, allRows);
  auto wrappedVector = decoded.wrap(intVector, *constantVector, allRows);
  for (auto i = 0; i < kSize; i++) {
    ASSERT_TRUE(
        wrappedVector->equalValueAt(intVector.get(), i, decoded.index(i)));
  }

  // null with empty size children
  intVector = makeFlatVector<int32_t>(0 /*size*/, [](auto row) { return row; });
  rowVector = std::make_shared<RowVector>(
      pool_.get(),
      rowVector->type(),
      makeNulls(kSize, VectorMaker::nullEvery(1)),
      kSize,
      std::vector<VectorPtr>{intVector});
  constantVector = BaseVector::wrapInConstant(kSize, 1, rowVector);
  decoded.decode(*constantVector, allRows);
  wrappedVector = decoded.wrap(intVector, *constantVector, allRows);
  for (auto i = 0; i < kSize; i++) {
    ASSERT_TRUE(wrappedVector->isNullAt(i));
  }
}

TEST_F(DecodedVectorTest, noValues) {
  // Tests decoding a flat vector that consists of all nulls and has
  // no values() buffer.
  constexpr vector_size_t kSize = 100;
  auto nulls = AlignedBuffer::allocate<uint64_t>(
      bits::nwords(kSize), pool_.get(), bits::kNull64);
  auto vector = std::make_shared<FlatVector<int32_t>>(
      pool_.get(),
      std::move(nulls),
      kSize,
      BufferPtr(nullptr),
      std::vector<BufferPtr>{});
  SelectivityVector rows(kSize);
  DecodedVector decoded;
  decoded.decode(*vector, rows);
  EXPECT_EQ(nullptr, decoded.data<int32_t>());
  EXPECT_TRUE(decoded.isNullAt(kSize - 1));
}

/// Test decoding Dict(Const) + nulls for empty rows.
TEST_F(DecodedVectorTest, emptyRowsDictOverConstWithNulls) {
  auto nulls = makeNulls(3, [](auto /* row */) { return true; });
  auto indices = makeIndices(3, [](auto /* row */) { return 2; });
  auto dict = BaseVector::wrapInDictionary(
      nulls, indices, 3, BaseVector::createConstant(1, 3, pool()));

  SelectivityVector rows(3, false);
  DecodedVector d(*dict, rows, false);
  VELOX_CHECK_EQ(d.size(), 0);
  VELOX_CHECK_NOT_NULL(d.indices());
}

/// Test decoding Dict(Dict(Flat)) for empty rows.
TEST_F(DecodedVectorTest, emptyRowsMultiDict) {
  vector_size_t size = 100;
  auto indices = makeIndicesInReverse(size);
  auto dict = wrapInDictionary(
      indices,
      size,
      wrapInDictionary(
          indices, size, makeFlatVector<int64_t>(size, [](auto row) {
            return row;
          })));

  SelectivityVector emptyRows(100, false);
  DecodedVector d(*dict, emptyRows, false);
  VELOX_CHECK_EQ(d.size(), 0);
  VELOX_CHECK_NOT_NULL(d.indices());
}

TEST_F(DecodedVectorTest, flatNulls) {
  // Flat vector with no nulls.
  auto flatNoNulls = makeFlatVector<int64_t>(100, [](auto row) { return row; });
  SelectivityVector rows(100);
  DecodedVector d(*flatNoNulls, rows);
  assertNoNulls(d);

  // Flat vector with nulls.
  auto flatWithNulls = makeFlatVector<int64_t>(
      100, [](auto row) { return row; }, nullEvery(7));
  d.decode(*flatWithNulls, rows);
  ASSERT_TRUE(d.nulls() != nullptr);
  for (auto i = 0; i < 100; ++i) {
    ASSERT_EQ(d.isNullAt(i), i % 7 == 0);
    ASSERT_EQ(bits::isBitNull(d.nulls(), i), i % 7 == 0);
  }
}

TEST_F(DecodedVectorTest, dictionaryOverFlatNulls) {
  SelectivityVector rows(100);
  DecodedVector d;

  auto flatNoNulls = makeFlatVector<int64_t>(100, [](auto row) { return row; });
  auto flatWithNulls = makeFlatVector<int64_t>(
      100, [](auto row) { return row; }, nullEvery(7));

  // Dictionary over flat with no nulls.
  auto dict = wrapInDictionary(makeIndicesInReverse(100), 100, flatNoNulls);
  d.decode(*dict, rows);
  assertNoNulls(d);

  // Dictionary over flat with nulls.
  auto indices = makeIndicesInReverse(100);
  dict = wrapInDictionary(indices, 100, flatWithNulls);
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dictionary that adds nulls over flat with no nulls.
  auto nulls = makeNulls(100, nullEvery(3));
  dict = BaseVector::wrapInDictionary(nulls, indices, 100, flatNoNulls);
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dictionary that adds nulls over flat with nulls.
  dict = BaseVector::wrapInDictionary(nulls, indices, 100, flatWithNulls);
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // 2 layers of dictionary over flat using all combinations of nulls/no-nulls
  // at each level.

  // Dict(Dict(Flat))
  dict = wrapInDictionary(
      indices, 100, wrapInDictionary(indices, 100, flatNoNulls));
  d.decode(*dict, rows);
  assertNoNulls(d);

  // Dict(Dict(Flat-with-Nulls))
  dict = wrapInDictionary(
      indices, 100, wrapInDictionary(indices, 100, flatWithNulls));
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dict(Dict-with-Nulls(Flat))
  dict = wrapInDictionary(
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatNoNulls));
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dict(Dict-with-Nulls(Flat-with-Nulls))
  dict = wrapInDictionary(
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatWithNulls));
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dict-with-Nulls(Dict(Flat))
  auto moreNulls = makeNulls(100, nullEvery(5));
  dict = BaseVector::wrapInDictionary(
      moreNulls, indices, 100, wrapInDictionary(indices, 100, flatNoNulls));
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dict-with-Nulls(Dict-with-Nulls(Flat))
  dict = BaseVector::wrapInDictionary(
      moreNulls,
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatNoNulls));
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dict-with-Nulls(Dict(Flat-with-Nulls))
  dict = BaseVector::wrapInDictionary(
      moreNulls, indices, 100, wrapInDictionary(indices, 100, flatWithNulls));
  d.decode(*dict, rows);
  assertNulls(dict, d);

  // Dict-with-Nulls(Dict-with-Nulls(Flat-with-Nulls))
  dict = BaseVector::wrapInDictionary(
      moreNulls,
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatWithNulls));
  d.decode(*dict, rows);
  assertNulls(dict, d);
}

TEST_F(DecodedVectorTest, dictionaryWrapping) {
  constexpr vector_size_t baseVectorSize{100};
  constexpr vector_size_t innerDictSize{30};
  constexpr vector_size_t outerDictSize{15};
  SelectivityVector rows(outerDictSize);
  VectorPtr dict;
  DecodedVector decoded;
  BufferPtr nullsBuffer;

  // Prepare indices to take every third element from the base vector.
  auto innerIndices =
      makeIndices(innerDictSize, [](auto row) { return row * 3; });
  // Indices for the outer dictionary (need two or more dictionaries so we don't
  // hit the simplified path for a single level dictionary).
  auto outerIndices = makeIndices(outerDictSize, [outerDictSize](auto row) {
    return (row * 11) % outerDictSize;
  });

  auto baseWithNulls = makeFlatVector<int64_t>(
      baseVectorSize, [](auto row) { return row; }, nullEvery(7));

  for (size_t i = 0; i < 4; ++i) {
    switch (i) {
      case 0: // Case dict_no_nulls(dict_no_nulls(base_with_nulls)).
        dict = wrapInDictionary(innerIndices, baseWithNulls);
        dict = wrapInDictionary(outerIndices, dict);
        break;
      case 1: // Case dict_no_nulls(dict_nulls(base_with_nulls)).
        nullsBuffer = makeNulls(innerDictSize, nullEvery(5));
        dict = BaseVector::wrapInDictionary(
            nullsBuffer, innerIndices, innerDictSize, baseWithNulls);
        dict = wrapInDictionary(outerIndices, dict);
        break;
      case 2: // Case dict_nulls(dict_no_nulls(base_with_nulls)).
        dict = wrapInDictionary(innerIndices, baseWithNulls);
        nullsBuffer = makeNulls(outerDictSize, nullEvery(5));
        dict = BaseVector::wrapInDictionary(
            nullsBuffer, outerIndices, outerDictSize, dict);
        break;
      case 3: // Case dict_nulls(dict_nulls(base_with_nulls)).
        nullsBuffer = makeNulls(innerDictSize, nullEvery(9));
        dict = BaseVector::wrapInDictionary(
            nullsBuffer, innerIndices, innerDictSize, baseWithNulls);
        nullsBuffer = makeNulls(outerDictSize, nullEvery(5));
        dict = BaseVector::wrapInDictionary(
            nullsBuffer, outerIndices, outerDictSize, dict);
        break;
      default:
        break;
    }

    // Get wrap and nulls from the decoded vector and ensure they are correct,
    // wrap base vector with them and compare initial dictionary and the new
    // one.
    decoded.decode(*dict, rows);
    auto wrapping = decoded.dictionaryWrapping(*dict, rows);
    auto wrapped = BaseVector::wrapInDictionary(
        std::move(wrapping.nulls),
        std::move(wrapping.indices),
        rows.end(),
        baseWithNulls);
    assertEqualVectors(dict, wrapped);
  }
}

} // namespace facebook::velox::test

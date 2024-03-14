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
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  DecodedVectorTest() : allSelected_(10010), halfSelected_(10010) {
    allSelected_.setAll();
    halfSelected_.setAll();
    for (int32_t i = 1; i < halfSelected_.size(); i += 2) {
      halfSelected_.setValid(i, false);
    }
  }

  void assertNoNulls(
      DecodedVector& decodedVector,
      const SelectivityVector* rows = nullptr) {
    ASSERT_TRUE(decodedVector.nulls(nullptr) == nullptr);
    for (auto i = 0; i < decodedVector.size(); ++i) {
      ASSERT_FALSE(decodedVector.isNullAt(i));
    }
  }

  void assertNulls(
      const VectorPtr& vector,
      DecodedVector& decodedVector,
      const SelectivityVector* rows = nullptr) {
    SCOPED_TRACE(vector->toString(true));
    ASSERT_TRUE(decodedVector.nulls(rows) != nullptr);
    for (auto i = 0; i < decodedVector.size(); ++i) {
      ASSERT_EQ(decodedVector.isNullAt(i), vector->isNullAt(i));
      ASSERT_EQ(
          bits::isBitNull(decodedVector.nulls(nullptr), i),
          vector->isNullAt(i));
    }
  }

  template <typename T>
  void assertDecodedVector(
      const std::vector<std::optional<T>>& expected,
      const SelectivityVector& selection,
      SimpleVector<T>* outVector,
      bool dbgPrintVec) {
    auto check = [&](auto& decoded) {
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
        std::string value;
        if constexpr (std::is_same_v<T, int128_t>) {
          value = std::to_string(actualValue);
        } else {
          value = folly::to<std::string>(actualValue);
        }
        if (dbgPrintVec) {
          LOG(INFO) << "[" << index << "]:" << (isNull ? "NULL" : value);
        }
        ASSERT_EQ(isNull, actualIsNull);
        if (!isNull) {
          ASSERT_EQ(*expected[index], actualValue);
        }
      }
    };

    {
      DecodedVector decoded(*outVector, selection);
      check(decoded);
    }

    {
      if (selection.isAllSelected()) {
        DecodedVector decoded(*outVector);
        check(decoded);
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
  void testFlat(
      vector_size_t cardinality = 10010,
      const TypePtr& type = CppToType<T>::create()) {
    auto cardData = genTestData<T>(cardinality, type, true /* includeNulls */);
    const auto& data = cardData.data();
    EXPECT_EQ(cardinality, data.size());
    auto flatVector = makeNullableFlatVector(data, type);
    assertDecodedVector(data, flatVector.get(), false);
  }

  template <typename T>
  void testConstant(const T& value) {
    variant var = variant(value);

    auto constantVector =
        BaseVector::createConstant(var.inferType(), var, 100, pool_.get());
    auto check = [&](auto& decoded) {
      EXPECT_TRUE(decoded.isConstantMapping());
      EXPECT_TRUE(!decoded.isIdentityMapping());
      EXPECT_TRUE(decoded.nulls(nullptr) == nullptr);
      for (int32_t i = 0; i < 100; i++) {
        EXPECT_FALSE(decoded.isNullAt(i));
        EXPECT_EQ(decoded.template valueAt<T>(i), value);
      }
    };

    {
      SelectivityVector selection(100);
      DecodedVector decoded(*constantVector, selection);
      check(decoded);
    }

    {
      DecodedVector decoded(*constantVector);
      check(decoded);
    }
  }

  void testConstant(const VectorPtr& base, vector_size_t index) {
    SCOPED_TRACE(base->toString());
    auto constantVector = std::make_shared<ConstantVector<ComplexType>>(
        pool_.get(), 100, index, base);

    auto check = [&](auto& decoded) {
      EXPECT_TRUE(decoded.isConstantMapping());
      EXPECT_TRUE(!decoded.isIdentityMapping());
      EXPECT_EQ(base->encoding(), decoded.base()->encoding());
      bool isNull = base->isNullAt(index);
      if (isNull) {
        EXPECT_TRUE(decoded.nulls(nullptr) != nullptr);
        for (int32_t i = 0; i < 100; i++) {
          EXPECT_TRUE(decoded.isNullAt(i)) << "at " << i;
          EXPECT_TRUE(bits::isBitNull(decoded.nulls(nullptr), i)) << "at " << i;
        }
      } else {
        EXPECT_TRUE(decoded.nulls(nullptr) == nullptr);
        for (int32_t i = 0; i < 100; i++) {
          EXPECT_FALSE(decoded.isNullAt(i));
          EXPECT_TRUE(
              base->equalValueAt(decoded.base(), index, decoded.index(i)))
              << "at " << i << ": " << base->toString(index) << " vs. "
              << decoded.base()->toString(decoded.index(i));
        }
      }
    };

    {
      SelectivityVector selection(100);
      DecodedVector decoded(*constantVector, selection);
      check(decoded);
    }

    {
      DecodedVector decoded(*constantVector);
      check(decoded);
    }
  }

  template <typename T>
  void testConstantOpaque(const std::shared_ptr<T>& value) {
    int uses = value.use_count();
    auto constantVector = BaseVector::createConstant(
        OpaqueType::create<T>(), variant::opaque(value), 100, pool_.get());

    auto check = [&](auto& decoded) {
      EXPECT_TRUE(decoded.isConstantMapping());
      EXPECT_FALSE(decoded.isIdentityMapping());
      for (int32_t i = 0; i < 100; i++) {
        EXPECT_FALSE(decoded.isNullAt(i));
        EXPECT_EQ(decoded.template valueAt<std::shared_ptr<T>>(i), value);
      }
      // despite all the decoding we only keep one copy
      EXPECT_EQ(uses + 1, value.use_count());
      EXPECT_EQ(decoded.base()->type(), OpaqueType::create<T>());
    };

    {
      SelectivityVector selection(100);
      DecodedVector decoded(*constantVector, selection);
      check(decoded);
    }

    {
      DecodedVector decoded(*constantVector);
      check(decoded);
    }
  }

  void testConstantNull(const TypePtr& type) {
    SCOPED_TRACE(type->toString());
    auto constantVector =
        BaseVector::createNullConstant(type, 100, pool_.get());
    EXPECT_EQ(constantVector->isScalar(), type->isPrimitiveType());

    auto check = [&](auto& decoded) {
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
      ASSERT_TRUE(decoded.nulls(nullptr) != nullptr);
      for (int32_t i = 0; i < 100; i++) {
        EXPECT_TRUE(decoded.isNullAt(i));
        EXPECT_TRUE(bits::isBitNull(decoded.nulls(nullptr), i));
      }
    };

    {
      SelectivityVector selection(100);
      DecodedVector decoded(*constantVector, selection);
      check(decoded);
    }

    {
      DecodedVector decoded(*constantVector);
      check(decoded);
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

    auto check = [&](auto& decoded) {
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
    };

    {
      SelectivityVector selection(dictionarySize);
      DecodedVector decoded(*dictionaryVector, selection, false);
      check(decoded);
    }

    {
      DecodedVector decoded(*dictionaryVector, false);
      check(decoded);
    }
  }

  template <typename T>
  void testDictionaryOverConstant(const T& value) {
    constexpr vector_size_t size = 1000;
    auto constantVector = BaseVector::createConstant(
        variant(value).inferType(), value, size, pool_.get());

    // Add nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    // Every even element is null. Every odd element is a constant.
    BufferPtr indices = makeEvenIndices(size);
    auto dictionarySize = size / 2;
    auto dictionaryVector = BaseVector::wrapInDictionary(
        nulls, indices, dictionarySize, constantVector);

    auto check = [&](auto& decoded) {
      ASSERT_FALSE(decoded.isIdentityMapping());
      ASSERT_FALSE(decoded.isConstantMapping());
      ASSERT_TRUE(decoded.nulls(nullptr) != nullptr);
      for (auto i = 0; i < dictionarySize; i++) {
        if (i % 2 == 0) {
          ASSERT_TRUE(decoded.isNullAt(i)) << "at " << i;
        } else {
          ASSERT_EQ(value, decoded.template valueAt<T>(i)) << "at " << i;
        }
        ASSERT_EQ(decoded.isNullAt(i), dictionaryVector->isNullAt(i));
        ASSERT_EQ(
            bits::isBitNull(decoded.nulls(nullptr), i),
            dictionaryVector->isNullAt(i));
      }
    };

    {
      SelectivityVector selection(dictionarySize);
      DecodedVector decoded(*dictionaryVector, selection, false);
      check(decoded);
    }

    {
      DecodedVector decoded(*dictionaryVector, false);
      check(decoded);
    }
  }

  void testDictionaryOverNullConstant() {
    constexpr vector_size_t size = 1000;
    auto constantNullVector = BaseVector::createConstant(
        BIGINT(), variant(TypeKind::BIGINT), size, pool_.get());

    // Add more nulls via dictionary. Make every 2-nd element a null.
    BufferPtr nulls = evenNulls(size);

    BufferPtr indices = makeEvenIndices(size);
    auto dictionarySize = size / 2;
    auto dictionaryVector = BaseVector::wrapInDictionary(
        nulls, indices, dictionarySize, constantNullVector);

    auto check = [&](const auto& decoded) {
      ASSERT_FALSE(decoded.isIdentityMapping());
      ASSERT_TRUE(decoded.isConstantMapping());
      for (auto i = 0; i < dictionarySize; i++) {
        ASSERT_TRUE(decoded.isNullAt(i)) << "at " << i;
      }
    };

    {
      SelectivityVector selection(dictionarySize);
      DecodedVector decoded(*dictionaryVector, selection, false);
      check(decoded);
    }

    {
      DecodedVector decoded(*dictionaryVector, false);
      check(decoded);
    }
  }

  BufferPtr evenNulls(vector_size_t size) {
    return makeNulls(size, VectorMaker::nullEvery(2));
  }

  template <typename T>
  void testDictionary(
      vector_size_t size,
      std::function<T(vector_size_t /*index*/)> valueAt,
      const TypePtr& type = CppToType<T>::create()) {
    BufferPtr indices = makeEvenIndices(size);
    auto dictionarySize = size / 2;
    auto flatVector = makeFlatVector<T>(size, valueAt, nullptr, type);

    auto dictionaryVector = std::dynamic_pointer_cast<DictionaryVector<T>>(
        BaseVector::wrapInDictionary(
            BufferPtr(nullptr), indices, dictionarySize, flatVector));
    auto check = [&](const auto& decoded) {
      EXPECT_FALSE(decoded.isConstantMapping());
      EXPECT_FALSE(decoded.isIdentityMapping());
      for (int32_t i = 0; i < dictionaryVector->size(); i++) {
        EXPECT_FALSE(decoded.isNullAt(i)) << "at " << i;
        EXPECT_EQ(decoded.template valueAt<T>(i), dictionaryVector->valueAt(i))
            << "at " << i;
      }
    };

    {
      SelectivityVector selection(dictionarySize);
      DecodedVector decoded(*dictionaryVector, selection);
      check(decoded);
    }

    {
      DecodedVector decoded(*dictionaryVector);
      check(decoded);
    }
  }

  SelectivityVector allSelected_;
  SelectivityVector halfSelected_;
};

template <>
void DecodedVectorTest::testConstant<StringView>(const StringView& value) {
  auto val = value.getString();
  auto constantVector = BaseVector::createConstant(
      VARCHAR(), folly::StringPiece{val}, 100, pool_.get());

  auto check = [&](auto& decoded) {
    EXPECT_TRUE(decoded.isConstantMapping());
    EXPECT_FALSE(decoded.isIdentityMapping());
    for (int32_t i = 0; i < 100; i++) {
      EXPECT_FALSE(decoded.isNullAt(i));
      EXPECT_EQ(decoded.template valueAt<StringView>(i).getString(), val);
    }
  };

  {
    SelectivityVector selection(100);
    DecodedVector decoded(*constantVector, selection);
    check(decoded);
  }
  {
    DecodedVector decoded(*constantVector);
    check(decoded);
  }
}

TEST_F(DecodedVectorTest, flat) {
  testFlat<int8_t>();
  testFlat<int16_t>();
  testFlat<int32_t>();
  testFlat<int64_t>();
  testFlat<bool>();
  testFlat<int64_t>(10010, DECIMAL(10, 4));
  testFlat<int128_t>(10010, DECIMAL(25, 19));
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
  testConstantNull(DECIMAL(10, 3));
  testConstantNull(DECIMAL(30, 3));
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

TEST_F(DecodedVectorTest, dictionary) {
  testDictionary<bool>(1000, [](vector_size_t i) { return i % 3 == 0; });
  testDictionary<int8_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<int16_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<int32_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<int64_t>(1000, [](vector_size_t i) { return i % 5; });
  testDictionary<float>(1000, [](vector_size_t i) { return i * 0.1; });
  testDictionary<double>(1000, [](vector_size_t i) { return i * 0.1; });
  testDictionary<int64_t>(
      1000, [](vector_size_t i) { return (i % 5); }, DECIMAL(10, 3));
  testDictionary<int128_t>(
      1000,
      [](vector_size_t i) { return HugeInt::build(i, i) % 5; },
      DECIMAL(25, 20));
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
  auto checkUnloaded = [&](const auto& decoded) {
    auto base = decoded.base();
    EXPECT_NE(base, nullptr);
    // Ensure we don't load the lazy vector under dictionary.
    auto lazyVector2 = dictionaryVector->valueVector()->as<LazyVector>();
    EXPECT_FALSE(lazyVector2->isLoaded());
  };

  auto checkLoaded = [&](const auto& decoded) {
    // Ensure we decode past the loaded lazy layer even when loadLazy=false.
    auto base = decoded.base();
    EXPECT_NE(base, nullptr);
    EXPECT_TRUE(base->isFlatEncoding());
  };

  {
    SelectivityVector selection(dictionarySize);
    DecodedVector decoded(*dictionaryVector, selection, false);
    checkUnloaded(decoded);
  }
  {
    DecodedVector decoded(*dictionaryVector, false);
    checkUnloaded(decoded);
  }
  dictionaryVector->loadedVector();
  EXPECT_TRUE(!dictionaryVector->valueVector()->isLazy());
  {
    SelectivityVector selection(dictionarySize);
    DecodedVector decoded(*dictionaryVector, selection, false);
    checkLoaded(decoded);
  }
  {
    DecodedVector decoded(*dictionaryVector, false);
    checkLoaded(decoded);
  }
}

TEST_F(DecodedVectorTest, nestedLazy) {
  constexpr vector_size_t size = 1000;
  auto columnType = ROW({"a", "b"}, {INTEGER(), INTEGER()});

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 5; },
      [](vector_size_t i) { return i % 7 == 0; });
  auto lazyVectorB = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 3; },
      [](vector_size_t i) { return i % 11 == 0; });

  std::vector<VectorPtr> children{lazyVectorA, lazyVectorB};
  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), columnType, BufferPtr(nullptr), size, children);
  EXPECT_TRUE(isLazyNotLoaded(*rowVector.get()));

  DecodedVector decoded(*rowVector, true);

  auto child = decoded.base()->as<RowVector>()->childAt(0);
  EXPECT_TRUE(child->isFlatEncoding());
  assertEqualVectors(child, lazyVectorA);

  child = decoded.base()->as<RowVector>()->childAt(1);
  EXPECT_TRUE(child->isFlatEncoding());
  assertEqualVectors(child, lazyVectorB);

  EXPECT_FALSE(isLazyNotLoaded(*decoded.base()));
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
  // This test exercises the use-case of unnesting the children of a rowVector
  // and making sure the wrap over the row vector is correctly applied on its
  // children. The input vector here is a dictionary wrapped over a
  // rowVector.
  const int kSize = 12;
  auto intChildVector =
      makeFlatVector<int32_t>(kSize, [](auto row) { return row; });
  auto rowVector = makeRowVector({intChildVector});
  SelectivityVector allRows(kSize);
  DecodedVector decoded;

  auto indicesOne =
      makeIndices(kSize, [](auto row) { return kSize - row - 1; });
  auto nullsOne = makeNulls(kSize, [](auto row) { return row < 2; });
  auto dictionaryVector =
      BaseVector::wrapInDictionary(nullsOne, indicesOne, kSize, rowVector);

  // Test dictionary with depth one, a.k.a. dict-over-flat encoding structure.
  auto checkDepthOne = [&](auto& decoded, auto& wrappedVector) {
    for (auto i = 0; i < kSize; i++) {
      if (i < 2) {
        ASSERT_TRUE(wrappedVector->isNullAt(i));
      } else {
        ASSERT_TRUE(wrappedVector->equalValueAt(
            intChildVector.get(), i, decoded.index(i)));
      }
    }
  };

  decoded.decode(*dictionaryVector, allRows);
  auto wrappedVector = decoded.wrap(intChildVector, *dictionaryVector, allRows);
  checkDepthOne(decoded, wrappedVector);

  decoded.decode(*dictionaryVector);
  wrappedVector = decoded.wrap(intChildVector, *dictionaryVector, kSize);
  checkDepthOne(decoded, wrappedVector);

  // Test dictionary with depth two, a.k.a. dict(dict(flat)) multi-level
  // encoding structure.
  auto nullsTwo =
      makeNulls(kSize, [](auto row) { return row >= 2 && row < 4; });
  auto indicesTwo = makeIndices(kSize, [](vector_size_t i) { return i; });
  auto dictionaryOverDictionaryVector = BaseVector::wrapInDictionary(
      nullsTwo, indicesTwo, kSize, dictionaryVector);

  auto checkDepthTwo = [&](auto& decoded, auto& wrappedVector) {
    for (auto i = 0; i < kSize; i++) {
      if (i < 4) {
        ASSERT_TRUE(wrappedVector->isNullAt(i));
      } else {
        ASSERT_TRUE(wrappedVector->equalValueAt(
            intChildVector.get(), i, decoded.index(i)));
      }
    }
  };

  decoded.decode(*dictionaryOverDictionaryVector, allRows);
  wrappedVector =
      decoded.wrap(intChildVector, *dictionaryOverDictionaryVector, allRows);
  checkDepthTwo(decoded, wrappedVector);

  decoded.decode(*dictionaryOverDictionaryVector);
  wrappedVector =
      decoded.wrap(intChildVector, *dictionaryOverDictionaryVector, kSize);
  checkDepthTwo(decoded, wrappedVector);

  // Test dictionary with depth two and no nulls
  auto noNullDictionaryVector =
      BaseVector::wrapInDictionary(nullptr, indicesOne, kSize, rowVector);
  auto noNullDictionaryOverDictionaryVector = BaseVector::wrapInDictionary(
      nullptr, indicesTwo, kSize, noNullDictionaryVector);

  auto checkDepthTwoAndNoNulls = [&](auto& decoded, auto& wrappedVector) {
    for (auto i = 0; i < kSize; i++) {
      ASSERT_TRUE(wrappedVector->equalValueAt(
          intChildVector.get(), i, decoded.index(i)));
      ASSERT_FALSE(wrappedVector->isNullAt(i));
    }
  };

  decoded.decode(*noNullDictionaryOverDictionaryVector, allRows);
  wrappedVector = decoded.wrap(
      intChildVector, *noNullDictionaryOverDictionaryVector, allRows);
  checkDepthTwoAndNoNulls(decoded, wrappedVector);

  decoded.decode(*noNullDictionaryOverDictionaryVector);
  wrappedVector = decoded.wrap(
      intChildVector, *noNullDictionaryOverDictionaryVector, kSize);
  checkDepthTwoAndNoNulls(decoded, wrappedVector);
}

TEST_F(DecodedVectorTest, wrapOnConstantEncoding) {
  // This test exercises the use-case of unnesting the children of a rowVector
  // and making sure the wrap over the row vector is correctly applied on its
  // children. The input vector here is a constant wrapped over a
  // rowVector.
  const int kSize = 12;
  SelectivityVector allRows(kSize);

  // non-null
  auto intVector = makeFlatVector<int32_t>(kSize, [](auto row) { return row; });
  auto rowVector = makeRowVector({intVector});
  auto constantVector = BaseVector::wrapInConstant(kSize, 1, rowVector);

  {
    auto check = [&](const auto& decoded, const auto& wrappedVector) {
      for (auto i = 0; i < kSize; i++) {
        ASSERT_TRUE(
            wrappedVector->equalValueAt(intVector.get(), i, decoded.index(i)));
      }
    };

    DecodedVector decoded;
    {
      decoded.decode(*constantVector, allRows);
      auto wrappedVector = decoded.wrap(intVector, *constantVector, allRows);
      check(decoded, wrappedVector);
    }

    {
      decoded.decode(*constantVector);
      auto wrappedVector = decoded.wrap(intVector, *constantVector, kSize);
      check(decoded, wrappedVector);
    }
  }
  {
    // null with empty size children
    intVector =
        makeFlatVector<int32_t>(0 /*size*/, [](auto row) { return row; });
    rowVector = std::make_shared<RowVector>(
        pool_.get(),
        rowVector->type(),
        makeNulls(kSize, VectorMaker::nullEvery(1)),
        kSize,
        std::vector<VectorPtr>{intVector});
    constantVector = BaseVector::wrapInConstant(kSize, 1, rowVector);

    auto check = [&](const auto& decoded, const auto& wrappedVector) {
      for (auto i = 0; i < kSize; i++) {
        ASSERT_TRUE(wrappedVector->isNullAt(i));
      }
    };

    DecodedVector decoded;

    {
      decoded.decode(*constantVector, allRows);
      auto wrappedVector = decoded.wrap(intVector, *constantVector, allRows);
      check(decoded, wrappedVector);
    }
    {
      decoded.decode(*constantVector);
      auto wrappedVector = decoded.wrap(intVector, *constantVector, kSize);
      check(decoded, wrappedVector);
    }
  }
}

TEST_F(DecodedVectorTest, dictionaryWrapOnConstantVector) {
  // This test exercises the use-case of unnesting the children of a rowVector
  // and making sure the wrap over the row vector is correctly applied on its
  // children. Input row vector used here contains a child which is a constant
  // vector.
  constexpr vector_size_t size = 100;
  auto constantVector =
      BaseVector::createConstant(VARCHAR(), variant("abc"), size, pool_.get());
  // int Vector
  auto intVector = makeFlatVector<int32_t>(size, [](auto row) { return row; });
  // Row (int, const)
  auto rowVector = makeRowVector({intVector, constantVector});
  // Dictionary encoded row
  auto indices = makeEvenIndices(size);
  auto dictionarySize = size / 2;
  auto dictionaryVector =
      BaseVector::wrapInDictionary(nullptr, indices, dictionarySize, rowVector);

  EXPECT_EQ(dictionarySize, dictionaryVector->size());
  SelectivityVector selection(dictionarySize);
  DecodedVector decoded(*dictionaryVector, selection);
  const auto& intChildVector = decoded.base()->as<RowVector>()->childAt(0);
  const auto& constChildVector = decoded.base()->as<RowVector>()->childAt(1);
  // Wrap the child vectors with the same dictionary wrapping as the parent
  // row vector.
  {
    auto wrappedIntVector =
        decoded.wrap(intChildVector, *dictionaryVector, selection);
    auto wrappedConstVector =
        decoded.wrap(constChildVector, *dictionaryVector, selection);

    // Ensure size of each child is same as the number of indices in the
    // dictionary encoding set.
    EXPECT_EQ(dictionarySize, wrappedIntVector->size());
    EXPECT_EQ(dictionarySize, wrappedConstVector->size());
  }
  {
    auto wrappedIntVector =
        decoded.wrap(intChildVector, *dictionaryVector, dictionarySize);
    auto wrappedConstVector =
        decoded.wrap(constChildVector, *dictionaryVector, dictionarySize);

    // Ensure size of each child is same as the number of indices in the
    // dictionary encoding set.
    EXPECT_EQ(dictionarySize, wrappedIntVector->size());
    EXPECT_EQ(dictionarySize, wrappedConstVector->size());
  }
}

TEST_F(DecodedVectorTest, testWrapBehavior) {
  // This test exercises various cases that wrap() can encounter and verifies
  // the expected behavior.
  size_t vectorSize = 5;
  auto intVector =
      makeFlatVector<int32_t>(vectorSize, [](auto row) { return row; });
  auto arrayVector = makeArrayVector<int32_t>(
      100,
      [](auto /* row */) { return 2; },
      [](auto row, auto index) { return row * index; });
  auto nullArray = makeNullableArrayVector<int32_t>({std::nullopt});
  auto indices = makeIndicesInReverse(vectorSize);
  auto nulls = makeNulls(vectorSize, nullEvery(2));
  BufferPtr noNulls = nullptr;

  // Case 1: Dictionary(Constant(Flat))
  // Dictionary: no nulls, Constant: no null
  {
    auto constant = BaseVector::wrapInConstant(vectorSize, 1, intVector);
    auto dict =
        BaseVector::wrapInDictionary(noNulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_TRUE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == constant.get());
    auto wrappedVector = decodedVector.wrap(constant, *dict, vectorSize);
    EXPECT_TRUE(wrappedVector->isConstantEncoding());
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 2: Dictionary(Constant(Complex))
  // Dictionary: no nulls, Constant: no null
  {
    auto constant = BaseVector::wrapInConstant(vectorSize, 1, arrayVector);
    auto dict =
        BaseVector::wrapInDictionary(noNulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_TRUE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == arrayVector.get());
    auto wrappedVector = decodedVector.wrap(arrayVector, *dict, vectorSize);
    EXPECT_TRUE(wrappedVector->isConstantEncoding());
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 3: Dictionary(Constant(Flat))
  // Dictionary: no nulls, Constant: null
  {
    auto constant = BaseVector::createNullConstant(
        intVector->type(), vectorSize, intVector->pool());
    auto dict =
        BaseVector::wrapInDictionary(noNulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_TRUE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == constant.get());
    auto wrappedVector = decodedVector.wrap(constant, *dict, vectorSize);
    EXPECT_TRUE(wrappedVector->isConstantEncoding());
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 4: Dictionary(Constant(Complex))
  // Dictionary: no nulls, Constant: null
  {
    auto constant = BaseVector::wrapInConstant(vectorSize, 0, nullArray);
    auto dict =
        BaseVector::wrapInDictionary(noNulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_TRUE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == constant->valueVector().get());
    // Resultant wrap would wrap input vector into a null constant wrap.
    auto wrappedVector = decodedVector.wrap(arrayVector, *dict, vectorSize);
    EXPECT_TRUE(wrappedVector->isConstantEncoding());
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 5: Dictionary(Constant(Flat))
  // Dictionary: adds nulls, Constant: no null
  {
    auto constant = BaseVector::wrapInConstant(vectorSize, 1, intVector);
    auto dict =
        BaseVector::wrapInDictionary(nulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_FALSE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == constant.get());
    auto wrappedVector = decodedVector.wrap(constant, *dict, vectorSize);
    EXPECT_TRUE(isDictionary(wrappedVector->encoding()));
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 6: Dictionary(Constant(Complex))
  // Dictionary: adds nulls, Constant: no null
  {
    auto constant = BaseVector::wrapInConstant(vectorSize, 1, arrayVector);
    auto dict =
        BaseVector::wrapInDictionary(nulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_FALSE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == arrayVector.get());
    auto wrappedVector = decodedVector.wrap(arrayVector, *dict, vectorSize);
    EXPECT_TRUE(isDictionary(wrappedVector->encoding()));
    assertEqualVectors(dict, wrappedVector);
  }
  // Case 7: Dictionary(Constant(Flat))
  // Dictionary: adds nulls, Constant: null
  {
    auto constant = BaseVector::createNullConstant(
        intVector->type(), vectorSize, intVector->pool());
    auto dict =
        BaseVector::wrapInDictionary(nulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_TRUE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == constant.get());
    auto wrappedVector = decodedVector.wrap(constant, *dict, vectorSize);
    EXPECT_TRUE(wrappedVector->isConstantEncoding());
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 8: Dictionary(Constant(Complex))
  // Dictionary: adds nulls, Constant: null
  {
    auto constant = BaseVector::wrapInConstant(vectorSize, 0, nullArray);
    auto dict =
        BaseVector::wrapInDictionary(nulls, indices, vectorSize, constant);

    DecodedVector decodedVector(*dict);
    EXPECT_TRUE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == constant->valueVector().get());
    // Resultant wrap would wrap input vector into a null constant wrap.
    auto wrappedVector = decodedVector.wrap(arrayVector, *dict, vectorSize);
    EXPECT_TRUE(wrappedVector->isConstantEncoding());
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 9: Dictionary(Flat)
  // Dictionary: adds nulls, Flat: has nulls
  // verify that nulls from the base (innermost vector) are also propagated in
  // the wrap. There used to exist a shortcut for Dictionary over flat where the
  // indices and nulls of the original dictionary layer were returned, which
  // would not contain the nulls from the base.
  {
    // Flat vector identical intVector but has a null at index 1.
    auto intNullableVector =
        makeFlatVector<int32_t>(vectorSize, [](auto row) { return row; });
    intNullableVector->setNull(1, true);
    // Dict null at indices = 0, 2, 4
    auto dict = BaseVector::wrapInDictionary(
        nulls, indices, vectorSize, intNullableVector);
    DecodedVector decodedVector(*dict);
    EXPECT_FALSE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == intNullableVector.get());

    // Now wrap intVector which does not have any nulls.
    auto wrappedVector = decodedVector.wrap(intVector, *dict, vectorSize);
    EXPECT_TRUE(isDictionary(wrappedVector->encoding()));
    // Ensure all nulls are propagated correctly.
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 10: Dictionary2(Dictionary1(Flat))
  // Dictionary1: adds nulls, Dictionary2: no nulls, Flat: has nulls
  // verify that nulls from the base (innermost vector) are also propagated in
  // the wrap.
  {
    // Flat vector identical intVector but has a null at index 1.
    auto intNullableVector =
        makeFlatVector<int32_t>(vectorSize, [](auto row) { return row; });
    intNullableVector->setNull(1, true);
    // Dict null at indices = 0, 2, 4
    auto dict = BaseVector::wrapInDictionary(
        nulls, indices, vectorSize, intNullableVector);
    dict = BaseVector::wrapInDictionary(nullptr, indices, vectorSize, dict);
    DecodedVector decodedVector(*dict);
    EXPECT_FALSE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == intNullableVector.get());

    // Now wrap intVector which does not have any nulls.
    auto wrappedVector = decodedVector.wrap(intVector, *dict, vectorSize);
    EXPECT_TRUE(isDictionary(wrappedVector->encoding()));
    // Ensure all nulls are propagated correctly.
    assertEqualVectors(dict, wrappedVector);
  }

  // Case 11: Dictionary(Flat)
  // Dictionary: no nulls, Flat: has nulls
  // Ideally the nulls from the base (innermost vector) should be propagated in
  // the wrap. But a previous fix tried to skip adding nulls if only the base
  // had nulls and the wrap did not have any nulls. This bug was introduced in
  // #2678 as a solution to another bug caused due to how Cast Expr mis-uses the
  // wrap functionality to implement its own peeling. Therefore, it should be
  // reverted once #3553 is merged. Keeping this test case for now with a
  // TODO to ensure it is updated with the right behavior once its fixed.
  {
    // Flat vector identical intVector but has a null at index 1.
    auto intNullableVector =
        makeFlatVector<int32_t>(vectorSize, [](auto row) { return row; });
    intNullableVector->setNull(1, true);
    auto dict = BaseVector::wrapInDictionary(
        noNulls, indices, vectorSize, intNullableVector);
    DecodedVector decodedVector(*dict);
    EXPECT_FALSE(decodedVector.isConstantMapping());
    EXPECT_TRUE(decodedVector.base() == intNullableVector.get());

    // Now wrap intVector which does not have any nulls.
    auto wrappedVector = decodedVector.wrap(intVector, *dict, vectorSize);
    EXPECT_TRUE(isDictionary(wrappedVector->encoding()));
    // TODO: Switch this to ensure all nulls from base are propagated correctly
    // which should be the correct expected behavior.
    auto exptected =
        BaseVector::wrapInDictionary(noNulls, indices, vectorSize, intVector);
    assertEqualVectors(exptected, wrappedVector);
  }
}

TEST_F(DecodedVectorTest, noValues) {
  // Tests decoding a flat vector that consists of all nulls and has
  // no values() buffer.
  constexpr vector_size_t kSize = 100;

  auto vector = std::make_shared<FlatVector<int32_t>>(
      pool_.get(),
      INTEGER(),
      allocateNulls(kSize, pool(), bits::kNull),
      kSize,
      BufferPtr(nullptr),
      std::vector<BufferPtr>{});

  auto check = [&](auto& decoded) {
    EXPECT_EQ(nullptr, decoded.template data<int32_t>());
    EXPECT_TRUE(decoded.isNullAt(kSize - 1));
  };

  {
    SelectivityVector rows(kSize);
    DecodedVector decoded;
    decoded.decode(*vector, rows);
    check(decoded);
  }

  {
    DecodedVector decoded;
    decoded.decode(*vector);
    check(decoded);
  }
}

/// Test decoding Dict(Const) + nulls for empty rows.
TEST_F(DecodedVectorTest, emptyRowsDictOverConstWithNulls) {
  auto nulls = makeNulls(3, [](auto /* row */) { return true; });
  auto indices = makeIndices(3, [](auto /* row */) { return 2; });
  auto dict = BaseVector::wrapInDictionary(
      nulls, indices, 3, BaseVector::createConstant(INTEGER(), 1, 3, pool()));

  {
    SelectivityVector rows(3, false);
    DecodedVector d(*dict, rows, false);
    VELOX_CHECK_EQ(d.size(), 0);
    VELOX_CHECK_NOT_NULL(d.indices());
  }

  {
    auto emptyDict = wrapInDictionary(allocateIndices(0, pool()), 0, dict);
    DecodedVector d(*emptyDict);
    VELOX_CHECK_EQ(d.size(), 0);
    VELOX_CHECK_NOT_NULL(d.indices());
  }
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

  {
    SelectivityVector emptyRows(100, false);
    DecodedVector d(*dict, emptyRows, false);
    VELOX_CHECK_EQ(d.size(), 0);
    VELOX_CHECK_NOT_NULL(d.indices());
  }

  {
    auto emptyDict = wrapInDictionary(allocateIndices(0, pool()), 0, dict);
    DecodedVector d(*emptyDict);
    VELOX_CHECK_EQ(d.size(), 0);
    VELOX_CHECK_NOT_NULL(d.indices());
  }
}

TEST_F(DecodedVectorTest, flatNulls) {
  // Flat vector with no nulls.
  auto flatNoNulls = makeFlatVector<int64_t>(100, [](auto row) { return row; });
  {
    SelectivityVector rows(100);
    DecodedVector d(*flatNoNulls, rows);
    assertNoNulls(d);
  }
  {
    DecodedVector d(*flatNoNulls);
    assertNoNulls(d);
  }

  // Flat vector with nulls.
  auto flatWithNulls = makeFlatVector<int64_t>(
      100, [](auto row) { return row; }, nullEvery(7));

  auto check = [&](auto& d) {
    ASSERT_TRUE(d.nulls(nullptr) != nullptr);
    for (auto i = 0; i < 100; ++i) {
      ASSERT_EQ(d.isNullAt(i), i % 7 == 0);
      ASSERT_EQ(bits::isBitNull(d.nulls(nullptr), i), i % 7 == 0);
    }
  };

  {
    SelectivityVector rows(100);
    DecodedVector d(*flatWithNulls, rows);
    check(d);
  }

  {
    DecodedVector d(*flatWithNulls);
    check(d);
  }
}

TEST_F(DecodedVectorTest, dictionaryOverFlatNulls) {
  SelectivityVector rows(100);
  DecodedVector d;

  auto flatNoNulls = makeFlatVector<int64_t>(100, [](auto row) { return row; });
  auto flatWithNulls = makeFlatVector<int64_t>(
      100, [](auto row) { return row; }, nullEvery(7));

  auto decodeAndCheckNulls = [&](auto& vector) {
    {
      d.decode(*vector, rows);
      assertNulls(vector, d, &rows);
    }

    {
      d.decode(*vector);
      assertNulls(vector, d);
    }
  };

  auto decodeAndCheckNotNulls = [&](auto& vector) {
    {
      d.decode(*vector, rows);
      assertNoNulls(d, &rows);
    }

    {
      d.decode(*vector);
      assertNoNulls(d);
    }
  };

  // Dictionary over flat with no nulls.
  auto dict = wrapInDictionary(makeIndicesInReverse(100), 100, flatNoNulls);
  decodeAndCheckNotNulls(dict);

  // Dictionary over flat with nulls.
  auto indices = makeIndicesInReverse(100);
  dict = wrapInDictionary(indices, 100, flatWithNulls);
  decodeAndCheckNulls(dict);

  // Dictionary that adds nulls over flat with no nulls.
  auto nulls = makeNulls(100, nullEvery(3));
  dict = BaseVector::wrapInDictionary(nulls, indices, 100, flatNoNulls);
  decodeAndCheckNulls(dict);

  // Dictionary that adds nulls over flat with nulls.
  dict = BaseVector::wrapInDictionary(nulls, indices, 100, flatWithNulls);
  decodeAndCheckNulls(dict);

  // 2 layers of dictionary over flat using all combinations of nulls/no-nulls
  // at each level.

  // Dict(Dict(Flat))
  dict = wrapInDictionary(
      indices, 100, wrapInDictionary(indices, 100, flatNoNulls));
  decodeAndCheckNotNulls(dict);

  // Dict(Dict(Flat-with-Nulls))
  dict = wrapInDictionary(
      indices, 100, wrapInDictionary(indices, 100, flatWithNulls));
  decodeAndCheckNulls(dict);

  // Dict(Dict-with-Nulls(Flat))
  dict = wrapInDictionary(
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatNoNulls));
  decodeAndCheckNulls(dict);

  // Dict(Dict-with-Nulls(Flat-with-Nulls))
  dict = wrapInDictionary(
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatWithNulls));
  decodeAndCheckNulls(dict);

  // Dict-with-Nulls(Dict(Flat))
  auto moreNulls = makeNulls(100, nullEvery(5));
  dict = BaseVector::wrapInDictionary(
      moreNulls, indices, 100, wrapInDictionary(indices, 100, flatNoNulls));
  decodeAndCheckNulls(dict);

  // Dict-with-Nulls(Dict-with-Nulls(Flat))
  dict = BaseVector::wrapInDictionary(
      moreNulls,
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatNoNulls));
  decodeAndCheckNulls(dict);

  // Dict-with-Nulls(Dict(Flat-with-Nulls))
  dict = BaseVector::wrapInDictionary(
      moreNulls, indices, 100, wrapInDictionary(indices, 100, flatWithNulls));
  decodeAndCheckNulls(dict);

  // Dict-with-Nulls(Dict-with-Nulls(Flat-with-Nulls))
  dict = BaseVector::wrapInDictionary(
      moreNulls,
      indices,
      100,
      BaseVector::wrapInDictionary(nulls, indices, 100, flatWithNulls));
  decodeAndCheckNulls(dict);
}

TEST_F(DecodedVectorTest, dictionaryWrapping) {
  constexpr vector_size_t baseVectorSize{100};
  constexpr vector_size_t innerDictSize{30};
  constexpr vector_size_t outerDictSize{15};
  VectorPtr dict;
  BufferPtr nullsBuffer;

  // Prepare indices to take every third element from the base vector.
  auto innerIndices =
      makeIndices(innerDictSize, [](auto row) { return row * 3; });
  // Indices for the outer dictionary (need two or more dictionaries so we
  // don't hit the simplified path for a single level dictionary).
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
    {
      SelectivityVector rows(outerDictSize);
      DecodedVector decoded;

      decoded.decode(*dict, rows);
      auto wrapping = decoded.dictionaryWrapping(*dict, rows);
      auto wrapped = BaseVector::wrapInDictionary(
          std::move(wrapping.nulls),
          std::move(wrapping.indices),
          rows.end(),
          baseWithNulls);
      assertEqualVectors(dict, wrapped);
    }

    // Test when rows are not passed.
    {
      DecodedVector decoded;
      decoded.decode(*dict);
      auto wrapping = decoded.dictionaryWrapping(*dict, outerDictSize);
      auto wrapped = BaseVector::wrapInDictionary(
          std::move(wrapping.nulls),
          std::move(wrapping.indices),
          outerDictSize,
          baseWithNulls);
      assertEqualVectors(dict, wrapped);
    }
  }
}

TEST_F(DecodedVectorTest, previousIndicesInReUsedDecodedVector) {
  // Verify that when DecodedVector is re-used with different set of valid rows,
  // then the unselected indices would still have valid values.

  // Create a Dict(Dict(flat)) where merged indices point to a large index.
  // 2-layers are created to ensure copiedIndices_ is used.
  auto indices = makeIndices(3, [](auto /* row */) { return 2; });
  auto innerindices = makeIndices(3, [](auto /* row */) { return 998; });
  auto flat = makeFlatVector<int64_t>(1000, [](auto row) { return row; });
  auto dict = BaseVector::wrapInDictionary(nullptr, innerindices, 3, flat);
  dict = BaseVector::wrapInDictionary(nullptr, indices, 3, dict);

  // Create another Dict(Dict(flat)) where merged indices point to a small
  // index.
  auto indices2 = makeIndices(3, [](auto /* row */) { return 0; });
  auto innerindices2 = makeIndices(3, [](auto /* row */) { return 0; });
  auto flat2 = makeNullableFlatVector<int64_t>({1, std::nullopt});
  auto dict2 = BaseVector::wrapInDictionary(nullptr, innerindices2, 3, flat2);
  dict2 = BaseVector::wrapInDictionary(nullptr, indices2, 3, dict2);

  // Used the first time with all selected rows.
  DecodedVector d(*dict);

  // 0, 1 row is not selected and DecodedVector is now re-used with this
  // selectivity.
  SelectivityVector rows(3, false);
  rows.setValid(2, true);
  rows.updateBounds();
  d.decode(*dict2, rows);
  auto wrapping = d.dictionaryWrapping(*d.base(), d.base()->size());
  auto rawIndices = wrapping.indices->as<vector_size_t>();
  // Ensure the previous index on the unselected row is reset.
  EXPECT_EQ(rawIndices[0], 0);
}

TEST_F(DecodedVectorTest, toString) {
  auto vector = makeNullableFlatVector<int32_t>({1, std::nullopt, 3});
  {
    DecodedVector decoded(*vector);
    EXPECT_EQ("1", decoded.toString(0));
    EXPECT_EQ("null", decoded.toString(1));
    EXPECT_EQ("3", decoded.toString(2));
  }

  auto dict = wrapInDictionary(makeIndicesInReverse(3), vector);
  {
    DecodedVector decoded(*dict);
    EXPECT_EQ("3", decoded.toString(0));
    EXPECT_EQ("null", decoded.toString(1));
    EXPECT_EQ("1", decoded.toString(2));
  }

  auto constant = makeConstant<int32_t>(123, 10);
  {
    DecodedVector decoded(*constant);
    EXPECT_EQ("123", decoded.toString(0));
    EXPECT_EQ("123", decoded.toString(1));
    EXPECT_EQ("123", decoded.toString(2));
  }

  constant = makeNullConstant(TypeKind::INTEGER, 5);
  {
    DecodedVector decoded(*constant);
    EXPECT_EQ("null", decoded.toString(0));
    EXPECT_EQ("null", decoded.toString(1));
    EXPECT_EQ("null", decoded.toString(2));
  }

  dict = BaseVector::wrapInDictionary(
      makeNulls(10, nullEvery(2)),
      makeIndices(10, [](auto row) { return row % 3; }),
      10,
      makeFlatVector<int32_t>({1, 2, 3}));
  {
    DecodedVector decoded(*dict);
    EXPECT_EQ("null", decoded.toString(0));
    EXPECT_EQ("2", decoded.toString(1));
    EXPECT_EQ("null", decoded.toString(2));
    EXPECT_EQ("1", decoded.toString(3));
  }
}

} // namespace facebook::velox::test

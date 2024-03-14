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
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorMap.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class EncodingTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
  }

  EncodingTest() = default;

  template <typename T>
  T testValue(int32_t i, BufferPtr& space) {
    return i;
  }

  VectorPtr mapFromScalar(VectorPtr vector, int itemsPerMap) {
    auto keys = makeFlatVector<int32_t>(
        vector->size(), [&](auto row) { return row % itemsPerMap; });
    auto numMaps = bits::roundUp(vector->size(), itemsPerMap) / itemsPerMap;
    auto sizes = makeIndices(numMaps, [&](auto) { return itemsPerMap; });
    auto offsets =
        makeIndices(numMaps, [&](auto row) { return row * itemsPerMap; });
    return std::make_shared<MapVector>(
        pool_.get(),
        MAP(keys->type(), vector->type()),
        BufferPtr(nullptr),
        numMaps,
        offsets,
        sizes,
        keys,
        vector);
  }

  template <TypeKind KIND>
  VectorPtr createScalar(
      TypePtr type,
      vector_size_t size,
      int32_t numDistinct,
      int32_t step,
      bool withNulls) {
    using T = typename TypeTraits<KIND>::NativeType;
    BufferPtr buffer;
    VectorPtr base = BaseVector::create(type, size, pool());
    auto flat = std::dynamic_pointer_cast<FlatVector<T>>(base);
    for (int32_t i = 0; i < flat->size(); ++i) {
      if (withNulls && i % 3 == 0) {
        flat->setNull(i, true);
      } else {
        flat->set(i, testValue<T>((i % numDistinct) * step, buffer));
      }
    }
    return base;
  }

  template <TypeKind kind>
  void checkTypeEncoding(const TypePtr& type) {
    auto vector = createScalar<kind>(type, 1000, 1, 0, false);
    auto constant = BaseVector::constantify(vector);
    assertEqualVectors(vector, constant);
    auto indices =
        makeIndices(vector->size(), [](auto row) { return row / 2; });
    auto wrappedVector = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, vector->size(), vector);
    assertEqualVectors(constant, BaseVector::constantify(wrappedVector));

    auto row = makeRowVector({"c0"}, {vector});
    auto constantRow = BaseVector::constantify(row);
    assertEqualVectors(row, constantRow);

    vector = createScalar<kind>(type, 1000, 1, 0, true);
    // A nullable vector does not make a constant.
    EXPECT_TRUE(BaseVector::constantify(vector) == nullptr);
    // It has 2 values, null and the single value.
    checkDictionarize(vector, 2);

    if (kind == TypeKind::BOOLEAN || kind == TypeKind::TINYINT) {
      return;
    }

    vector = createScalar<kind>(type, 1000, 1000, 1, false);

    // A vector with different values does not make a constant.
    EXPECT_TRUE(BaseVector::constantify(vector) == nullptr);
    checkDictionarize(vector, 1000);
    row = makeRowVector({"c0"}, {vector});
    EXPECT_TRUE(BaseVector::constantify(row) == nullptr);

    checkDictionarize(row, 1000);

    vector = createScalar<kind>(type, 1000, 10, 1, false);
    // The vector has values repeating in in a 10 value cycle. If each 10 values
    // are map values with the same key, the map from the base is constant. If
    // we take every 5 consecutive values as map values with the same keys, we
    // have 2 distinct maps.
    auto map = mapFromScalar(vector, 10);
    auto constantMap = BaseVector::constantify(map);
    assertEqualVectors(map, constantMap);
    map = mapFromScalar(vector, 5);
    EXPECT_TRUE(BaseVector::constantify(map) == nullptr);
    checkDictionarize(map, 2);
  }

  void checkDictionarize(const VectorPtr& vector, int expectDistincts) {
    auto indices =
        AlignedBuffer::allocate<vector_size_t>(vector->size(), pool_.get());
    VectorMap map(*vector);
    EXPECT_EQ(expectDistincts, map.size());

    VectorMap map2(vector->type(), pool_.get());
    raw_vector<vector_size_t> temp;
    folly::Range<const vector_size_t*> rows(
        iota(vector->size(), temp), vector->size());
    map2.addMultiple(*vector, rows, true, indices->asMutable<vector_size_t>());
    EXPECT_EQ(expectDistincts, map2.size());
    assertEqualVectors(
        vector,
        BaseVector::wrapInDictionary(
            BufferPtr(nullptr), indices, vector->size(), map2.alphabetOwned()));
    if (vector->typeKind() == TypeKind::VARCHAR ||
        vector->typeKind() == TypeKind::VARBINARY) {
      for (auto i = 0; i < vector->size(); ++i) {
        auto length = vector->isNullAt(i)
            ? 0
            : vector->as<SimpleVector<StringView>>()->valueAt(i).size() + 4;
        EXPECT_EQ(map2.lengthAt(indices->as<int32_t>()[i]), length);
      }
    }
  }
};

template <>
int128_t EncodingTest::testValue<int128_t>(int32_t i, BufferPtr& /*space*/) {
  return HugeInt::build(i % 2 ? (i * -1) : i, 0xAAAAAAAAAAAAAAAA);
}

template <>
StringView EncodingTest::testValue(int32_t n, BufferPtr& buffer) {
  if (!buffer || buffer->capacity() < 1000) {
    buffer = AlignedBuffer::allocate<char>(1000, pool());
  }
  std::stringstream out;
  out << n;
  for (int32_t i = 0; i < n % 20; ++i) {
    out << " " << i * i;
  }
  std::string str = out.str();
  EXPECT_LE(str.size(), buffer->capacity());
  memcpy(buffer->asMutable<char>(), str.data(), str.size());
  return StringView(buffer->as<char>(), str.size());
}

template <>
bool EncodingTest::testValue(int32_t i, BufferPtr& /*space*/) {
  return (i % 2) == 1;
}

template <>
Timestamp EncodingTest::testValue(int32_t i, BufferPtr& /*space*/) {
  // Return even milliseconds.
  return Timestamp(i * 1000, (i % 1000) * 1000000);
}

TEST_F(EncodingTest, basic) {
  checkTypeEncoding<TypeKind::BOOLEAN>(BOOLEAN());
  checkTypeEncoding<TypeKind::TINYINT>(TINYINT());
  checkTypeEncoding<TypeKind::SMALLINT>(SMALLINT());
  checkTypeEncoding<TypeKind::INTEGER>(INTEGER());
  checkTypeEncoding<TypeKind::BIGINT>(BIGINT());
  checkTypeEncoding<TypeKind::VARCHAR>(VARCHAR());
  checkTypeEncoding<TypeKind::TIMESTAMP>(TIMESTAMP());
}

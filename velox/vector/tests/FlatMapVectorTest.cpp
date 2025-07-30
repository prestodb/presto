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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {
namespace {

class FlatMapVectorTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  template <typename TKey, typename TValue>
  void testFlatMapToMap(
      const std::vector<
          std::optional<std::vector<std::pair<TKey, std::optional<TValue>>>>>&
          mapData) {
    auto mapVector = maker_.mapVector<TKey, TValue>(mapData);
    auto flatMapVector = maker_.flatMapVectorNullable<TKey, TValue>(mapData);
    assertEqualVectors(mapVector, flatMapVector->toMapVector());
  }

  void testFlatMapCopy(
      const std::vector<std::string>& baseData,
      const std::vector<std::string>& sourceData,
      const std::vector<std::string>& expectedData,
      const std::vector<BaseVector::CopyRange>& ranges) {
    auto baseFlatMap = makeFlatMapVectorFromJson<int64_t, int32_t>(baseData);
    auto sourceFlatMap =
        makeFlatMapVectorFromJson<int64_t, int32_t>(sourceData);
    baseFlatMap->copyRanges(
        sourceFlatMap.get(),
        folly::Range<const BaseVector::CopyRange*>{ranges});

    // Validate that the updated flat map matches the expectation.
    auto expectedFlatMap =
        makeFlatMapVectorFromJson<int64_t, int32_t>(expectedData);
    EXPECT_EQ(baseFlatMap->size(), expectedFlatMap->size());

    for (vector_size_t i = 0; i < baseFlatMap->size(); ++i) {
      EXPECT_EQ(
          baseFlatMap->compare(
              expectedFlatMap.get(), i, i, CompareFlags{.equalsOnly = true}),
          0)
          << "Failed at index " << i << ":\n  " << baseFlatMap->toString(i)
          << "\nvs.\n  " << expectedFlatMap->toString(i);
    }
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  VectorMaker maker_{pool_.get()};
};

TEST_F(FlatMapVectorTest, fail) {
  auto buildVector = [&](const TypePtr& type,
                         const VectorPtr& keys = nullptr,
                         const std::vector<VectorPtr>& mapValues = {},
                         const std::vector<BufferPtr>& inMaps = {}) {
    return std::make_shared<FlatMapVector>(
        pool_.get(), type, nullptr, 3, keys, mapValues, inMaps);
  };

  // Wrong type.
  VELOX_ASSERT_THROW(
      buildVector(BIGINT()), "FlatMapVector requires a MAP type.");

  auto type = MAP(INTEGER(), BIGINT());

  // Wrong distinct keys vectors type and encoding.
  auto wrongKeys = maker_.flatVector<int64_t>({101, 102});

  VELOX_ASSERT_THROW(
      buildVector(type, wrongKeys), "Unexpected key type: BIGINT");

  // Wrong number of map value vectors/buffers.
  auto keys = maker_.flatVector<int32_t>({101, 102});

  VELOX_ASSERT_THROW(
      buildVector(type, keys), "(2 vs. 0) Wrong number of map value vectors.");

  std::vector<VectorPtr> mapValues = {
      maker_.flatVector<int32_t>({0, 0, 0}),
      maker_.flatVector<int32_t>({1, 1, 1}),
  };

  // It's ok if in maps are not provided.
  ASSERT_NO_THROW(buildVector(type, keys, mapValues));

  // All good including in maps.
  ASSERT_NO_THROW(buildVector(
      type, keys, mapValues, std::vector<BufferPtr>{nullptr, nullptr}));
}

TEST_F(FlatMapVectorTest, empty) {
  auto flatMapVector = std::make_shared<FlatMapVector>(
      pool_.get(),
      MAP(BIGINT(), REAL()),
      nullptr,
      0,
      nullptr,
      std::vector<VectorPtr>{},
      std::vector<BufferPtr>{});

  EXPECT_EQ(flatMapVector->size(), 0);
  EXPECT_FALSE(flatMapVector->mayHaveNulls());

  EXPECT_EQ(*flatMapVector->type(), *MAP(BIGINT(), REAL()));
  EXPECT_EQ(flatMapVector->encoding(), VectorEncoding::Simple::FLAT_MAP);

  EXPECT_NE(flatMapVector->distinctKeys(), nullptr);
  EXPECT_EQ(flatMapVector->distinctKeys()->size(), 0);
  EXPECT_EQ(flatMapVector->numDistinctKeys(), 0);
  EXPECT_TRUE(flatMapVector->mapValues().empty());
  EXPECT_TRUE(flatMapVector->inMaps().empty());

  EXPECT_EQ(flatMapVector->projectKey((int64_t)0), nullptr);
  EXPECT_EQ(flatMapVector->projectKey((int64_t)1), nullptr);
  EXPECT_EQ(flatMapVector->projectKey((int64_t)101), nullptr);

  VELOX_ASSERT_THROW(flatMapVector->sizeAt(0), "");
  VELOX_ASSERT_THROW(
      flatMapVector->mapValuesAt(0),
      "(0 vs. 0) Trying to access non-existing key channel in FlatMapVector.");
  VELOX_ASSERT_THROW(
      flatMapVector->toString(0), "Vector index should be less than length.");
}

TEST_F(FlatMapVectorTest, simple) {
  auto flatMapVector = maker_.flatMapVector<std::string, std::string>({
      {{"a", "1"}, {"b", "2"}},
      {{"a", "This is a test"}, {"b", "This is another test"}, {"c", "test"}},
      {{"b", "5"}, {"d", "last"}},
  });

  EXPECT_EQ(flatMapVector->size(), 3);
  EXPECT_EQ(*MAP(VARCHAR(), VARCHAR()), *flatMapVector->type());
  EXPECT_FALSE(flatMapVector->mayHaveNulls());

  // Validate we have the right distinct keys.
  auto distinctKeys = flatMapVector->distinctKeys();
  EXPECT_NE(distinctKeys, nullptr);
  EXPECT_EQ(distinctKeys->size(), 4);
  EXPECT_EQ(flatMapVector->numDistinctKeys(), 4);

  // Map sizes for each row.
  EXPECT_EQ(flatMapVector->sizeAt(0), 2);
  EXPECT_EQ(flatMapVector->sizeAt(1), 3);
  EXPECT_EQ(flatMapVector->sizeAt(2), 2);

  // Validate values of key projections.
  auto mapValues = flatMapVector->projectKey("a")->as<FlatVector<StringView>>();
  EXPECT_EQ(mapValues->size(), 3);
  EXPECT_EQ(mapValues->valueAt(0), "1");
  EXPECT_EQ(mapValues->valueAt(1), "This is a test");
  EXPECT_TRUE(mapValues->isNullAt(2)); // not in map is also set to null.

  mapValues = flatMapVector->projectKey("b")->as<FlatVector<StringView>>();
  EXPECT_EQ(mapValues->size(), 3);
  EXPECT_EQ(mapValues->valueAt(0), "2");
  EXPECT_EQ(mapValues->valueAt(1), "This is another test");
  EXPECT_EQ(mapValues->valueAt(2), "5");

  EXPECT_EQ(flatMapVector->projectKey("xyz"), nullptr);

  // Getting key channels.
  EXPECT_EQ(*flatMapVector->getKeyChannel("a"), 0);
  EXPECT_EQ(*flatMapVector->getKeyChannel("b"), 1);
  EXPECT_EQ(*flatMapVector->getKeyChannel("c"), 2);
  EXPECT_EQ(*flatMapVector->getKeyChannel("d"), 3);
  EXPECT_EQ(flatMapVector->getKeyChannel("e"), std::nullopt);

  // Validate in maps.
  auto channel = flatMapVector->getKeyChannel("a");
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 0));
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 1));
  EXPECT_FALSE(flatMapVector->isInMap(*channel, 2));

  channel = flatMapVector->getKeyChannel("b");
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 0));
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 1));
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 2));

  channel = flatMapVector->getKeyChannel("a");
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 0));
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 1));
  EXPECT_FALSE(flatMapVector->isInMap(*channel, 2));
}

TEST_F(FlatMapVectorTest, withNulls) {
  // Nullable flat map.
  auto flatMapVector = maker_.flatMapVectorNullable<int64_t, int64_t>({
      // null row.
      std::nullopt,
      // empty vector/map.
      std::vector<std::pair<int64_t, std::optional<int64_t>>>{},
      // null value.
      {{{42L, std::nullopt}}},
  });

  EXPECT_EQ(flatMapVector->size(), 3);
  EXPECT_EQ(*MAP(BIGINT(), BIGINT()), *flatMapVector->type());
  EXPECT_TRUE(flatMapVector->mayHaveNulls());

  // Top-level nulls.
  EXPECT_TRUE(flatMapVector->isNullAt(0));
  EXPECT_FALSE(flatMapVector->isNullAt(1));
  EXPECT_FALSE(flatMapVector->isNullAt(2));

  EXPECT_TRUE(flatMapVector->containsNullAt(0));
  EXPECT_FALSE(flatMapVector->containsNullAt(1));
  EXPECT_TRUE(flatMapVector->containsNullAt(2));

  // Check sizes of maps for each row.
  EXPECT_EQ(flatMapVector->sizeAt(0), 0);
  EXPECT_EQ(flatMapVector->sizeAt(1), 0);
  EXPECT_EQ(flatMapVector->sizeAt(2), 1);

  // Check values for projected key 42.
  auto channel = flatMapVector->getKeyChannel((int64_t)42);
  ASSERT_NE(channel, std::nullopt);

  auto projectedKey = flatMapVector->mapValuesAt(*channel);

  EXPECT_TRUE(projectedKey->isNullAt(0)); // not in map.
  EXPECT_TRUE(projectedKey->isNullAt(1)); // not in map.
  EXPECT_TRUE(projectedKey->isNullAt(2)); // actual null.

  EXPECT_FALSE(flatMapVector->isInMap(*channel, 0));
  EXPECT_FALSE(flatMapVector->isInMap(*channel, 1));
  EXPECT_TRUE(flatMapVector->isInMap(*channel, 2));
}

TEST_F(FlatMapVectorTest, primitiveKeys) {
  // bigint key.
  auto flatMapVector = maker_.flatMapVector<int64_t, double>({
      {{101, 0}, {102, 0}, {1'234'567'890, 0}},
  });
  EXPECT_EQ(*flatMapVector->keyType(), *BIGINT());

  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)101), 0);
  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)102), 1);
  EXPECT_EQ(flatMapVector->getKeyChannel((int64_t)100), std::nullopt);
  EXPECT_EQ(flatMapVector->getKeyChannel((int64_t)103), std::nullopt);
  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)1'234'567'890), 2);
  EXPECT_EQ(flatMapVector->getKeyChannel((int64_t)1'234'567'891), std::nullopt);

  // Wrong key type.
  VELOX_ASSERT_THROW(
      *flatMapVector->getKeyChannel((int32_t)101),
      "Incompatible vector type for flat map vector keys");
  VELOX_ASSERT_THROW(
      *flatMapVector->getKeyChannel("bad key"),
      "Incompatible vector type for flat map vector keys");

  // integer key.
  flatMapVector = maker_.flatMapVector<int32_t, double>({
      {{10'987, 0}, {99, 0}, {1'234'567, 0}},
  });

  EXPECT_EQ(*flatMapVector->getKeyChannel((int32_t)10'987), 0);
  EXPECT_EQ(*flatMapVector->getKeyChannel((int32_t)99), 1);
  EXPECT_EQ(flatMapVector->getKeyChannel((int32_t)100), std::nullopt);
  EXPECT_EQ(*flatMapVector->getKeyChannel((int32_t)1'234'567), 2);
  EXPECT_EQ(flatMapVector->getKeyChannel((int32_t)1'234'568), std::nullopt);

  // Wrong key type.
  VELOX_ASSERT_THROW(
      *flatMapVector->getKeyChannel((int64_t)101),
      "Incompatible vector type for flat map vector keys");
  VELOX_ASSERT_THROW(
      *flatMapVector->getKeyChannel("bad key"),
      "Incompatible vector type for flat map vector keys");

  // string key.
  flatMapVector = maker_.flatMapVector<std::string, double>({
      {{"k1", 0}, {"k2", 0}},
  });

  EXPECT_EQ(*flatMapVector->getKeyChannel("k1"), 0);
  EXPECT_EQ(*flatMapVector->getKeyChannel("k2"), 1);
  EXPECT_EQ(flatMapVector->getKeyChannel("k3"), std::nullopt);

  // Wrong key type.
  VELOX_ASSERT_THROW(
      *flatMapVector->getKeyChannel((int64_t)101),
      "Incompatible vector type for flat map vector keys");
  VELOX_ASSERT_THROW(
      *flatMapVector->getKeyChannel((int32_t)1),
      "Incompatible vector type for flat map vector keys");
}

TEST_F(FlatMapVectorTest, complexKeys) {
  // Distinct keys vector.
  auto arrayVector = maker_.arrayVector<int64_t>({
      {{1, 2, 3}, {1, 2}, {1}},
  });

  std::vector<VectorPtr> mapValues{
      // For key [1, 2, 3]
      maker_.flatVector<int64_t>({101, 102}),

      // For key [1, 2]
      maker_.flatVector<int64_t>({201, 202}),

      // For key [1]
      maker_.flatVector<int64_t>({301, 302}),
  };

  auto flatMapVector = std::make_shared<FlatMapVector>(
      pool_.get(),
      MAP(ARRAY(BIGINT()), REAL()),
      nullptr,
      2,
      arrayVector,
      std::move(mapValues),
      std::vector<BufferPtr>{nullptr, nullptr, nullptr});

  EXPECT_EQ(*flatMapVector->keyType(), *ARRAY(BIGINT()));

  // For key [1, 2, 3]
  auto channel = flatMapVector->getKeyChannel(arrayVector, 0);
  ASSERT_NE(channel, std::nullopt);
  auto projectedKey =
      flatMapVector->mapValuesAt(*channel)->as<FlatVector<int64_t>>();

  EXPECT_EQ(projectedKey->size(), 2);
  EXPECT_EQ(projectedKey->valueAt(0), 101);
  EXPECT_EQ(projectedKey->valueAt(1), 102);

  // For key [1, 2]
  channel = flatMapVector->getKeyChannel(arrayVector, 1);
  ASSERT_NE(channel, std::nullopt);
  projectedKey =
      flatMapVector->mapValuesAt(*channel)->as<FlatVector<int64_t>>();

  EXPECT_EQ(projectedKey->size(), 2);
  EXPECT_EQ(projectedKey->valueAt(0), 201);
  EXPECT_EQ(projectedKey->valueAt(1), 202);
}

TEST_F(FlatMapVectorTest, setDistinctKeys) {
  // Distinct keys vector.
  auto distinctKeys = maker_.flatVector<int64_t>({
      {101, 102, 103},
  });

  auto type = MAP(BIGINT(), REAL());
  auto flatMapVector = std::make_shared<FlatMapVector>(
      pool_.get(),
      type,
      nullptr,
      2,
      distinctKeys,
      std::vector<VectorPtr>{nullptr, nullptr, nullptr},
      std::vector<BufferPtr>{nullptr, nullptr, nullptr});

  EXPECT_EQ(flatMapVector->numDistinctKeys(), 3);
  EXPECT_EQ(flatMapVector->mapValues().size(), 3);
  EXPECT_EQ(flatMapVector->inMaps().size(), 3);

  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)101), 0);
  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)102), 1);
  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)103), 2);

  // New vector of distinct keys.
  distinctKeys = maker_.flatVector<int64_t>({
      {20, 21, 22, 23, 24, 25, 26, 27, 28, 29},
  });
  flatMapVector->setDistinctKeys(distinctKeys);

  EXPECT_EQ(flatMapVector->numDistinctKeys(), 10);
  EXPECT_EQ(flatMapVector->mapValues().size(), 10);
  EXPECT_EQ(flatMapVector->inMaps().size(), 0);

  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)20), 0);
  EXPECT_EQ(*flatMapVector->getKeyChannel((int64_t)29), 9);

  EXPECT_EQ(flatMapVector->getKeyChannel((int64_t)101), std::nullopt);
  EXPECT_EQ(flatMapVector->getKeyChannel((int64_t)102), std::nullopt);
  EXPECT_EQ(flatMapVector->getKeyChannel((int64_t)103), std::nullopt);
}

TEST_F(FlatMapVectorTest, sortedKeyIndices) {
  auto flatMapVector = maker_.flatMapVectorNullable<int64_t, int64_t>({
      {{{101, 1}, {105, 5}, {100, 0}, {102, 2}}},
      {{{104, 4}, {102, 2}}},
      {std::nullopt},
  });

  auto indices = flatMapVector->sortedKeyIndices(0);
  ASSERT_THAT(indices, ::testing::ElementsAre(2, 0, 3, 1));

  indices = flatMapVector->sortedKeyIndices(1);
  ASSERT_THAT(indices, ::testing::ElementsAre(3, 4));

  indices = flatMapVector->sortedKeyIndices(2);
  ASSERT_THAT(indices, ::testing::ElementsAre());
}

TEST_F(FlatMapVectorTest, toString) {
  auto vector = maker_.flatMapVectorNullable<int64_t, int64_t>({
      {{}},
      {std::nullopt},
      {{{1, 0}}},
      {{{1, 1}, {2, std::nullopt}}},
      {{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}},
  });

  EXPECT_EQ(
      vector->toString(), "[FLAT_MAP MAP<BIGINT,BIGINT>: 5 elements, 1 nulls]");

  EXPECT_EQ(vector->toString(0), "<empty>");
  EXPECT_EQ(vector->toString(1), "null");
  EXPECT_EQ(vector->toString(2), "1 elements {1 => 0}");
  EXPECT_EQ(vector->toString(3), "2 elements {1 => 1, 2 => null}");
  EXPECT_EQ(
      vector->toString(4),
      "6 elements {1 => 1, 2 => 2, 0 => 0, 3 => 3, 4 => 4, ...}");
}

TEST_F(FlatMapVectorTest, compare) {
  auto vector = maker_.flatMapVectorNullable<int64_t, int64_t>({
      {std::nullopt}, // 0
      {{{1, 0}}}, // 1
      {{{1, 0}, {2, 0}}}, // 2
      {{{1, 0}, {3, 0}, {4, 0}}}, // 3
      {{{1, 0}, {3, 0}, {2, 0}}}, // 4
      {{{1, 0}, {2, 0}, {3, 0}}}, // 5
      {{{3, 0}, {1, 0}, {2, 1}}}, // 6
  });

  enum class Result {
    kEq,
    kNe,
    kLt,
    kGt,
  };

  const auto testCompare =
      [&](int32_t left, int32_t right, CompareFlags flags, Result expected) {
        auto mapVector = vector->toMapVector();
        auto flatMapFlatMapResult =
            vector->compare(vector.get(), left, right, flags);
        auto flatMapMapResult =
            vector->compare(mapVector.get(), left, right, flags);
        auto mapFlatMapResult =
            mapVector->compare(vector.get(), left, right, flags);

        switch (expected) {
          case Result::kEq:
            EXPECT_EQ(flatMapFlatMapResult, 0);
            EXPECT_EQ(flatMapMapResult, 0);
            EXPECT_EQ(mapFlatMapResult, 0);
            break;
          case Result::kNe:
            EXPECT_NE(flatMapFlatMapResult, 0);
            EXPECT_NE(flatMapMapResult, 0);
            EXPECT_NE(mapFlatMapResult, 0);
            break;
          case Result::kLt:
            EXPECT_LT(flatMapFlatMapResult, 0);
            EXPECT_LT(flatMapMapResult, 0);
            EXPECT_LT(mapFlatMapResult, 0);
            break;
          case Result::kGt:
            EXPECT_GT(flatMapFlatMapResult, 0);
            EXPECT_GT(flatMapMapResult, 0);
            EXPECT_GT(mapFlatMapResult, 0);
            break;
        }
      };

  // Null map equals to null.
  testCompare(0, 0, {}, Result::kEq);

  // Maps of different sizes.
  testCompare(1, 2, {.equalsOnly = true}, Result::kNe);

  testCompare(2, 3, {.equalsOnly = true}, Result::kLt);
  testCompare(3, 2, {.equalsOnly = true}, Result::kGt);

  testCompare(2, 3, {}, Result::kLt);
  testCompare(3, 2, {}, Result::kGt);

  testCompare(2, 4, {}, Result::kLt);
  testCompare(4, 2, {}, Result::kGt);

  testCompare(4, 5, {}, Result::kEq);
  testCompare(5, 4, {}, Result::kEq);

  testCompare(5, 6, {}, Result::kLt);
  testCompare(6, 5, {}, Result::kGt);

  // Descending.
  testCompare(5, 6, {.ascending = false}, Result::kGt);
  testCompare(6, 5, {.ascending = false}, Result::kLt);

  // Cannot compare maps with nulls as indeterminate.
  VELOX_ASSERT_THROW(
      vector->compare(
          vector.get(),
          6,
          5,
          {.nullHandlingMode =
               CompareFlags::NullHandlingMode::kNullAsIndeterminate}),
      "Map is not orderable type");

  // Cannot compare maps with nulls as indeterminate.
  VELOX_ASSERT_THROW(
      vector->compare(
          vector->toMapVector().get(),
          6,
          5,
          {.nullHandlingMode =
               CompareFlags::NullHandlingMode::kNullAsIndeterminate}),
      "Map is not orderable type");
}

TEST_F(FlatMapVectorTest, hash) {
  auto vector = maker_.flatMapVectorNullable<int64_t, int64_t>({
      {std::nullopt}, // 0
      {{{6, 0}}}, // 1
      {{{1, 0}, {2, 0}}}, // 2
      {{{1, 0}, {3, 0}, {2, 0}}}, // 3
      {{{1, 0}, {2, 0}, {3, 0}}}, // 4
      {{{1, 0}, {2, 0}, {3, 1}}}, // 5
      {{{1, 0}, {2, 0}, {3, std::nullopt}}}, // 6
      {std::nullopt}, // 7
  });

  // Nulls.
  EXPECT_EQ(vector->hashValueAt(0), vector->hashValueAt(7));

  EXPECT_NE(vector->hashValueAt(0), vector->hashValueAt(1));
  EXPECT_NE(vector->hashValueAt(1), vector->hashValueAt(2));
  EXPECT_NE(vector->hashValueAt(2), vector->hashValueAt(3));
  EXPECT_NE(vector->hashValueAt(4), vector->hashValueAt(5));
  EXPECT_NE(vector->hashValueAt(5), vector->hashValueAt(6));
  EXPECT_NE(vector->hashValueAt(4), vector->hashValueAt(6));

  EXPECT_EQ(vector->hashValueAt(3), vector->hashValueAt(4));
}

TEST_F(FlatMapVectorTest, slice) {
  auto vector = maker_.flatMapVectorNullable<int64_t, int64_t>({
      {{{101, 1}, {102, 2}, {103, 3}}},
      {{{105, 0}, {106, 0}}},
      {std::nullopt},
      {{{101, 11}, {103, 13}, {105, std::nullopt}}},
      {{{101, 1}, {102, 2}, {103, 3}}},
  });

  auto slicedVector = vector->slice(1, 3);

  EXPECT_EQ(slicedVector->size(), 3);
  EXPECT_EQ(slicedVector->compare(vector.get(), 0, 1), 0);
  EXPECT_EQ(slicedVector->compare(vector.get(), 1, 2), 0);
  EXPECT_EQ(slicedVector->compare(vector.get(), 2, 3), 0);

  EXPECT_NE(slicedVector->compare(vector.get(), 0, 0), 0);
}

TEST_F(FlatMapVectorTest, toMapVector) {
  testFlatMapToMap<int64_t, int64_t>({});
  testFlatMapToMap<int64_t, int64_t>({
      {{{0, 0}}},
  });
  testFlatMapToMap<int32_t, StringView>({
      {{{0, "0"}}},
      {{{1, "1"}}},
      {{{2, "2"}}},
      {{{3, std::nullopt}}},
  });

  testFlatMapToMap<int64_t, int64_t>({
      {{{101, 1}, {102, 2}, {103, 3}}},
      {{{105, 0}, {106, 0}}},
      {std::nullopt},
      {{{101, 11}, {103, 13}, {105, std::nullopt}}},
      {{{101, 1}, {102, 2}, {103, 3}}},
  });
}

TEST_F(FlatMapVectorTest, copyRanges) {
  std::vector<std::string> baseData = {
      "{1:10, 2:20, 3:null}",
      "null",
      "{}",
      "{4:40, 5:null, 6:null}",
  };

  std::vector<BaseVector::CopyRange> fullRange = {
      BaseVector::CopyRange{0, 0, (vector_size_t)baseData.size()},
  };

  // Empty and identity copy.
  testFlatMapCopy(baseData, {}, baseData, {});
  testFlatMapCopy(baseData, baseData, baseData, fullRange);

  std::vector<std::string> updatedData = {
      "{1:null, 3:30, 6:66}",
      "{7:60}",
      "{8:80}",
      "null",
  };

  // Test ranges with size zero.
  testFlatMapCopy(
      baseData, updatedData, baseData, {BaseVector::CopyRange{0, 0, 0}});
  testFlatMapCopy(
      baseData, updatedData, baseData, {BaseVector::CopyRange{1, 1, 0}});

  // Test copy all records in both directions.
  testFlatMapCopy(baseData, updatedData, updatedData, fullRange);
  testFlatMapCopy(updatedData, baseData, baseData, fullRange);

  // Copy first row only.
  testFlatMapCopy(
      baseData,
      updatedData,
      {
          "{1:null, 3:30, 6:66}",
          "null",
          "{}",
          "{4:40, 5:null, 6:null}",
      },
      {BaseVector::CopyRange{0, 0, 1}});

  // Only second row.
  testFlatMapCopy(
      baseData,
      updatedData,
      {
          "{1:10, 2:20, 3:null}",
          "{7:60}",
          "{}",
          "{4:40, 5:null, 6:null}",
      },
      {BaseVector::CopyRange{1, 1, 1}});

  // Second and third row.
  testFlatMapCopy(
      baseData,
      updatedData,
      {
          "{1:10, 2:20, 3:null}",
          "{7:60}",
          "{8:80}",
          "{4:40, 5:null, 6:null}",
      },
      {BaseVector::CopyRange{1, 1, 2}});

  // Copy first row into the second.
  testFlatMapCopy(
      baseData,
      updatedData,
      {
          "{1:10, 2:20, 3:null}",
          "{1:null, 3:30, 6:66}",
          "{}",
          "{4:40, 5:null, 6:null}",
      },
      {BaseVector::CopyRange{0, 1, 1}});

  // Copy second row into the first.
  testFlatMapCopy(
      baseData,
      updatedData,
      {
          "{7:60}",
          "null",
          "{}",
          "{4:40, 5:null, 6:null}",
      },
      {BaseVector::CopyRange{1, 0, 1}});

  // Copy elements 1 and 2 into 0 and 1.
  testFlatMapCopy(
      baseData,
      updatedData,
      {
          "{7:60}",
          "{8:80}",
          "{}",
          "{4:40, 5:null, 6:null}",
      },
      {BaseVector::CopyRange{1, 0, 2}});

  // Copy elements 0 and 1 into 1 and 2, and 3 into 3.
  testFlatMapCopy(
      baseData,
      updatedData,
      {
          "{1:10, 2:20, 3:null}",
          "{1:null, 3:30, 6:66}",
          "{7:60}",
          "null",
      },
      {BaseVector::CopyRange{0, 1, 2}, BaseVector::CopyRange{3, 3, 1}});
}

} // namespace
} // namespace facebook::velox::test

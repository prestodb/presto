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

#include "velox/common/fuzzer/ConstrainedGenerators.h"

#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/functions/prestosql/json/JsonExtractor.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/type/Variant.h"

namespace facebook::velox::fuzzer::test {

class ConstrainedGeneratorsTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  template <TypeKind KIND>
  void testRandomPrimitive(const TypePtr& type, bool testNull) {
    VELOX_CHECK_EQ(type->kind(), KIND);
    using T = typename TypeTraits<KIND>::NativeType;

    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<RandomInputGenerator<T>>(
            0, type, testNull ? 1.0 : 0.0);
    auto value = generator->generate();
    if (testNull) {
      EXPECT_FALSE(value.hasValue());
    } else {
      EXPECT_TRUE(value.hasValue());
    }
    EXPECT_EQ(value.kind(), KIND);
  }

  template <TypeKind KIND>
  void testRandomComplex(const TypePtr& type, bool testNull) {
    VELOX_CHECK_EQ(type->kind(), KIND);
    using T = typename TypeTraits<KIND>::ImplType;

    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<RandomInputGenerator<T>>(
            0, type, testNull ? 1.0 : 0.0);
    auto value = generator->generate();
    EXPECT_EQ(value.kind(), KIND);
    if (testNull) {
      EXPECT_FALSE(value.hasValue());
    } else {
      EXPECT_TRUE(value.hasValue());
    }
  }

  template <typename T>
  void testRangePrimitive(const TypePtr& type, T min, T max, bool testNull) {
    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<RangeConstrainedGenerator<T>>(
            0, type, testNull ? 1.0 : 0.0, min, max);

    const uint32_t kIterations = 1000;
    const variant lower = variant(min);
    const variant upper = variant(max);
    for (uint32_t i = 0; i < kIterations; ++i) {
      auto value = generator->generate();
      EXPECT_EQ(value.kind(), type->kind());
      if (testNull) {
        EXPECT_FALSE(value.hasValue());
      } else {
        EXPECT_TRUE(value.hasValue());
        EXPECT_TRUE(lower < value || lower == value);
        EXPECT_TRUE(value < upper || value == upper);
      }
    }
  }

  template <typename T>
  void testRangeInComplex(const TypePtr& type, T min, T max, bool testNull) {
    EXPECT_TRUE(type->isArray() && type->childAt(0)->isPrimitiveType());
    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<RandomInputGenerator<ArrayType>>(
            0,
            type,
            0.0,
            1000,
            std::make_unique<RangeConstrainedGenerator<T>>(
                0, type, testNull ? 1.0 : 0.0, min, max));

    const auto value = generator->generate();
    EXPECT_EQ(value.kind(), TypeKind::ARRAY);
    EXPECT_TRUE(value.hasValue());

    const auto elements = value.array();
    const variant lower = variant(min);
    const variant upper = variant(max);
    for (const auto& element : elements) {
      if (testNull) {
        EXPECT_FALSE(element.hasValue());
      } else {
        EXPECT_TRUE(element.hasValue());
        EXPECT_TRUE(lower < element || lower == element);
        EXPECT_TRUE(element < upper || element == upper);
      }
    }
  }

  template <typename T>
  void testContainInArray(const TypePtr& type, T containedValue) {
    EXPECT_TRUE(type->isArray() && type->childAt(0)->isPrimitiveType());
    const auto kMaxLength = 10;
    const auto kContainAtIndex = 5;
    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<RandomInputGenerator<ArrayType>>(
            0,
            type,
            0.0,
            kMaxLength,
            nullptr,
            kContainAtIndex,
            std::make_unique<SetConstrainedGenerator>(
                0,
                type->childAt(0),
                std::vector<variant>{variant(containedValue)}));

    const uint32_t kIterations = 100;
    for (auto i = 0; i < kIterations; ++i) {
      const auto value = generator->generate();
      EXPECT_EQ(value.kind(), TypeKind::ARRAY);
      EXPECT_TRUE(value.hasValue());

      const auto elements = value.array();
      EXPECT_EQ(elements[kContainAtIndex], variant(containedValue));
    }
  }

  template <typename TKey, typename TValue>
  void testContainInMap(
      const TypePtr& type,
      TKey containedKey,
      TValue containedValue) {
    EXPECT_TRUE(
        type->isMap() && type->childAt(0)->isPrimitiveType() &&
        type->childAt(1)->isPrimitiveType());
    const auto kMaxLength = 10;
    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<RandomInputGenerator<MapType>>(
            0,
            type,
            0.0,
            kMaxLength,
            nullptr,
            nullptr,
            std::make_unique<SetConstrainedGenerator>(
                0,
                type->childAt(0),
                std::vector<variant>{variant(containedKey)}),
            std::make_unique<SetConstrainedGenerator>(
                0,
                type->childAt(1),
                std::vector<variant>{variant(containedValue)}));

    const uint32_t kIterations = 100;
    for (auto i = 0; i < kIterations; ++i) {
      const auto value = generator->generate();
      EXPECT_EQ(value.kind(), TypeKind::MAP);
      EXPECT_TRUE(value.hasValue());

      const auto pairs = value.map();
      if (!pairs.empty()) {
        EXPECT_TRUE(pairs.count(variant{containedKey}) > 0);
        EXPECT_TRUE(pairs.at(variant{containedKey}) == variant{containedValue});
      }
    }
  }

  template <TypeKind KIND, typename TValue>
  void testNotEqualPrimitive(
      const TypePtr& type,
      const TValue& excludedValue,
      bool testNull) {
    VELOX_CHECK_EQ(type->kind(), KIND);
    using T = typename TypeTraits<KIND>::NativeType;

    variant excludedVariant{excludedValue};
    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<NotEqualConstrainedGenerator>(
            0,
            type,
            excludedVariant,
            std::make_unique<RandomInputGenerator<T>>(
                0, type, testNull ? 1.0 : 0.0));

    const uint32_t kIterations = 1000;
    for (uint32_t i = 0; i < kIterations; ++i) {
      auto value = generator->generate();
      EXPECT_EQ(value.kind(), KIND);
      if (testNull) {
        EXPECT_FALSE(value.hasValue());
      } else {
        EXPECT_TRUE(value.hasValue());
        EXPECT_NE(value, excludedVariant);
      }
    }
  }

  template <TypeKind KIND>
  void testNotEqualComplex(
      const TypePtr& type,
      const variant& excludedVariant,
      bool testNull) {
    VELOX_CHECK_EQ(type->kind(), KIND);
    using T = typename TypeTraits<KIND>::ImplType;

    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<NotEqualConstrainedGenerator>(
            0,
            type,
            excludedVariant,
            std::make_unique<RandomInputGenerator<T>>(
                0, type, testNull ? 1.0 : 0.0));

    const uint32_t kIterations = 1000;
    for (uint32_t i = 0; i < kIterations; ++i) {
      auto value = generator->generate();
      EXPECT_EQ(value.kind(), KIND);
      if (testNull) {
        EXPECT_FALSE(value.hasValue());
      } else {
        EXPECT_TRUE(value.hasValue());
        EXPECT_NE(value, excludedVariant);
      }
    }
  }

  template <TypeKind KIND, typename TSet>
  void testSetPrimitive(const TypePtr& type, const TSet& setOfRawValues) {
    VELOX_CHECK_EQ(type->kind(), KIND);

    const uint32_t kIterations = 1000;
    std::vector<variant> variants;
    for (const auto& value : setOfRawValues) {
      variants.push_back(variant{value});
    }
    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<SetConstrainedGenerator>(0, type, variants);

    for (uint32_t i = 0; i < kIterations; ++i) {
      auto value = generator->generate();
      EXPECT_TRUE(value.hasValue());
      EXPECT_EQ(value.kind(), KIND);
      EXPECT_NE(setOfRawValues.count(value.value<KIND>()), 0);
    }
  }

  template <TypeKind KIND>
  void testSetComplex(
      const TypePtr& type,
      const std::vector<variant>& variants) {
    VELOX_CHECK_EQ(type->kind(), KIND);

    std::set<variant> setOfVariants{variants.begin(), variants.end()};
    std::unique_ptr<AbstractInputGenerator> generator =
        std::make_unique<SetConstrainedGenerator>(0, type, variants);

    const uint32_t kIterations = 1000;
    for (uint32_t i = 0; i < kIterations; ++i) {
      auto value = generator->generate();
      EXPECT_TRUE(value.hasValue());
      EXPECT_EQ(value.kind(), KIND);
      EXPECT_NE(setOfVariants.count(value), 0);
    }
  }
};

TEST_F(ConstrainedGeneratorsTest, randomPrimitive) {
  testRandomPrimitive<TypeKind::INTEGER>(INTEGER(), false);
  testRandomPrimitive<TypeKind::VARCHAR>(VARCHAR(), false);

  testRandomPrimitive<TypeKind::INTEGER>(INTEGER(), true);
  testRandomPrimitive<TypeKind::VARCHAR>(VARCHAR(), true);
}

TEST_F(ConstrainedGeneratorsTest, randomComplex) {
  testRandomComplex<TypeKind::ARRAY>(
      ARRAY(MAP(VARCHAR(), ROW({BIGINT()}))), false);
  testRandomComplex<TypeKind::ARRAY>(
      ARRAY(MAP(VARCHAR(), ROW({BIGINT()}))), true);
}

TEST_F(ConstrainedGeneratorsTest, rangePrimitive) {
  testRangePrimitive<int64_t>(BIGINT(), 1, 10, false);
  testRangePrimitive<int64_t>(BIGINT(), 1, 10, true);

  testRangePrimitive<float>(REAL(), 1.0, 10.0, false);
  testRangePrimitive<float>(REAL(), 1.0, 10.0, true);
}

TEST_F(ConstrainedGeneratorsTest, rangeInComplex) {
  testRangeInComplex<int64_t>(ARRAY(BIGINT()), 1, 10, false);
  testRangeInComplex<int64_t>(ARRAY(BIGINT()), 1, 10, true);

  testRangeInComplex<float>(ARRAY(REAL()), 1.0, 10.0, false);
  testRangeInComplex<float>(ARRAY(REAL()), 1.0, 10.0, true);
}

TEST_F(ConstrainedGeneratorsTest, containInComplex) {
  testContainInArray<int64_t>(ARRAY(BIGINT()), 999);
  testContainInMap<std::string, int64_t>(MAP(VARCHAR(), BIGINT()), "key", 999);
}

TEST_F(ConstrainedGeneratorsTest, notEqPrimitive) {
  testNotEqualPrimitive<TypeKind::TINYINT>(
      TINYINT(), static_cast<int8_t>(1), false);
  testNotEqualPrimitive<TypeKind::VARCHAR>(VARCHAR(), ""_sv, false);

  testNotEqualPrimitive<TypeKind::TINYINT>(
      TINYINT(), static_cast<int8_t>(1), true);
  testNotEqualPrimitive<TypeKind::VARCHAR>(VARCHAR(), ""_sv, true);
}

TEST_F(ConstrainedGeneratorsTest, notEqComplex) {
  auto excludedVariant = variant::array({variant::map(
      {{variant{"1"}, variant::row({variant{static_cast<int32_t>(1)}})}})});
  testNotEqualComplex<TypeKind::ARRAY>(
      ARRAY(MAP(VARCHAR(), ROW({BIGINT()}))), excludedVariant, false);
  testNotEqualComplex<TypeKind::ARRAY>(
      ARRAY(MAP(VARCHAR(), ROW({BIGINT()}))), excludedVariant, true);
}

TEST_F(ConstrainedGeneratorsTest, setPrimitive) {
  std::unordered_set<int32_t> integers{{1, 2, 3}};
  testSetPrimitive<TypeKind::INTEGER>(INTEGER(), integers);

  std::unordered_set<std::string> strings{{"1", "2", "3"}};
  testSetPrimitive<TypeKind::VARCHAR>(VARCHAR(), strings);
}

TEST_F(ConstrainedGeneratorsTest, setComplex) {
  std::vector<variant> variants{
      variant::array({variant::map(
          {{variant{"1"}, variant::row({variant{static_cast<int32_t>(1)}})}})}),
      variant::array({variant::map(
          {{variant{"2"},
            variant::row({variant{static_cast<int32_t>(2)}})}})})};
  testSetComplex<TypeKind::ARRAY>(
      ARRAY(MAP(VARCHAR(), ROW({BIGINT()}))), variants);
}

TEST_F(ConstrainedGeneratorsTest, json) {
  const TypePtr type = ARRAY(MAP(DOUBLE(), ROW({BIGINT()})));
  std::unique_ptr<JsonInputGenerator> generator =
      std::make_unique<JsonInputGenerator>(
          0,
          JSON(),
          0.4,
          std::make_unique<RandomInputGenerator<ArrayType>>(0, type, 0.4));

  const uint32_t kIterations = 1000;
  const auto& opts = generator->serializationOptions();
  for (uint32_t i = 0; i < kIterations; ++i) {
    auto value = generator->generate();
    EXPECT_EQ(value.kind(), TypeKind::VARCHAR);
    if (value.hasValue()) {
      EXPECT_TRUE(value.hasValue());
      folly::dynamic json;
      auto jsonString = value.value<TypeKind::VARCHAR>();
      EXPECT_NO_THROW(
          json = folly::parseJson(value.value<TypeKind::VARCHAR>(), opts));
      EXPECT_TRUE(json.isNull() || json.isArray());
    }
  }
}

TEST_F(ConstrainedGeneratorsTest, jsonPath) {
  const std::vector<variant> mapKeys{"k1", "k2"};
  const size_t kMaxContainerSize = 3;
  TypePtr type = ARRAY(MAP(VARCHAR(), ROW({BIGINT(), MAP(VARCHAR(), REAL())})));
  auto jsonGenerator = std::make_unique<JsonInputGenerator>(
      0,
      JSON(),
      0.2,
      getRandomInputGenerator(0, type, 0.2, mapKeys, kMaxContainerSize));
  auto jsonPathGenerator = std::make_unique<JsonPathGenerator>(
      0, VARCHAR(), 0.2, type, mapKeys, kMaxContainerSize);

  const uint32_t kIterations = 1000;
  bool hasNull = false;
  for (uint32_t i = 0; i < kIterations; ++i) {
    const auto json = jsonGenerator->generate();
    const auto jsonPath = jsonPathGenerator->generate();
    if (jsonPath.hasValue()) {
      if (json.hasValue()) {
        EXPECT_NO_THROW(functions::jsonExtract(
            json.value<TypeKind::VARCHAR>(),
            jsonPath.value<TypeKind::VARCHAR>()));
      }
    } else {
      hasNull = true;
    }
  }
  EXPECT_TRUE(hasNull);
}

} // namespace facebook::velox::fuzzer::test

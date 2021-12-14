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

#include <optional>
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/common/base/Exceptions.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace {
using namespace facebook::velox;

DecodedVector* decode(DecodedVector& decoder, const BaseVector& vector) {
  SelectivityVector rows(vector.size());
  decoder.decode(vector, rows);
  return &decoder;
}

class MapViewTest : public functions::test::FunctionBaseTest {
 protected:
  using map_type = std::vector<std::pair<int64_t, std::optional<int64_t>>>;
  map_type map1 = {};
  map_type map2 = {{1, 4}, {3, 3}, {4, std::nullopt}};
  map_type map3 = {
      {10, 10},
      {4, std::nullopt},
      {1, 4},
      {10, 4},
      {10, std::nullopt},
  };

  std::vector<map_type> mapsData = {map1, map2, map3};

  MapVectorPtr createTestMapVector() {
    return makeMapVector<int64_t, int64_t>(mapsData);
  }
};

TEST_F(MapViewTest, testReadingRangeLoop) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  for (auto i = 0; i < mapsData.size(); i++) {
    auto mapView = reader[i];
    auto it = mapsData[i].begin();
    int count = 0;
    ASSERT_EQ(mapsData[i].size(), mapView.size());
    for (const auto& entry : mapView) {
      ASSERT_EQ(entry.first, it->first);
      ASSERT_EQ(entry.second.has_value(), it->second.has_value());
      if (it->second.has_value()) {
        ASSERT_EQ(entry.second.value(), it->second.value());
      }
      ASSERT_EQ(entry.second, it->second);
      it++;
      count++;
    }
    ASSERT_EQ(count, mapsData[i].size());
  }
}

TEST_F(MapViewTest, testReadingIteratorLoop) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  for (auto i = 0; i < mapsData.size(); ++i) {
    auto mapView = reader[i];
    auto it = mapsData[i].begin();
    int count = 0;
    ASSERT_EQ(mapsData[i].size(), mapView.size());
    for (auto itView = mapView.begin(); itView != mapView.end(); ++itView) {
      ASSERT_EQ(itView->first, it->first);
      ASSERT_EQ(itView->second.has_value(), it->second.has_value());
      if (it->second.has_value()) {
        ASSERT_EQ(itView->second.value(), it->second.value());
      }
      ASSERT_EQ(itView->second, it->second);
      it++;
      count++;
    }
    ASSERT_EQ(count, mapsData[i].size());
  }
}

// MapView can be seen as std::vector<pair<key, value>>.
TEST_F(MapViewTest, testIndexedLoop) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  for (auto i = 0; i < mapsData.size(); ++i) {
    auto mapView = reader[i];
    auto it = mapsData[i].begin();
    int count = 0;
    ASSERT_EQ(mapsData[i].size(), mapView.size());
    for (int j = 0; j < mapView.size(); j++) {
      ASSERT_EQ(mapView[j].first, it->first);
      ASSERT_EQ(mapView[j].second.has_value(), it->second.has_value());
      if (it->second.has_value()) {
        ASSERT_EQ(mapView[j].second.value(), it->second.value());
      }
      ASSERT_EQ(mapView[j].second, it->second);
      it++;
      count++;
    }
    ASSERT_EQ(count, mapsData[i].size());
  }
}

TEST_F(MapViewTest, encoded) {
  VectorPtr mapVector = createTestMapVector();
  // Wrap in dictionary.
  auto vectorSize = mapVector->size();
  BufferPtr indices =
      AlignedBuffer::allocate<vector_size_t>(vectorSize, pool_.get());
  auto rawIndices = indices->asMutable<vector_size_t>();
  // Assign indices such that array is reversed.
  for (size_t i = 0; i < vectorSize; ++i) {
    rawIndices[i] = vectorSize - 1 - i;
  }
  mapVector = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, vectorSize, mapVector);

  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(decode(decoded, *mapVector));

  ASSERT_EQ(reader[0].size(), 5);
  ASSERT_EQ(reader[1].size(), 3);
  ASSERT_EQ(reader[2].size(), 0);
}

TEST_F(MapViewTest, testCompareLazyValueAccess) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  // Compare LazyValueAccess with constant.
  ASSERT_EQ(reader[1][0].first, 1);
  ASSERT_NE(reader[1][0].first, 10);
  ASSERT_EQ(1, reader[1][0].first);
  ASSERT_NE(10, reader[1][0].first);

  // Compare LazyValueAccess with LazyValueAccess.
  ASSERT_EQ(reader[2][2].first, reader[1][0].first);
  ASSERT_NE(reader[2][2].first, reader[1][1].first);

  // Compare LazyValueAccess with VectorOptionalValueAccessor value.
  ASSERT_EQ(reader[2][1].first, reader[1][0].second.value());
  ASSERT_NE(reader[2][2].first, reader[1][1].second.value());
  ASSERT_EQ(reader[1][0].second.value(), reader[2][1].first);
  ASSERT_NE(reader[1][1].second.value(), reader[2][2].first);

  // Compare null VectorOptionalValueAccessor with LazyValueAccess.
  ASSERT_NE(reader[1][1].second.value(), reader[1][2].first);
}

TEST_F(MapViewTest, testCompareVectorOptionalValueAccessor) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  // Compare VectorOptionalValueAccessor with std::optional.
  ASSERT_EQ(reader[2][2].second, std::optional(4));
  ASSERT_EQ(reader[2][2].second, std::optional(4l));
  ASSERT_EQ(reader[2][2].second, std::optional(4ll));
  ASSERT_EQ(reader[2][2].second, std::optional(4.0F));

  ASSERT_NE(reader[2][2].second, std::optional(4.01F));
  ASSERT_NE(reader[2][2].second, std::optional(8));

  ASSERT_EQ(std::optional(4), reader[2][2].second);
  ASSERT_EQ(std::optional(4l), reader[2][2].second);
  ASSERT_EQ(std::optional(4ll), reader[2][2].second);

  ASSERT_NE(std::optional(4.01F), reader[2][2].second);

  ASSERT_EQ(std::nullopt, reader[1][2].second);
  ASSERT_NE(std::nullopt, reader[1][1].second);

  std::optional<int64_t> nullOpt;
  ASSERT_EQ(reader[1][2].second, std::nullopt);
  ASSERT_NE(reader[1][1].second, std::nullopt);

  ASSERT_EQ(reader[1][2].second, nullOpt);
  ASSERT_NE(reader[1][1].second, nullOpt);

  // Compare VectorOptionalValueAccessor<T> with T::exec_t.
  ASSERT_EQ(reader[2][2].second, 4);
  ASSERT_EQ(reader[2][2].second, 4l);
  ASSERT_EQ(reader[2][2].second, 4ll);
  ASSERT_EQ(reader[2][2].second, 4.0F);

  ASSERT_NE(reader[2][2].second, 4.01F);
  ASSERT_NE(reader[2][2].second, 8);

  ASSERT_EQ(4, reader[2][2].second);
  ASSERT_EQ(4l, reader[2][2].second);
  ASSERT_EQ(4ll, reader[2][2].second);
  ASSERT_NE(4.01F, reader[2][2].second);

  // VectorOptionalValueAccessor is null here.
  ASSERT_NE(4.01F, reader[1][2].second);
  ASSERT_NE(reader[1][2].second, 4);

  // Compare VectorOptionalValueAccessor with VectorOptionalValueAccessor.
  ASSERT_EQ(reader[2][2].second, reader[2][3].second);
  ASSERT_NE(reader[2][2].second, reader[2][0].second);

  // Compare with empty VectorOptionalValueAccessor.
  // One null and one not null.
  ASSERT_NE(reader[1][1].second, reader[1][2].second);
  ASSERT_NE(reader[1][2].second, reader[1][1].second);
  // Both are null.
  ASSERT_EQ(reader[2][1].second, reader[1][2].second);
}

TEST_F(MapViewTest, testCompareMapViewElement) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  // Compare VectorOptionalValueAccessor with constant.
  ASSERT_NE(reader[2][2], reader[2][1]);
  ASSERT_EQ(reader[1][0], reader[2][2]);
}

TEST_F(MapViewTest, testAssignToOptional) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  std::optional<int64_t> element = reader[2][2].second;
  std::optional<int64_t> element2 = reader[2][1].second;
  ASSERT_EQ(element, reader[2][2].second);
  ASSERT_EQ(element2, reader[2][1].second);
  ASSERT_NE(element2, element);
}

TEST_F(MapViewTest, testFind) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  ASSERT_EQ(reader[1].find(5), reader[1].end());
  ASSERT_NE(reader[1].find(4), reader[1].end());
  ASSERT_EQ(reader[1].find(4)->first, 4);
  ASSERT_EQ(reader[1].find(4)->second, std::nullopt);
}

TEST_F(MapViewTest, testAt) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  ASSERT_THROW(reader[1].at(5), VeloxException);
  ASSERT_EQ(reader[1].at(4), std::nullopt);
  ASSERT_EQ(reader[1].at(3), 3);
}

TEST_F(MapViewTest, testValueOr) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  ASSERT_EQ(reader[1].at(4).value_or(10), 10);
  ASSERT_EQ(reader[1].at(3).value_or(10), 3);
}

// Function that takes a map from array of doubles to integer as input.
template <typename T>
struct MapComplexKeyF {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      double& out,
      const arg_type<Map<Array<double>, double>>& input) {
    out = 0;
    for (const auto& entry : input) {
      for (auto v : entry.first) {
        out += v.value();
      }
    }

    double outTest = 0;
    // Test operator-> on MapView::Iterator.
    for (auto it = input.begin(); it != input.end(); it++) {
      auto keyArray = it->first;
      for (auto v : keyArray) {
        outTest += v.value();
      }
    }

    EXPECT_EQ(out, outTest);
    return true;
  }
};

TEST_F(MapViewTest, mapCoplexKey) {
  registerFunction<MapComplexKeyF, double, Map<Array<double>, double>>(
      {"func"});

  const vector_size_t size = 10;
  auto values1 = makeArrayVector<double>(
      size,
      [](auto /*row*/) { return 10; },
      [](auto row, auto /*index*/) { return row; });

  auto values2 = makeArrayVector<double>(
      size,
      [](auto /*row*/) { return 1; },
      [](auto /*row*/, auto index) { return 1.2 * index; });

  auto result = evaluate<FlatVector<double>>(
      "func(map(array_constructor(c0), c1))",
      makeRowVector({values1, values2}));

  auto expected =
      makeFlatVector<double>(size, [](auto row) { return row * 10; });

  ASSERT_EQ(size, result->size());
  for (auto i = 0; i < size; i++) {
    EXPECT_NEAR(expected->valueAt(i), result->valueAt(i), 0.0000001);
  }
}

TEST_F(MapViewTest, testReadingStructureBindingLoop) {
  auto mapVector = createTestMapVector();
  DecodedVector decoded;
  exec::VectorReader<Map<int64_t, int64_t>> reader(
      decode(decoded, *mapVector.get()));

  for (auto i = 0; i < mapsData.size(); i++) {
    auto mapView = reader[i];
    auto it = mapsData[i].begin();
    int count = 0;
    ASSERT_EQ(mapsData[i].size(), mapView.size());
    for (const auto& [key, value] : mapView) {
      ASSERT_EQ(key, it->first);
      ASSERT_EQ(value.has_value(), it->second.has_value());
      if (it->second.has_value()) {
        ASSERT_EQ(value.value(), it->second.value());
      }
      ASSERT_EQ(value, it->second);
      it++;
      count++;
    }
    ASSERT_EQ(count, mapsData[i].size());
  }
}

} // namespace

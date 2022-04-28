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

#include "velox/functions/prestosql/aggregates/PrestoHasher.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/tz/TimeZoneMap.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::aggregate::test {
template <typename T>
using limits = std::numeric_limits<T>;

class PrestoHasherTest : public testing::Test,
                         public facebook::velox::test::VectorTestBase {
 protected:
  template <typename T>
  void assertHash(
      const std::vector<std::optional<T>>& data,
      const std::vector<int64_t>& expected) {
    auto vector = makeNullableFlatVector<T>(data);
    assertHash(vector, expected);
  }

  SelectivityVector selectEvenRows(vector_size_t size) {
    SelectivityVector evenRows(size, false);
    for (int i = 0; i < size; i += 2) {
      evenRows.setValid(i, true);
    }
    evenRows.updateBounds();
    return evenRows;
  }

  void assertHash(
      const VectorPtr& vector,
      const std::vector<int64_t>& expected) {
    auto vectorSize = vector->size();
    PrestoHasher hasher(vector->type());

    auto checkHashes = [&](const VectorPtr& vector,
                           const std::vector<int64_t>& expected) {
      auto size = vector->size();
      SelectivityVector rows(size);

      BufferPtr hashes = AlignedBuffer::allocate<int64_t>(size, pool_.get());
      hasher.hash(vector, rows, hashes);

      auto rawHashes = hashes->as<int64_t>();
      for (int i = 0; i < expected.size(); i++) {
        EXPECT_EQ(expected[i], rawHashes[i]) << "at " << i;
      }

      // Check for a subset of rows.
      auto evenRows = selectEvenRows(size);
      hasher.hash(vector, evenRows, hashes);
      evenRows.applyToSelected([&](auto row) {
        EXPECT_EQ(expected[row], rawHashes[row]) << "at " << row;
      });
    };

    checkHashes(vector, expected);

    // Wrap in Dictionary
    BufferPtr indices = makeIndices(vectorSize, [](auto row) { return row; });
    auto dictionaryVector = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, vectorSize, vector);

    checkHashes(dictionaryVector, expected);

    // Repeating rows.
    std::vector<int64_t> modifiedExpected;
    modifiedExpected.reserve(vectorSize);
    auto rawIndices = indices->asMutable<vector_size_t>();

    for (size_t i = 0; i < vectorSize; ++i) {
      rawIndices[i] = i / 2;
      modifiedExpected[i] = expected[i / 2];
    }

    dictionaryVector = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, vectorSize, vector);

    checkHashes(dictionaryVector, modifiedExpected);

    // Subset of rows.
    auto subsetSize = vectorSize / 2;
    for (size_t i = 0; i < subsetSize; ++i) {
      rawIndices[i] = i * 2;
      modifiedExpected[i] = expected[i * 2];
    }

    dictionaryVector = wrapInDictionary(indices, subsetSize, vector);

    checkHashes(dictionaryVector, modifiedExpected);

    // Wrap in a constant vector of various sizes.

    for (int size = 1; size <= vectorSize - 1; size++) {
      for (int i = 0; i < vectorSize - size; i++) {
        auto constantVector = BaseVector::wrapInConstant(size, i, vector);
        std::vector<int64_t> constantExpected(size, expected[i]);
        checkHashes(constantVector, constantExpected);
      }
    }
  }

  template <typename T>
  void testIntegral() {
    assertHash<T>(
        {1, 0, 100, std::nullopt},
        {9155312661752487122, 0, -367469560765523433, 0});
  }
};

TEST_F(PrestoHasherTest, tinyints) {
  testIntegral<int8_t>();
  assertHash<int8_t>({99, std::nullopt}, {4525604468207172933, 0});
  assertHash<int8_t>(
      {limits<int8_t>::max(), limits<int8_t>::min()},
      {695722463566662829, 6845023471056522234});
}

TEST_F(PrestoHasherTest, smallints) {
  testIntegral<int16_t>();
  assertHash<int16_t>({9999, std::nullopt}, {995971853744766661, 0});
  assertHash<int16_t>(
      {limits<int16_t>::max(), limits<int16_t>::min()},
      {670463525973519066, -4530432376425028794});
}

TEST_F(PrestoHasherTest, ints) {
  testIntegral<int32_t>();
  assertHash<int32_t>({99999, std::nullopt}, {-6111805494244633133, 0});
  assertHash<int32_t>(
      {limits<int32_t>::max(), limits<int32_t>::min()},
      {-7833837855883860365, -3072160283202506188});
}

TEST_F(PrestoHasherTest, bigints) {
  testIntegral<int64_t>();
  assertHash<int64_t>({99999, std::nullopt}, {-6111805494244633133, 0});
  assertHash<int64_t>(
      {limits<int64_t>::max(), limits<int64_t>::min()},
      {516552589260593319, 7024193345362591744});
}

TEST_F(PrestoHasherTest, timestamp) {
  assertHash<Timestamp>(
      {Timestamp(1, 100),
       Timestamp(10, 10),
       Timestamp(100, 1000),
       Timestamp(0, 0),
       std::nullopt},
      {2343331593029422743, -3897102175227929705, 3043507167507853989, 0, 0});
}

TEST_F(PrestoHasherTest, date) {
  assertHash<Date>(
      {Date(0), Date(1000), std::nullopt}, {0, 2343331593029422743, 0});
}

TEST_F(PrestoHasherTest, doubles) {
  assertHash<double>(
      {1.0,
       0.0,
       std::nan("0"),
       99999.99,
       std::nullopt,
       limits<double>::max(),
       limits<double>::lowest()},
      {2156309669339463680L,
       0,
       -6389168865844396032,
       9140591727513491554,
       0,
       -7863427759443830617,
       -839234414081238873});
}

TEST_F(PrestoHasherTest, floats) {
  assertHash<float>(
      {1.0f,
       0.0f,
       std::nanf("0"),
       99999.99f,
       std::nullopt,
       limits<float>::max(),
       limits<float>::lowest()},
      {-6641611864725600567L,
       0,
       6018425597037803642,
       -6781949276808836923,
       0,
       -4588979133863754154,
       -3262782034081892214});
}

TEST_F(PrestoHasherTest, varchars) {
  assertHash<StringView>(
      {"abcd"_sv, ""_sv, std::nullopt, "Thanks \u0020\u007F"_sv},
      {-2449070131962342708, -1205034819632174695, 0, 2911531567394159200});
}

TEST_F(PrestoHasherTest, bools) {
  assertHash<bool>({true, false, std::nullopt}, {1231, 1237, 0});
}

TEST_F(PrestoHasherTest, arrays) {
  auto baseArrayVector = vectorMaker_.arrayVectorNullable<int64_t>(
      {{{1, 2}},
       {{3, 4}},
       {{4, 5}},
       {{6, 7}},
       {{8, 9}},
       {{10, 11}},
       {{12, std::nullopt}},
       std::nullopt,
       {{}}});

  assertHash(
      baseArrayVector,
      {4329740752828760473,
       655643799837771513,
       8633635089947142034,
       9138382565482297209,
       1065928229506940121,
       8616704993676952121,
       1942639070761256766,
       0,
       0});

  // Array of arrays.
  auto arrayOfArrayVector =
      vectorMaker_.arrayVector({0, 2, 4}, baseArrayVector);

  assertHash(
      arrayOfArrayVector,
      {5750398621562484864, 79909248200426023, -4270118586511114434});

  // Array with nulls
  auto arrayWithNulls = vectorMaker_.arrayVectorNullable<int64_t>({
      {{std::nullopt}},
      {{1, 2, 3}},
      {{1024, std::nullopt, -99, -999}},
      {{}},
      {{std::nullopt, -1}},
  });

  assertHash(
      arrayWithNulls,
      {0, -2582109863103049084, 2242047241851842487, 0, -6507640756101998425});
}

TEST_F(PrestoHasherTest, maps) {
  auto mapVectorPair = makeMapVector<int64_t, double>(
      {{{1, 17.0}, {2, 36.0}, {3, 8.0}, {4, 28.0}, {5, 24.0}, {6, 32.0}}});

  assertHash(mapVectorPair, {-8280251289038521351});

  auto mapVector = makeMapVector<int64_t, int64_t>(
      3,
      [](auto /**/) { return 1; },
      [](auto row) { return row; },
      [](auto row) { return row + 1; });

  assertHash(
      mapVector,
      {9155312661752487122, -6461599496541202183, 5488675304642487510});

  auto mapOfArrays = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, {{1, 2, 3}}}}, {{2, {{4, 5, 6}}}}, {{3, {{7, 8, 9}}}}});
  assertHash(
      mapOfArrays,
      {-6691024575166067114, -7912800814947937532, -5636922976001735986});

  // map with nulls
  auto mapWithNullArrays = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, std::nullopt}},
       {{2, {{4, 5, std::nullopt}}}},
       {{std::nullopt, {{7, 8, 9}}}}});
  assertHash(
      mapWithNullArrays,
      {9155312661752487122, 6562918552317873797, 2644717257979355699});
}

TEST_F(PrestoHasherTest, rows) {
  auto row = makeRowVector(
      {makeFlatVector<int64_t>({1, 3}), makeFlatVector<int64_t>({2, 4})});

  assertHash(row, {4329740752828761434, 655643799837772474});

  row = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, std::nullopt}),
       makeNullableFlatVector<int64_t>({std::nullopt, 4})});

  assertHash(row, {7113531408683827503, -1169223928725763049});
}

TEST_F(PrestoHasherTest, wrongVectorType) {
  PrestoHasher hasher(ARRAY(BIGINT()));
  auto vector = makeNullableFlatVector<StringView>({"abc"_sv});
  SelectivityVector rows(1);
  BufferPtr hashes = AlignedBuffer::allocate<int64_t>(1, pool_.get());
  ASSERT_ANY_THROW(hasher.hash(vector, rows, hashes));
}

TEST_F(PrestoHasherTest, timestampWithTimezone) {
  const auto toUnixtimeWithTimeZone =
      [&](const std::vector<std::optional<std::pair<int64_t, std::string>>>&
              timestampWithTimeZones) {
        std::vector<std::optional<int64_t>> timestamps;
        std::vector<std::optional<int16_t>> timeZoneIds;
        auto size = timestampWithTimeZones.size();
        BufferPtr nulls =
            AlignedBuffer::allocate<uint64_t>(bits::nwords(size), pool());
        auto rawNulls = nulls->asMutable<uint64_t>();
        timestamps.reserve(size);
        timeZoneIds.reserve(size);

        for (auto i = 0; i < size; i++) {
          auto timestampWithTimeZone = timestampWithTimeZones[i];
          if (timestampWithTimeZone.has_value()) {
            auto timestamp = timestampWithTimeZone.value().first;
            auto tz = timestampWithTimeZone.value().second;
            const int16_t tzid = util::getTimeZoneID(tz);
            timeZoneIds.push_back(tzid);
            timestamps.push_back(timestamp);
            bits::clearNull(rawNulls, i);
          } else {
            timeZoneIds.push_back(std::nullopt);
            timestamps.push_back(std::nullopt);
            bits::setNull(rawNulls, i);
          }
        }

        return std::make_shared<RowVector>(
            pool(),
            TIMESTAMP_WITH_TIME_ZONE(),
            nulls,
            size,
            std::vector<VectorPtr>{
                makeNullableFlatVector(timestamps),
                makeNullableFlatVector(timeZoneIds)});
      };

  auto timestampWithTimeZones = toUnixtimeWithTimeZone(
      {std::optional(std::pair{1639426440000, "+01:00"}),
       std::optional(std::pair{1639426440000, "+03:00"}),
       std::optional(std::pair{1549770072000, "-14:00"}),
       std::nullopt});
  assertHash(
      timestampWithTimeZones,
      {-3461822077982309083, -3461822077982309083, -8497890125589769483, 0});
}

} // namespace facebook::velox::aggregate::test

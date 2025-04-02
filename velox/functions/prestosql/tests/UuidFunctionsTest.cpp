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

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::prestosql {

namespace {

class UuidFunctionsTest : public functions::test::FunctionBaseTest {};

TEST_F(UuidFunctionsTest, uuid) {
  auto result =
      evaluate<FlatVector<int128_t>>("uuid()", makeRowVector(ROW({}), 10));

  // Sanity check results. All values are unique.
  std::unordered_set<int128_t> uuids;
  for (auto i = 0; i < 10; ++i) {
    const auto uuid = result->valueAt(i);
    ASSERT_TRUE(uuids.insert(uuid).second);
  }
  ASSERT_EQ(10, uuids.size());
}

TEST_F(UuidFunctionsTest, typeof) {
  auto result = evaluate("typeof(uuid())", makeRowVector(ROW({}), 10));

  auto expected = makeConstant("uuid", 10);
  velox::test::assertEqualVectors(expected, result);

  result = evaluate(
      "typeof(array_constructor(uuid(), uuid()))", makeRowVector(ROW({}), 10));

  expected = makeConstant("array(uuid)", 10);
  velox::test::assertEqualVectors(expected, result);
}

TEST_F(UuidFunctionsTest, castAsVarchar) {
  const vector_size_t size = 1'000;
  auto uuids =
      evaluate<FlatVector<int128_t>>("uuid()", makeRowVector(ROW({}), size));

  auto result = evaluate<FlatVector<StringView>>(
      "cast(c0 as varchar)", makeRowVector({uuids}));

  // Verify that CAST results as the same as boost::lexical_cast. We do not use
  // boost::lexical_cast to implement CAST because it is too slow.
  auto expected = makeFlatVector<std::string>(size, [&](auto row) {
    const auto uuid = DecimalUtil::bigEndian(uuids->valueAt(row));

    boost::uuids::uuid u;
    memcpy(&u, &uuid, 16);

    return boost::lexical_cast<std::string>(u);
  });

  velox::test::assertEqualVectors(expected, result);

  // Sanity check results. All strings are unique. Each string is 36 bytes
  // long.
  std::unordered_set<std::string> uniqueUuids;
  for (auto i = 0; i < size; ++i) {
    const auto uuid = result->valueAt(i).str();
    ASSERT_EQ(36, uuid.size());
    ASSERT_TRUE(uniqueUuids.insert(uuid).second);
  }
  ASSERT_EQ(size, uniqueUuids.size());
}

TEST_F(UuidFunctionsTest, castAsVarbinary) {
  const vector_size_t size = 1'000;
  auto uuids =
      evaluate<FlatVector<int128_t>>("uuid()", makeRowVector(ROW({}), size));

  auto result = evaluate<FlatVector<StringView>>(
      "cast(c0 as varbinary)", makeRowVector({uuids}));

  auto expected = makeFlatVector<std::string>(
      size,
      [&](auto row) {
        const auto uuid = DecimalUtil::bigEndian(uuids->valueAt(row));

        boost::uuids::uuid u;
        memcpy(&u, &uuid, 16);

        return std::string(u.begin(), u.end());
      },
      nullptr,
      VARBINARY());

  velox::test::assertEqualVectors(expected, result);

  // Sanity check results. All binaries are unique. Each binary is 16 bytes
  // long.
  std::unordered_set<std::string> uniqueUuids;
  for (auto i = 0; i < size; ++i) {
    const auto uuid = result->valueAt(i);
    ASSERT_EQ(16, uuid.size());
    ASSERT_TRUE(
        uniqueUuids.insert(std::string(uuid.data(), uuid.size())).second);
  }
  ASSERT_EQ(size, uniqueUuids.size());
}

TEST_F(UuidFunctionsTest, varcharCastRoundTrip) {
  auto strings = makeFlatVector<std::string>({
      "33355449-2c7d-43d7-967a-f53cd23215ad",
      "eed9f812-4b0c-472f-8a10-4ae7bff79a47",
      "f768f36d-4f09-4da7-a298-3564d8f3c986",
  });

  auto uuids = evaluate("cast(c0 as uuid)", makeRowVector({strings}));
  auto stringsCopy = evaluate("cast(c0 as varchar)", makeRowVector({uuids}));
  auto uuidsCopy = evaluate("cast(c0 as uuid)", makeRowVector({stringsCopy}));

  velox::test::assertEqualVectors(strings, stringsCopy);
  velox::test::assertEqualVectors(uuids, uuidsCopy);
}

TEST_F(UuidFunctionsTest, varbinaryCastRoundTrip) {
  auto binaries = makeFlatVector<std::string>(
      {
          folly::unhexlify("333554492c7d43d7967af53cd23215ad"),
          folly::unhexlify("eed9f8124b0c472f8a104ae7bff79a47"),
          folly::unhexlify("f768f36d4f094da7a2983564d8f3c986"),
      },
      VARBINARY());

  auto uuids = evaluate("cast(c0 as uuid)", makeRowVector({binaries}));
  auto binariesCopy = evaluate("cast(c0 as varbinary)", makeRowVector({uuids}));
  auto uuidsCopy = evaluate("cast(c0 as uuid)", makeRowVector({binariesCopy}));

  velox::test::assertEqualVectors(binaries, binariesCopy);
  velox::test::assertEqualVectors(uuids, uuidsCopy);
}

TEST_F(UuidFunctionsTest, unsupportedCast) {
  auto input = makeRowVector(ROW({}), 10);
  VELOX_ASSERT_THROW(
      evaluate("cast(uuid() as integer)", input),
      "Cannot cast UUID to INTEGER");
  VELOX_ASSERT_THROW(
      evaluate("cast(123 as uuid())", input), "Cannot cast BIGINT to UUID.");
}

TEST_F(UuidFunctionsTest, comparisons) {
  const auto uuidEval = [&](const std::optional<std::string>& lhs,
                            const std::string& operation,
                            const std::optional<std::string>& rhs) {
    return evaluateOnce<bool>(
        fmt::format("cast(c0 as uuid) {} cast(c1 as uuid)", operation),
        lhs,
        rhs);
  };

  ASSERT_EQ(
      true,
      uuidEval(
          "33355449-2c7d-43d7-967a-f53cd23215ad",
          "<",
          "ffffffff-ffff-ffff-ffff-ffffffffffff"));
  ASSERT_EQ(
      false,
      uuidEval(
          "33355449-2c7d-43d7-967a-f53cd23215ad",
          "<",
          "00000000-0000-0000-0000-000000000000"));
  ASSERT_EQ(
      true,
      uuidEval(
          "f768f36d-4f09-4da7-a298-3564d8f3c986",
          ">",
          "00000000-0000-0000-0000-000000000000"));
  ASSERT_EQ(
      false,
      uuidEval(
          "f768f36d-4f09-4da7-a298-3564d8f3c986",
          ">",
          "ffffffff-ffff-ffff-ffff-ffffffffffff"));

  ASSERT_EQ(
      true,
      uuidEval(
          "33355449-2c7d-43d7-967a-f53cd23215ad",
          "<=",
          "33355449-2c7d-43d7-967a-f53cd23215ad"));
  ASSERT_EQ(
      true,
      uuidEval(
          "33355449-2c7d-43d7-967a-f53cd23215ad",
          "<=",
          "ffffffff-ffff-ffff-ffff-ffffffffffff"));
  ASSERT_EQ(
      true,
      uuidEval(
          "33355449-2c7d-43d7-967a-f53cd23215ad",
          ">=",
          "33355449-2c7d-43d7-967a-f53cd23215ad"));
  ASSERT_EQ(
      true,
      uuidEval(
          "ffffffff-ffff-ffff-ffff-ffffffffffff",
          ">=",
          "33355449-2c7d-43d7-967a-f53cd23215ad"));

  ASSERT_EQ(
      true,
      uuidEval(
          "f768f36d-4f09-4da7-a298-3564d8f3c986",
          "==",
          "f768f36d-4f09-4da7-a298-3564d8f3c986"));
  ASSERT_EQ(
      true,
      uuidEval(
          "eed9f812-4b0c-472f-8a10-4ae7bff79a47",
          "!=",
          "f768f36d-4f09-4da7-a298-3564d8f3c986"));

  ASSERT_EQ(
      true,
      uuidEval(
          "11000000-0000-0022-0000-000000000000",
          "<",
          "22000000-0000-0011-0000-000000000000"));
  ASSERT_EQ(
      true,
      uuidEval(
          "00000000-0000-0000-2200-000000000011",
          ">",
          "00000000-0000-0000-1100-000000000022"));
  ASSERT_EQ(
      false,
      uuidEval(
          "00000000-0000-0000-0000-000000000011",
          ">",
          "22000000-0000-0000-0000-000000000000"));
  ASSERT_EQ(
      false,
      uuidEval(
          "11000000-0000-0000-0000-000000000000",
          "<",
          "00000000-0000-0000-0000-000000000022"));

  std::string lhs = "12342345-3456-4567-5678-678978908901";
  std::string rhs = "23451234-4567-3456-6789-567889017890";
  ASSERT_EQ(true, uuidEval(lhs, "<", rhs));

  for (vector_size_t i = 0; i < lhs.size(); i++) {
    if (lhs[i] == '-') {
      continue;
    }
    lhs[i] = '0';
    rhs[i] = '0';
    bool expected = boost::lexical_cast<boost::uuids::uuid>(lhs) <
        boost::lexical_cast<boost::uuids::uuid>(rhs);
    ASSERT_EQ(expected, uuidEval(lhs, "<", rhs));
  }
}

TEST_F(UuidFunctionsTest, between) {
  const auto uuidEval = [&](const std::optional<std::string>& value,
                            const std::optional<std::string>& low,
                            const std::optional<std::string>& high) {
    return evaluateOnce<bool>(
        "cast(c0 as uuid) between cast(c1 as uuid) and cast(c2 as uuid)",
        value,
        low,
        high);
  };

  ASSERT_EQ(
      true,
      uuidEval(
          "33355449-2c7d-43d7-967a-f53cd23215ad",
          "00000000-0000-0000-0000-000000000000",
          "ffffffff-ffff-ffff-ffff-ffffffffffff"));
  ASSERT_EQ(
      false,
      uuidEval(
          "33355449-2c7d-43d7-967a-f53cd23215ad",
          "ffffffff-ffff-ffff-ffff-ffffffffffff",
          "00000000-0000-0000-0000-000000000000"));
  ASSERT_EQ(
      true,
      uuidEval(
          "00000000-0000-0000-2200-000000000011",
          "00000000-0000-0000-0000-000000000022",
          "11000000-0000-0000-0000-000000000000"));
  ASSERT_EQ(
      false,
      uuidEval(
          "00000000-0000-0000-0000-000000000022",
          "00000000-0000-0000-2200-000000000011",
          "11000000-0000-0000-0000-000000000000"));
  ASSERT_EQ(
      false,
      uuidEval(
          "11000000-0000-0000-0000-000000000000",
          "00000000-0000-0000-2200-000000000011",
          "00000000-0000-0000-0000-000000000022"));
  ASSERT_EQ(
      true,
      uuidEval(
          "00000000-0000-0000-2200-000000000011",
          "00000000-0000-0000-2200-000000000011",
          "11000000-0000-0000-0000-000000000000"));
  ASSERT_EQ(
      true,
      uuidEval(
          "11000000-0000-0000-0000-000000000000",
          "00000000-0000-0000-2200-000000000011",
          "11000000-0000-0000-0000-000000000000"));
}

} // namespace
} // namespace facebook::velox::functions::prestosql

/*
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

#include <cmath>
#include <limits>

#include "presto_cpp/main/connectors/iceberg/functions/IcebergFunctionRegistration.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

namespace facebook::presto::iceberg::functions::test {
namespace {

class ZOrderFunctionTest : public velox::functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    velox::functions::test::FunctionBaseTest::SetUpTestCase();
    registerIcebergNativeFunctions("");
  }

  // Returns zorder(c0) for each row of 'rows' as an uppercase hex string.
  std::vector<std::optional<std::string>> zorderHex(const VectorPtr& rows) {
    const auto result = evaluate("to_hex(zorder(c0))", makeRowVector({rows}));
    // The result is constant encoded when the input is.
    DecodedVector decoded(*result);
    std::vector<std::optional<std::string>> hex;
    for (vector_size_t row = 0; row < result->size(); ++row) {
      hex.push_back(
          decoded.isNullAt(row)
              ? std::nullopt
              : std::optional(decoded.valueAt<StringView>(row).str()));
    }
    return hex;
  }
};

// The expected values are the outputs of the Presto Java Iceberg connector's
// zorder function for the same inputs.
TEST_F(ZOrderFunctionTest, matchesJava) {
  EXPECT_THAT(
      zorderHex(makeRowVector(
          {makeFlatVector<int32_t>({1}), makeFlatVector<int32_t>({2})})),
      testing::ElementsAre("C0000000000000000000000000000006"));
  EXPECT_THAT(
      zorderHex(makeRowVector(
          {makeFlatVector<int64_t>({-1}), makeFlatVector<int64_t>({5})})),
      testing::ElementsAre("6AAAAAAAAAAAAAAAAAAAAAAAAAAAAABB"));
  EXPECT_THAT(
      zorderHex(makeRowVector({
          makeFlatVector<int8_t>({-3}),
          makeFlatVector<int16_t>({300}),
          makeFlatVector<int32_t>({-70'000}),
          makeFlatVector<int64_t>({9'000'000'000}),
      })),
      testing::ElementsAre(
          "5AAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAABBAAAABBBAAA9AAA9BABCA8CACC08"));
  EXPECT_THAT(
      zorderHex(makeRowVector(
          {makeFlatVector<float>({1.5}), makeFlatVector<double>({-2.25})})),
      testing::ElementsAre("8FFFFFD1555555556AAAAA1000000000"));
  EXPECT_THAT(
      zorderHex(makeRowVector({
          makeFlatVector<double>({std::nan("")}),
          makeFlatVector<float>({std::numeric_limits<float>::infinity()}),
      })),
      testing::ElementsAre("FFFFFF8000000000FFFFFE0000000000"));
  EXPECT_THAT(
      zorderHex(makeRowVector(
          {makeFlatVector<bool>({true}), makeFlatVector<bool>({false})})),
      testing::ElementsAre("80020000000000000000000000000000"));
  EXPECT_THAT(
      zorderHex(makeRowVector({
          makeFlatVector<int32_t>({19'737}, DATE()),
          makeFlatVector<int32_t>({-3'593}, DATE()),
      })),
      testing::ElementsAre("95555555555555555555555575A35797"));
  EXPECT_THAT(
      zorderHex(makeRowVector({
          makeFlatVector<std::string>({"abc"}),
          makeFlatVector<std::string>({"\x01\x02"}, VARBINARY()),
      })),
      testing::ElementsAre(
          "2803280C280A00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"));
  EXPECT_THAT(
      zorderHex(makeRowVector({makeFlatVector<int32_t>({42})})),
      testing::ElementsAre("800000000000002A"));
}

TEST_F(ZOrderFunctionTest, nullFields) {
  // A null field encodes to 8 zero bytes, even when it is a string.
  EXPECT_THAT(
      zorderHex(makeRowVector({
          makeNullableFlatVector<int32_t>({std::nullopt}),
          makeFlatVector<std::string>(
              {"a string that is longer than thirty-two bytes"}),
      })),
      testing::ElementsAre(
          "140104001505151015041441145414152074686174206973206C6F6E676572207468616E20746869"));
  EXPECT_THAT(
      zorderHex(makeRowVector({
          makeNullableFlatVector<std::string>({std::nullopt}),
          makeFlatVector<int32_t>({7}),
      })),
      testing::ElementsAre("40000000000000000000000000000015"));
  // An unsupported type fails only on a value, so a null one is encoded.
  EXPECT_THAT(
      zorderHex(makeRowVector({
          makeNullableFlatVector<int64_t>({std::nullopt}, DECIMAL(10, 2)),
          makeFlatVector<int32_t>({1}),
      })),
      testing::ElementsAre("40000000000000000000000000000001"));
}

TEST_F(ZOrderFunctionTest, nullRow) {
  auto rows = makeRowVector({makeFlatVector<int32_t>({1, 2})});
  rows->setNull(1, true);
  EXPECT_THAT(
      zorderHex(rows), testing::ElementsAre("8000000000000001", std::nullopt));
}

TEST_F(ZOrderFunctionTest, timestamp) {
  // A timestamp encodes as its milliseconds since the epoch.
  const auto millis = makeFlatVector<int64_t>({1'705'314'600'123, -1});
  const auto timestamps = makeFlatVector<Timestamp>(
      {Timestamp::fromMillis(1'705'314'600'123), Timestamp::fromMillis(-1)});
  EXPECT_EQ(
      zorderHex(makeRowVector({timestamps})),
      zorderHex(makeRowVector({millis})));
}

TEST_F(ZOrderFunctionTest, encodedInput) {
  // Dictionary and constant encoded fields give the same result as flat ones.
  const auto flat = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<std::string>({"a", "b", "c"})});
  const auto indices = makeIndicesInReverse(3);
  const auto dictionary = makeRowVector({
      wrapInDictionary(indices, makeFlatVector<int32_t>({3, 2, 1})),
      wrapInDictionary(indices, makeFlatVector<std::string>({"c", "b", "a"})),
  });
  EXPECT_EQ(zorderHex(dictionary), zorderHex(flat));
  EXPECT_EQ(
      zorderHex(BaseVector::wrapInConstant(3, 1, flat)),
      std::vector<std::optional<std::string>>(3, zorderHex(flat)[1]));
}

TEST_F(ZOrderFunctionTest, unsupportedType) {
  VELOX_ASSERT_USER_THROW(
      zorderHex(
          makeRowVector({makeFlatVector<int64_t>({150}, DECIMAL(10, 2))})),
      "Cannot use column of type DECIMAL(10, 2) in ZOrdering");
  VELOX_ASSERT_USER_THROW(
      zorderHex(makeRowVector({makeFlatVector<int64_t>({1}, TIME())})),
      "Cannot use column of type TIME in ZOrdering");
}

TEST_F(ZOrderFunctionTest, emptyRow) {
  const auto result = evaluate(
      "to_hex(zorder(c0))", makeRowVector({makeRowVector(ROW({}, {}), 2)}));
  velox::test::assertEqualVectors(
      makeFlatVector<std::string>({"", ""}), result);
}

} // namespace
} // namespace facebook::presto::iceberg::functions::test

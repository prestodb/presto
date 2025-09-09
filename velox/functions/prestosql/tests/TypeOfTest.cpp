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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/BigintEnumType.h"
#include "velox/functions/prestosql/types/BingTileType.h"
#include "velox/functions/prestosql/types/GeometryType.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/P4HyperLogLogType.h"
#include "velox/functions/prestosql/types/QDigestType.h"
#include "velox/functions/prestosql/types/TDigestType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/VarcharEnumType.h"

namespace facebook::velox::functions {
namespace {

class TypeOfTest : public functions::test::FunctionBaseTest {
 public:
  std::string typeOf(const TypePtr& type) {
    auto data = makeRowVector({
        BaseVector::createNullConstant(type, 1, pool()),
    });
    return evaluateOnce<std::string>("typeof(c0)", data).value();
  }
};

TEST_F(TypeOfTest, basic) {
  EXPECT_EQ("boolean", typeOf(BOOLEAN()));

  EXPECT_EQ("tinyint", typeOf(TINYINT()));
  EXPECT_EQ("smallint", typeOf(SMALLINT()));
  EXPECT_EQ("integer", typeOf(INTEGER()));
  EXPECT_EQ("bigint", typeOf(BIGINT()));

  EXPECT_EQ("real", typeOf(REAL()));
  EXPECT_EQ("double", typeOf(DOUBLE()));

  EXPECT_EQ("decimal(5,2)", typeOf(DECIMAL(5, 2)));
  EXPECT_EQ("decimal(25,7)", typeOf(DECIMAL(25, 7)));

  EXPECT_EQ("varchar", typeOf(VARCHAR()));
  EXPECT_EQ("varbinary", typeOf(VARBINARY()));

  EXPECT_EQ("timestamp", typeOf(TIMESTAMP()));
  EXPECT_EQ("date", typeOf(DATE()));

  EXPECT_EQ("unknown", typeOf(UNKNOWN()));

  EXPECT_EQ("array(integer)", typeOf(ARRAY(INTEGER())));
  EXPECT_EQ("map(varchar, array(date))", typeOf(MAP(VARCHAR(), ARRAY(DATE()))));
  EXPECT_EQ(
      "row(integer, varchar, array(date))",
      typeOf(ROW({INTEGER(), VARCHAR(), ARRAY(DATE())})));
  EXPECT_EQ(
      "row(\"a\" integer, \"b\" varchar, \"c\" array(date))",
      typeOf(ROW({"a", "b", "c"}, {INTEGER(), VARCHAR(), ARRAY(DATE())})));

  VELOX_ASSERT_THROW(typeOf(OPAQUE<int>()), "Unsupported type: OPAQUE<int>");
}

TEST_F(TypeOfTest, customTypes) {
  EXPECT_EQ("timestamp with time zone", typeOf(TIMESTAMP_WITH_TIME_ZONE()));
  EXPECT_EQ("bingtile", typeOf(BINGTILE()));
  EXPECT_EQ("geometry", typeOf(GEOMETRY()));

  EXPECT_EQ("json", typeOf(JSON()));

  EXPECT_EQ("HyperLogLog", typeOf(HYPERLOGLOG()));
  EXPECT_EQ("P4HyperLogLog", typeOf(P4HYPERLOGLOG()));

  EXPECT_EQ("tdigest(double)", typeOf(TDIGEST(DOUBLE())));

  EXPECT_EQ("qdigest(bigint)", typeOf(QDIGEST(BIGINT())));
  EXPECT_EQ("qdigest(real)", typeOf(QDIGEST(REAL())));
  EXPECT_EQ("qdigest(double)", typeOf(QDIGEST(DOUBLE())));

  std::unordered_map<std::string, int64_t> enumMap = {
      {"CURIOUS", -2}, {"HAPPY", 0}};
  LongEnumParameter moodInfo("test.enum.mood", enumMap);
  LongEnumParameter otherInfo("someEnumType", enumMap);
  EXPECT_EQ("test.enum.mood", typeOf(BIGINT_ENUM(moodInfo)));
  EXPECT_EQ("someEnumType", typeOf(BIGINT_ENUM(otherInfo)));

  std::unordered_map<std::string, std::string> varcharEnumMap = {
      {"RED", "red"}, {"BLUE", "blue"}};
  VarcharEnumParameter colorInfo("test.enum.color", varcharEnumMap);
  VarcharEnumParameter otherColorInfo("someColorType", varcharEnumMap);
  EXPECT_EQ("test.enum.color", typeOf(VARCHAR_ENUM(colorInfo)));
  EXPECT_EQ("someColorType", typeOf(VARCHAR_ENUM(otherColorInfo)));
}
} // namespace
} // namespace facebook::velox::functions

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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/fuzzer/PrestoSql.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::velox::exec::test {
namespace {

TEST(PrestoSqlTest, toTypeSql) {
  EXPECT_EQ(toTypeSql(BOOLEAN()), "BOOLEAN");
  EXPECT_EQ(toTypeSql(TINYINT()), "TINYINT");
  EXPECT_EQ(toTypeSql(SMALLINT()), "SMALLINT");
  EXPECT_EQ(toTypeSql(INTEGER()), "INTEGER");
  EXPECT_EQ(toTypeSql(BIGINT()), "BIGINT");
  EXPECT_EQ(toTypeSql(REAL()), "REAL");
  EXPECT_EQ(toTypeSql(DOUBLE()), "DOUBLE");
  EXPECT_EQ(toTypeSql(VARCHAR()), "VARCHAR");
  EXPECT_EQ(toTypeSql(VARBINARY()), "VARBINARY");
  EXPECT_EQ(toTypeSql(TIMESTAMP()), "TIMESTAMP");
  EXPECT_EQ(toTypeSql(DATE()), "DATE");
  EXPECT_EQ(toTypeSql(TIMESTAMP_WITH_TIME_ZONE()), "TIMESTAMP WITH TIME ZONE");
  EXPECT_EQ(toTypeSql(ARRAY(BOOLEAN())), "ARRAY(BOOLEAN)");
  EXPECT_EQ(toTypeSql(MAP(BOOLEAN(), INTEGER())), "MAP(BOOLEAN, INTEGER)");
  EXPECT_EQ(
      toTypeSql(ROW({{"a", BOOLEAN()}, {"b", INTEGER()}})),
      "ROW(a BOOLEAN, b INTEGER)");
  EXPECT_EQ(
      toTypeSql(
          ROW({{"a_", BOOLEAN()}, {"b$", INTEGER()}, {"c d", INTEGER()}})),
      "ROW(a_ BOOLEAN, b$ INTEGER, c d INTEGER)");
  EXPECT_EQ(toTypeSql(JSON()), "JSON");
  EXPECT_EQ(toTypeSql(UNKNOWN()), "UNKNOWN");
  VELOX_ASSERT_THROW(
      toTypeSql(FUNCTION({INTEGER()}, INTEGER())),
      "Type is not supported: FUNCTION");
}
} // namespace
} // namespace facebook::velox::exec::test

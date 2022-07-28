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
#include "velox/duckdb/conversion/DuckConversion.h"
#include <gtest/gtest.h>
#include <limits>
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/type/Variant.h"

using namespace facebook::velox;
using namespace facebook::velox::duckdb;

using ::duckdb::LogicalType;
using ::duckdb::Value;

TEST(DuckConversionTest, duckValueToVariant) {
  // NULLs must be the same.
  EXPECT_EQ(variant(TypeKind::UNKNOWN), duckValueToVariant(Value(), true));

  // Booleans.
  EXPECT_EQ(variant(false), duckValueToVariant(Value::BOOLEAN(0), true));
  EXPECT_EQ(variant(true), duckValueToVariant(Value::BOOLEAN(1), true));

  // Integers.
  auto min8 = std::numeric_limits<int8_t>::min();
  auto max8 = std::numeric_limits<int8_t>::max();
  EXPECT_EQ(variant(min8), duckValueToVariant(Value::TINYINT(min8), true));
  EXPECT_EQ(variant(max8), duckValueToVariant(Value::TINYINT(max8), true));

  auto min16 = std::numeric_limits<int16_t>::min();
  auto max16 = std::numeric_limits<int16_t>::max();
  EXPECT_EQ(variant(min16), duckValueToVariant(Value::SMALLINT(min16), true));
  EXPECT_EQ(variant(max16), duckValueToVariant(Value::SMALLINT(max16), true));

  auto min32 = std::numeric_limits<int32_t>::min();
  auto max32 = std::numeric_limits<int32_t>::max();
  EXPECT_EQ(variant(min32), duckValueToVariant(Value::INTEGER(min32), true));
  EXPECT_EQ(variant(max32), duckValueToVariant(Value::INTEGER(max32), true));

  auto min64 = std::numeric_limits<int64_t>::min();
  auto max64 = std::numeric_limits<int64_t>::max();
  EXPECT_EQ(variant(min64), duckValueToVariant(Value::BIGINT(min64), true));
  EXPECT_EQ(variant(max64), duckValueToVariant(Value::BIGINT(max64), true));

  // Doubles.
  for (const auto i : {0.99L, 88.321L, -3.23L}) {
    EXPECT_EQ(variant(double(i)), duckValueToVariant(Value::DOUBLE(i), true));

    // Floats are harder to compare because of low-precision. Just making sure
    // they don't throw.
    EXPECT_NO_THROW(duckValueToVariant(Value::FLOAT(i), true));
  }

  // Strings.
  std::vector<std::string> vec = {"", "asdf", "aS$!#^*HFD"};
  for (const auto& i : vec) {
    EXPECT_EQ(variant(i), duckValueToVariant(Value(i), true));
  }

  // Decimal type inference.
  EXPECT_EQ(
      *DECIMAL(4, 2),
      *duckValueToVariant(Value::DECIMAL(static_cast<int16_t>(10), 4, 2), false)
           .inferType());
  EXPECT_EQ(
      *DECIMAL(9, 2),
      *duckValueToVariant(Value::DECIMAL(static_cast<int32_t>(10), 9, 2), false)
           .inferType());
  EXPECT_EQ(
      *DECIMAL(17, 2),
      *duckValueToVariant(
           Value::DECIMAL(static_cast<int64_t>(10), 17, 2), false)
           .inferType());
  EXPECT_EQ(
      *DECIMAL(20, 2),
      *duckValueToVariant(
           Value::DECIMAL(::duckdb::hugeint_t(100), 20, 2), false)
           .inferType());
}

TEST(DuckConversionTest, duckValueToVariantUnsupported) {
  std::vector<LogicalType> unsupported = {
      LogicalType::TIME,
      LogicalType::TIMESTAMP,
      LogicalType::INTERVAL,
      LogicalType::LIST({LogicalType::INTEGER}),
      LogicalType::STRUCT(
          {{"a", LogicalType::INTEGER}, {"b", LogicalType::TINYINT}})};

  for (const auto& i : unsupported) {
    EXPECT_THROW(duckValueToVariant(Value(i), true), std::runtime_error);
  }
}

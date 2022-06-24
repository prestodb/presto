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
#include "velox/duckdb/conversion/DuckWrapper.h"
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/vector/tests/VectorMaker.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::duckdb;

class BaseDuckWrapperTest : public testing::Test {
 public:
  template <class T>
  void verifyUnaryResult(
      const std::string& query,
      const std::vector<T>& expectedOutput,
      const std::vector<bool>& nulls) {
    auto result = db_->execute(query);
    ASSERT_EQ(result->success(), true)
        << "Query failed: " << result->errorMessage();
    ASSERT_EQ(result->columnCount(), 1);
    size_t currentOffset = 0;
    while (result->next()) {
      auto rowVector = result->getVector();
      auto simpleVector = rowVector->childAt(0)->as<SimpleVector<T>>();
      ASSERT_NE(simpleVector, nullptr);
      for (auto i = 0; i < simpleVector->size(); i++) {
        auto rowNr = currentOffset + i;
        ASSERT_LE(rowNr, expectedOutput.size());
        if (nulls[rowNr]) {
          ASSERT_EQ(simpleVector->isNullAt(i), true);
        } else {
          ASSERT_EQ(simpleVector->isNullAt(i), false);
          ASSERT_EQ(simpleVector->valueAt(i), expectedOutput[rowNr]);
        }
      }
      currentOffset += simpleVector->size();
    }
    ASSERT_EQ(currentOffset, expectedOutput.size());
  }

  template <class T>
  void verifyUnaryResult(
      const std::string& query,
      const std::vector<T>& expectedOutput) {
    std::vector<bool> nulls(expectedOutput.size(), false);
    verifyUnaryResult<T>(move(query), move(expectedOutput), move(nulls));
  }

  template <class T>
  void verifyDuckToVeloxDecimal(
      const std::string& query,
      const std::vector<std::optional<T>>& expected) {
    auto result = db_->execute(query);
    ASSERT_EQ(result->success(), true)
        << "Query failed: " << result->errorMessage();
    ASSERT_EQ(result->columnCount(), 1);
    size_t currentOffset = 0;
    while (result->next()) {
      auto rowVector = result->getVector();
      auto simpleVector = rowVector->childAt(0)->as<SimpleVector<T>>();
      ASSERT_NE(simpleVector, nullptr);
      for (auto i = 0; i < simpleVector->size(); i++) {
        if (simpleVector->isNullAt(i)) {
          ASSERT_FALSE(expected[i].has_value());
          continue;
        }
        ASSERT_EQ(simpleVector->valueAt(i), expected[i]);
      }
    }
  }

  void execute(const std::string& query) {
    auto result = db_->execute(query);
    ASSERT_EQ(result->success(), true)
        << "Query failed: " << result->errorMessage();
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  std::unique_ptr<DuckDBWrapper> db_{
      std::make_unique<DuckDBWrapper>(execCtx_.get())};
};

TEST_F(BaseDuckWrapperTest, simpleSelect) {
  // scalar query
  verifyUnaryResult<int32_t>("SELECT 42::INTEGER", {42});

  // bit more complex
  verifyUnaryResult<int32_t>(
      "SELECT a::INTEGER FROM (VALUES (1), (2), (3)) tbl(a)", {1, 2, 3});

  // now with a table
  execute("CREATE TABLE integers(i INTEGER)");
  execute("INSERT INTO integers VALUES (1), (2), (3), (NULL)");

  verifyUnaryResult<int32_t>(
      "SELECT * FROM integers", {1, 2, 3, 0}, {false, false, false, true});
}

TEST_F(BaseDuckWrapperTest, scalarTypes) {
  // test various types
  // integer types
  verifyUnaryResult<int8_t>("SELECT 42::TINYINT", {42});
  verifyUnaryResult<int16_t>("SELECT 42::SMALLINT", {42});
  verifyUnaryResult<int32_t>("SELECT 42::INTEGER", {42});
  verifyUnaryResult<int64_t>("SELECT 42::BIGINT", {42});

  // hugeint is cast to double
  verifyUnaryResult<double>("SELECT 42::HUGEINT", {42});

  // numeric types
  verifyUnaryResult<float>("SELECT 1::FLOAT", {1.0});
  verifyUnaryResult<double>("SELECT 1::DOUBLE", {1.0});

  // date/timestamp
  verifyUnaryResult<Date>("SELECT DATE '1992-01-01'", {Date(8035)});
  verifyUnaryResult<Timestamp>(
      "SELECT TIMESTAMP '1992-01-01 13:04:20'", {Timestamp(694271060, 0)});

  // varchar
  verifyUnaryResult<StringView>("SELECT 'shortstr'", {StringView("shortstr")});
  verifyUnaryResult<StringView>(
      "SELECT '12characters'", {StringView("12characters")});
  verifyUnaryResult<StringView>(
      "SELECT 'this is a long, non-inlined, example string'",
      {StringView("this is a long, non-inlined, example string")});
}

TEST_F(BaseDuckWrapperTest, types) {
  // test various types
  // integer types
  verifyUnaryResult<int8_t>(
      "SELECT i::TINYINT FROM (VALUES (1), (2), (3), (NULL)) tbl(i)",
      {1, 2, 3, 0},
      {false, false, false, true});
  verifyUnaryResult<int16_t>(
      "SELECT i::SMALLINT FROM (VALUES (1), (2), (3), (NULL)) tbl(i)",
      {1, 2, 3, 0},
      {false, false, false, true});
  verifyUnaryResult<int32_t>(
      "SELECT i::INTEGER FROM (VALUES (1), (2), (3), (NULL)) tbl(i)",
      {1, 2, 3, 0},
      {false, false, false, true});
  verifyUnaryResult<int64_t>(
      "SELECT i::BIGINT FROM (VALUES (1), (2), (3), (NULL)) tbl(i)",
      {1, 2, 3, 0},
      {false, false, false, true});

  // hugeint is cast to double
  verifyUnaryResult<double>(
      "SELECT i::HUGEINT FROM (VALUES (1), (2), (4), (NULL)) tbl(i)",
      {1, 2, 4, 0},
      {false, false, false, true});

  // numeric types
  verifyUnaryResult<float>(
      "SELECT i::FLOAT FROM (VALUES (1), (2), (4), (NULL)) tbl(i)",
      {1, 2, 4, 0},
      {false, false, false, true});
  verifyUnaryResult<double>(
      "SELECT i::DOUBLE FROM (VALUES (1), (2), (4), (NULL)) tbl(i)",
      {1, 2, 4, 0},
      {false, false, false, true});

  // date/timestamp
  verifyUnaryResult<Date>(
      "SELECT i FROM (VALUES (DATE '1992-01-01'), (NULL)) tbl(i)",
      {Date(8035), Date(0)},
      {false, true});
  verifyUnaryResult<Timestamp>(
      "SELECT i FROM (VALUES (TIMESTAMP '1992-01-01 13:04:20'), (NULL)) tbl(i)",
      {Timestamp(694271060, 0), Timestamp(0, 0)},
      {false, true});

  // varchar
  verifyUnaryResult<StringView>(
      "SELECT * FROM (VALUES ('shortstr'), ('12characters'), ('this is a long, non-inlined, example string'), (NULL)) tbl(i)",
      {StringView("shortstr"),
       StringView("12characters"),
       StringView("this is a long, non-inlined, example string"),
       StringView("")},
      {false, false, false, true});
}

TEST_F(BaseDuckWrapperTest, tpchEmpty) {
  // test TPC-H loading and querying with an empty database
  execute("CALL dbgen(sf=0)");
  verifyUnaryResult<int32_t>(
      "SELECT l_orderkey FROM lineitem WHERE l_orderkey=1", {});
}

TEST_F(BaseDuckWrapperTest, tpchSF1) {
  // test TPC-H loading and querying SF0.01
  execute("CALL dbgen(sf=0.01)");
  // test conversion of date, decimal and string
  verifyUnaryResult<ShortDecimal>(
      "SELECT l_discount FROM lineitem LIMIT 1", {ShortDecimal(4)});
  verifyUnaryResult<Date>(
      "SELECT l_shipdate FROM lineitem LIMIT 1", {Date(9568)});
  verifyUnaryResult<StringView>(
      "SELECT l_comment FROM lineitem LIMIT 1",
      {StringView("egular courts above the")});
}

TEST_F(BaseDuckWrapperTest, duckToVeloxDecimal) {
  // Test SMALLINT decimal to ShortDecimal conversion.
  verifyDuckToVeloxDecimal<ShortDecimal>(
      "select * from (values (NULL), ('1.2'::decimal(2,1)),"
      "('2.2'::decimal(2,1)),('-4.2'::decimal(2,1)), (NULL))",
      {std::nullopt,
       ShortDecimal(12),
       ShortDecimal(22),
       ShortDecimal(-42),
       std::nullopt});

  // Test INTEGER decimal to ShortDecimal conversion.
  verifyDuckToVeloxDecimal<ShortDecimal>(
      "select * from (values ('1111.1111'::decimal(8,4)),"
      "('2222.2222'::decimal(8,4)),('-3333.3333'::decimal(8,4)))",
      {ShortDecimal(11111111),
       ShortDecimal(22222222),
       ShortDecimal(-33333333)});

  // Test BIGINT decimal to LongDecimal conversion.
  verifyDuckToVeloxDecimal<ShortDecimal>(
      "select * from (values ('-111111.111111'::decimal(12,6)),"
      "('222222.222222'::decimal(12,6)),('333333.333333'::decimal(12,6)))",
      {ShortDecimal(-111111111111),
       ShortDecimal(222222222222),
       ShortDecimal(333333333333)});

  verifyDuckToVeloxDecimal<LongDecimal>(
      "select * from (values (NULL),"
      "('12345678901234.789'::decimal(18,3) * 10000.555::decimal(20,3)),"
      "('-55555555555555.789'::decimal(18,3) * 10000.555::decimal(20,3)), (NULL),"
      "('-22222222222222.789'::decimal(18,3) * 10000.555::decimal(20,3)))",
      {std::nullopt,
       LongDecimal(buildInt128(0X1a24, 0Xfa35bb8777ffff77)),
       LongDecimal(buildInt128(0XFFFFFFFFFFFF8A59, 0X99FC706655BFAC11)),
       std::nullopt,
       LongDecimal(buildInt128(0XFFFFFFFFFFFFD0F0, 0XA3FE935B081D8D69))});
}

TEST_F(BaseDuckWrapperTest, decimalDictCoversion) {
  constexpr int32_t size = 6;
  ::duckdb::LogicalType* duckDecimalType =
      static_cast<::duckdb::LogicalType*>(duckdb_create_decimal_type(4, 2));
  ::duckdb::Vector data(*duckDecimalType, size);
  auto dataPtr = reinterpret_cast<int16_t*>(data.GetBuffer()->GetData());
  // Make dirty data which shouldn't be accessed.
  memset(dataPtr, 0xAB, sizeof(int16_t) * size);
  dataPtr[0] = 5000;
  dataPtr[2] = 1000;
  dataPtr[4] = 2000;
  // Turn vector into dictionary.
  ::duckdb::SelectionVector sel(size);
  sel.set_index(0, 2);
  sel.set_index(1, 4);
  sel.set_index(2, 0);
  sel.set_index(3, 4);
  sel.set_index(4, 2);
  sel.set_index(5, 0);
  data.Slice(sel, size);

  auto decimalType = DECIMAL(4, 2);
  auto actual = toVeloxVector(size, data, decimalType, pool_.get());
  std::vector<ShortDecimal> expectedData(
      {ShortDecimal(1000),
       ShortDecimal(2000),
       ShortDecimal(5000),
       ShortDecimal(2000),
       ShortDecimal(1000),
       ShortDecimal(5000)});

  test::VectorMaker maker(pool_.get());
  auto expectedFlatVector = maker.flatVector<ShortDecimal>(size, decimalType);

  for (auto i = 0; i < expectedData.size(); ++i) {
    expectedFlatVector->set(i, expectedData[i]);
  }

  for (auto i = 0; i < actual->size(); i++) {
    ASSERT_TRUE(expectedFlatVector->equalValueAt(actual.get(), i, i));
  }
  delete duckDecimalType;
}

TEST_F(BaseDuckWrapperTest, dictConversion) {
  ::duckdb::Vector data(::duckdb::LogicalTypeId::VARCHAR, 5);
  auto dataPtr =
      reinterpret_cast<::duckdb::string_t*>(data.GetBuffer()->GetData());
  // Make dirty data which shouldn't be accessed.
  memset(dataPtr, 0xAB, sizeof(::duckdb::string_t) * 5);
  dataPtr[2] = ::duckdb::string_t("value1");
  dataPtr[4] = ::duckdb::string_t("value2");

  // Turn vector into dictionary.
  ::duckdb::SelectionVector sel(5);
  sel.set_index(0, 2);
  sel.set_index(1, 4);
  sel.set_index(2, 2);
  sel.set_index(3, 4);
  sel.set_index(4, 2);
  data.Slice(sel, 5);

  auto actual = toVeloxVector(5, data, VarcharType::create(), pool_.get());

  test::VectorMaker maker(pool_.get());
  std::vector<std::string> expectedData(
      {"value1", "value2", "value1", "value2", "value1"});
  auto expected = maker.flatVector(expectedData);
  for (auto i = 0; i < actual->size(); i++) {
    ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i));
  }
}

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

  void execute(const std::string& query) {
    auto result = db_->execute(query);
    ASSERT_EQ(result->success(), true)
        << "Query failed: " << result->errorMessage();
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
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
  verifyUnaryResult<Timestamp>(
      "SELECT DATE '1992-01-01'", {Timestamp(694224000, 0)});
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
  verifyUnaryResult<Timestamp>(
      "SELECT i FROM (VALUES (DATE '1992-01-01'), (NULL)) tbl(i)",
      {Timestamp(694224000, 0), Timestamp(0, 0)},
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
  verifyUnaryResult<double>("SELECT l_discount FROM lineitem LIMIT 1", {0.04});
  verifyUnaryResult<Timestamp>(
      "SELECT l_shipdate FROM lineitem LIMIT 1", {Timestamp(826675200, 0)});
  verifyUnaryResult<StringView>(
      "SELECT l_comment FROM lineitem LIMIT 1",
      {StringView("egular courts above the")});
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

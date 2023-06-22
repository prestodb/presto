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

#include "velox/connectors/hive/TableHandle.h"

#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

using namespace facebook::velox;

TEST(FileHandleTest, hiveColumnHandle) {
  Type::registerSerDe();
  connector::hive::HiveColumnHandle::registerSerDe();
  auto columnType = ROW(
      {{"c0c0", BIGINT()},
       {"c0c1",
        ARRAY(MAP(
            VARCHAR(), ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})))}});
  auto columnHandle = exec::test::HiveConnectorTestBase::makeColumnHandle(
      "columnHandle", columnType, columnType, {"c0.c0c1[3][\"foo\"].c0c1c0"});
  ASSERT_EQ(columnHandle->name(), "columnHandle");
  ASSERT_EQ(
      columnHandle->columnType(),
      connector::hive::HiveColumnHandle::ColumnType::kRegular);
  ASSERT_EQ(columnHandle->dataType(), columnType);
  ASSERT_EQ(columnHandle->hiveType(), columnType);
  ASSERT_FALSE(columnHandle->isPartitionKey());

  auto str = columnHandle->toString();
  auto obj = columnHandle->serialize();
  auto clone =
      ISerializable::deserialize<connector::hive::HiveColumnHandle>(obj);
  ASSERT_EQ(clone->toString(), str);

  auto incompatibleHiveType = ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}});
  VELOX_ASSERT_THROW(
      exec::test::HiveConnectorTestBase::makeColumnHandle(
          "columnHandle",
          columnType,
          incompatibleHiveType,
          {"c0.c0c1[3][\"foo\"].c0c1c0"}),
      "data type ROW<c0c0:BIGINT,c0c1:ARRAY<MAP<VARCHAR,ROW<c0c1c0:BIGINT,c0c1c1:BIGINT>>>> and hive type ROW<c0c0:BIGINT,c0c1:BIGINT> do not match");
}

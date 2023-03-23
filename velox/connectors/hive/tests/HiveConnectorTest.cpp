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

#include "velox/connectors/hive/HiveConnector.h"
#include <gtest/gtest.h>

namespace facebook::velox::connector::hive {
namespace {

using namespace facebook::velox::common;

class HiveConnectorTest : public testing::Test {
 protected:
  std::shared_ptr<memory::MemoryPool> pool_ = memory::getDefaultMemoryPool();
};

HiveColumnHandle makeColumnHandle(
    const std::string& name,
    const TypePtr& type,
    const std::vector<std::string>& requiredSubfields) {
  std::vector<Subfield> subfields;
  for (auto& path : requiredSubfields) {
    subfields.emplace_back(path);
  }
  return HiveColumnHandle(
      name, HiveColumnHandle::ColumnType::kRegular, type, std::move(subfields));
}

void validateNullConstant(const ScanSpec& spec, const Type& type) {
  ASSERT_TRUE(spec.isConstant());
  auto constant = spec.constantValue();
  ASSERT_TRUE(constant->isConstantEncoding());
  ASSERT_EQ(*constant->type(), type);
  ASSERT_TRUE(constant->isNullAt(0));
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_multilevel) {
  auto columnType = ROW(
      {{"c0c0", BIGINT()},
       {"c0c1",
        ARRAY(MAP(
            VARCHAR(), ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})))}});
  auto rowType = ROW({{"c0", columnType}});
  auto columnHandle =
      makeColumnHandle("c0", columnType, {"c0.c0c1[3][\"foo\"].c0c1c0"});
  auto scanSpec =
      HiveDataSource::makeScanSpec({}, rowType, {&columnHandle}, pool_.get());
  auto* c0c0 = scanSpec->childByName("c0")->childByName("c0c0");
  validateNullConstant(*c0c0, *BIGINT());
  auto* c0c1 = scanSpec->childByName("c0")->childByName("c0c1");
  ASSERT_EQ(c0c1->maxArrayElementsCount(), 3);
  auto* elements = c0c1->childByName(ScanSpec::kArrayElementsFieldName);
  auto* keysFilter =
      elements->childByName(ScanSpec::kMapKeysFieldName)->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_TRUE(applyFilter(*keysFilter, "foo"_sv));
  ASSERT_FALSE(applyFilter(*keysFilter, "bar"_sv));
  ASSERT_FALSE(keysFilter->testNull());
  auto* values = elements->childByName(ScanSpec::kMapValuesFieldName);
  auto* c0c1c0 = values->childByName("c0c1c0");
  ASSERT_FALSE(c0c1c0->isConstant());
  ASSERT_FALSE(c0c1c0->filter());
  validateNullConstant(*values->childByName("c0c1c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeFields) {
  auto columnType = ROW(
      {{"c0c0",
        ROW(
            {{"c0c0c0", BIGINT()},
             {"c0c0c1", BIGINT()},
             {"c0c0c2", BIGINT()}})},
       {"c0c1", ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})}});
  auto rowType = ROW({{"c0", columnType}});
  auto columnHandle = makeColumnHandle(
      "c0",
      columnType,
      {"c0.c0c0.c0c0c0", "c0.c0c0.c0c0c2", "c0.c0c1", "c0.c0c1.c0c1c0"});
  auto scanSpec =
      HiveDataSource::makeScanSpec({}, rowType, {&columnHandle}, pool_.get());
  auto* c0c0 = scanSpec->childByName("c0")->childByName("c0c0");
  ASSERT_FALSE(c0c0->childByName("c0c0c0")->isConstant());
  ASSERT_FALSE(c0c0->childByName("c0c0c2")->isConstant());
  validateNullConstant(*c0c0->childByName("c0c0c1"), *BIGINT());
  auto* c0c1 = scanSpec->childByName("c0")->childByName("c0c1");
  ASSERT_FALSE(c0c1->isConstant());
  ASSERT_FALSE(c0c1->hasFilter());
  ASSERT_FALSE(c0c1->childByName("c0c1c0")->isConstant());
  ASSERT_FALSE(c0c1->childByName("c0c1c1")->isConstant());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeArray) {
  auto columnType =
      ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto rowType = ROW({{"c0", columnType}});
  auto columnHandle =
      makeColumnHandle("c0", columnType, {"c0[1].c0c0", "c0[2].c0c2"});
  auto scanSpec =
      HiveDataSource::makeScanSpec({}, rowType, {&columnHandle}, pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_EQ(c0->maxArrayElementsCount(), 2);
  auto* elements = c0->childByName(ScanSpec::kArrayElementsFieldName);
  ASSERT_FALSE(elements->childByName("c0c0")->isConstant());
  ASSERT_FALSE(elements->childByName("c0c2")->isConstant());
  validateNullConstant(*elements->childByName("c0c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeMap) {
  auto columnType =
      MAP(BIGINT(),
          ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto rowType = ROW({{"c0", columnType}});
  auto columnHandle =
      makeColumnHandle("c0", columnType, {"c0[10].c0c0", "c0[20].c0c2"});
  auto scanSpec =
      HiveDataSource::makeScanSpec({}, rowType, {&columnHandle}, pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  auto* keysFilter = c0->childByName(ScanSpec::kMapKeysFieldName)->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_TRUE(applyFilter(*keysFilter, 10));
  ASSERT_TRUE(applyFilter(*keysFilter, 20));
  ASSERT_FALSE(applyFilter(*keysFilter, 15));
  auto* values = c0->childByName(ScanSpec::kMapValuesFieldName);
  ASSERT_FALSE(values->childByName("c0c0")->isConstant());
  ASSERT_FALSE(values->childByName("c0c2")->isConstant());
  validateNullConstant(*values->childByName("c0c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_allSubscripts) {
  auto columnType =
      MAP(BIGINT(), ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}})));
  auto rowType = ROW({{"c0", columnType}});
  for (auto* path : {"c0", "c0[*]", "c0[*][*]"}) {
    SCOPED_TRACE(path);
    auto columnHandle = makeColumnHandle("c0", columnType, {path});
    auto scanSpec =
        HiveDataSource::makeScanSpec({}, rowType, {&columnHandle}, pool_.get());
    auto* c0 = scanSpec->childByName("c0");
    ASSERT_FALSE(c0->childByName(ScanSpec::kMapKeysFieldName)->filter());
    auto* values = c0->childByName(ScanSpec::kMapValuesFieldName);
    ASSERT_EQ(
        values->maxArrayElementsCount(),
        std::numeric_limits<vector_size_t>::max());
    auto* elements = values->childByName(ScanSpec::kArrayElementsFieldName);
    ASSERT_FALSE(elements->hasFilter());
    ASSERT_FALSE(elements->childByName("c0c0")->isConstant());
    ASSERT_FALSE(elements->childByName("c0c1")->isConstant());
  }
  auto columnHandle = makeColumnHandle("c0", columnType, {"c0[*][*].c0c0"});
  auto scanSpec =
      HiveDataSource::makeScanSpec({}, rowType, {&columnHandle}, pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_FALSE(c0->childByName(ScanSpec::kMapKeysFieldName)->filter());
  auto* values = c0->childByName(ScanSpec::kMapValuesFieldName);
  ASSERT_EQ(
      values->maxArrayElementsCount(),
      std::numeric_limits<vector_size_t>::max());
  auto* elements = values->childByName(ScanSpec::kArrayElementsFieldName);
  ASSERT_FALSE(elements->hasFilter());
  ASSERT_FALSE(elements->childByName("c0c0")->isConstant());
  validateNullConstant(*elements->childByName("c0c1"), *BIGINT());
}

} // namespace
} // namespace facebook::velox::connector::hive

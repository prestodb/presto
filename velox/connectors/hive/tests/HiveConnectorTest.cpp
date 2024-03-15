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
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/expression/ExprToSubfieldFilter.h"

namespace facebook::velox::connector::hive {
namespace {

using namespace facebook::velox::common;
using namespace facebook::velox::exec::test;

class HiveConnectorTest : public exec::test::HiveConnectorTestBase {
 protected:
  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
};

void validateNullConstant(const ScanSpec& spec, const Type& type) {
  ASSERT_TRUE(spec.isConstant());
  auto constant = spec.constantValue();
  ASSERT_TRUE(constant->isConstantEncoding());
  ASSERT_EQ(*constant->type(), type);
  ASSERT_TRUE(constant->isNullAt(0));
}

std::vector<Subfield> makeSubfields(const std::vector<std::string>& paths) {
  std::vector<Subfield> subfields;
  for (auto& path : paths) {
    subfields.emplace_back(path);
  }
  return subfields;
}

folly::F14FastMap<std::string, std::vector<const common::Subfield*>>
groupSubfields(const std::vector<Subfield>& subfields) {
  folly::F14FastMap<std::string, std::vector<const common::Subfield*>> grouped;
  for (auto& subfield : subfields) {
    auto& name =
        static_cast<const common::Subfield::NestedField&>(*subfield.path()[0])
            .name();
    grouped[name].push_back(&subfield);
  }
  return grouped;
}

TEST_F(HiveConnectorTest, hiveConfig) {
  ASSERT_EQ(
      HiveConfig::insertExistingPartitionsBehaviorString(
          HiveConfig::InsertExistingPartitionsBehavior::kError),
      "ERROR");
  ASSERT_EQ(
      HiveConfig::insertExistingPartitionsBehaviorString(
          HiveConfig::InsertExistingPartitionsBehavior::kOverwrite),
      "OVERWRITE");
  ASSERT_EQ(
      HiveConfig::insertExistingPartitionsBehaviorString(
          static_cast<HiveConfig::InsertExistingPartitionsBehavior>(100)),
      "UNKNOWN BEHAVIOR 100");
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_multilevel) {
  auto columnType = ROW(
      {{"c0c0", BIGINT()},
       {"c0c1",
        ARRAY(MAP(
            VARCHAR(), ROW({{"c0c1c0", BIGINT()}, {"c0c1c1", BIGINT()}})))}});
  auto rowType = ROW({{"c0", columnType}});
  auto subfields = makeSubfields({"c0.c0c1[3][\"foo\"].c0c1c0"});
  auto scanSpec = makeScanSpec(
      rowType, groupSubfields(subfields), {}, nullptr, {}, {}, pool_.get());
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
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0.c0c0.c0c0c0", "c0.c0c0.c0c0c2", "c0.c0c1", "c0.c0c1.c0c1c0"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
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
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[1].c0c0", "c0[2].c0c2"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_EQ(c0->maxArrayElementsCount(), 2);
  ASSERT_TRUE(c0->flatMapFeatureSelection().empty());
  auto* elements = c0->childByName(ScanSpec::kArrayElementsFieldName);
  ASSERT_FALSE(elements->childByName("c0c0")->isConstant());
  ASSERT_FALSE(elements->childByName("c0c2")->isConstant());
  validateNullConstant(*elements->childByName("c0c1"), *BIGINT());
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeArrayNegative) {
  auto columnType =
      ARRAY(ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto rowType = ROW({{"c0", columnType}});
  auto subfields = makeSubfields({"c0[1].c0c0", "c0[-1].c0c2"});
  auto groupedSubfields = groupSubfields(subfields);
  VELOX_ASSERT_USER_THROW(
      makeScanSpec(rowType, groupedSubfields, {}, nullptr, {}, {}, pool_.get()),
      "Non-positive array subscript cannot be push down");
}

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_mergeMap) {
  auto columnType =
      MAP(BIGINT(),
          ROW({{"c0c0", BIGINT()}, {"c0c1", BIGINT()}, {"c0c2", BIGINT()}}));
  auto rowType = ROW({{"c0", columnType}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[10].c0c0", "c0[20].c0c2"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_EQ(
      c0->flatMapFeatureSelection(), std::vector<std::string>({"10", "20"}));
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
    auto scanSpec = makeScanSpec(
        rowType,
        groupSubfields(makeSubfields({path})),
        {},
        nullptr,
        {},
        {},
        pool_.get());
    auto* c0 = scanSpec->childByName("c0");
    ASSERT_TRUE(c0->flatMapFeatureSelection().empty());
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
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[*][*].c0c0"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
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

TEST_F(HiveConnectorTest, makeScanSpec_requiredSubfields_doubleMapKey) {
  auto rowType =
      ROW({{"c0", MAP(REAL(), BIGINT())}, {"c1", MAP(DOUBLE(), BIGINT())}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[0]", "c1[-1]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* keysFilter = scanSpec->childByName("c0")
                         ->childByName(ScanSpec::kMapKeysFieldName)
                         ->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_TRUE(applyFilter(*keysFilter, 0.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, 0.99f));
  ASSERT_FALSE(applyFilter(*keysFilter, 1.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, -0.99f));
  ASSERT_FALSE(applyFilter(*keysFilter, -1.0f));
  keysFilter = scanSpec->childByName("c1")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(keysFilter);
  ASSERT_FALSE(applyFilter(*keysFilter, 0.0));
  ASSERT_TRUE(applyFilter(*keysFilter, -1.0));
  ASSERT_TRUE(applyFilter(*keysFilter, -1.99));
  ASSERT_FALSE(applyFilter(*keysFilter, -2.0));

  // Integer min and max means infinities.
  scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0[-9223372036854775808]", "c1[9223372036854775807]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  keysFilter = scanSpec->childByName("c0")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, -1e30f));
  ASSERT_FALSE(applyFilter(*keysFilter, -9223370000000000000.0f));
  keysFilter = scanSpec->childByName("c1")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, 1e100));
  ASSERT_FALSE(applyFilter(*keysFilter, 9223372036854700000.0));
  scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0[9223372036854775807]", "c0[-9223372036854775808]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  keysFilter = scanSpec->childByName("c0")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, -1e30f));
  ASSERT_FALSE(applyFilter(*keysFilter, 0.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, 1e30f));

  // Unrepresentable values.
  scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields({"c0[-100000000]", "c0[100000000]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  keysFilter = scanSpec->childByName("c0")
                   ->childByName(ScanSpec::kMapKeysFieldName)
                   ->filter();
  ASSERT_TRUE(applyFilter(*keysFilter, -100000000.0f));
  ASSERT_FALSE(applyFilter(*keysFilter, -100000008.0f));
  ASSERT_FALSE(applyFilter(*keysFilter, 0.0f));
  ASSERT_TRUE(applyFilter(*keysFilter, 100000000.0f));
  ASSERT_FALSE(applyFilter(*keysFilter, 100000008.0f));
}

TEST_F(HiveConnectorTest, makeScanSpec_filtersNotInRequiredSubfields) {
  auto c0Type = ROW({
      {"c0c0", BIGINT()},
      {"c0c1", VARCHAR()},
      {"c0c2", ROW({{"c0c2c0", BIGINT()}})},
      {"c0c3", ROW({{"c0c3c0", BIGINT()}})},
      {"c0c4", BIGINT()},
  });
  auto c1c1Type = ROW({{"c1c1c0", BIGINT()}, {"c1c1c1", BIGINT()}});
  auto c1Type = ROW({
      {"c1c0", ROW({{"c1c0c0", BIGINT()}, {"c1c0c1", BIGINT()}})},
      {"c1c1", c1c1Type},
  });
  SubfieldFilters filters;
  filters.emplace(Subfield("c0.c0c0"), exec::equal(42));
  filters.emplace(Subfield("c0.c0c2"), exec::isNotNull());
  filters.emplace(Subfield("c0.c0c3"), exec::isNotNull());
  filters.emplace(Subfield("c1.c1c0.c1c0c0"), exec::equal(43));
  auto scanSpec = makeScanSpec(
      ROW({{"c0", c0Type}}),
      groupSubfields(makeSubfields({"c0.c0c1", "c0.c0c3"})),
      filters,
      ROW({{"c0", c0Type}, {"c1", c1Type}}),
      {},
      {},
      pool_.get());
  auto c0 = scanSpec->childByName("c0");
  ASSERT_FALSE(c0->isConstant());
  ASSERT_TRUE(c0->projectOut());
  // Filter only.
  auto* c0c0 = scanSpec->childByName("c0")->childByName("c0c0");
  ASSERT_FALSE(c0c0->isConstant());
  ASSERT_TRUE(c0c0->filter());
  // Project output.
  auto* c0c1 = scanSpec->childByName("c0")->childByName("c0c1");
  ASSERT_FALSE(c0c1->isConstant());
  ASSERT_FALSE(c0c1->filter());
  // Filter on struct, no children.
  auto* c0c2 = scanSpec->childByName("c0")->childByName("c0c2");
  ASSERT_FALSE(c0c2->isConstant());
  ASSERT_TRUE(c0c2->filter());
  validateNullConstant(*c0c2->childByName("c0c2c0"), *BIGINT());
  // Filtered and project out.
  auto* c0c3 = scanSpec->childByName("c0")->childByName("c0c3");
  ASSERT_FALSE(c0c3->isConstant());
  ASSERT_TRUE(c0c3->filter());
  ASSERT_FALSE(c0c3->childByName("c0c3c0")->isConstant());
  // Filter only, column not projected out.
  auto* c1 = scanSpec->childByName("c1");
  ASSERT_FALSE(c1->isConstant());
  ASSERT_FALSE(c1->projectOut());
  auto* c1c0 = c1->childByName("c1c0");
  ASSERT_FALSE(c1c0->childByName("c1c0c0")->isConstant());
  ASSERT_TRUE(c1c0->childByName("c1c0c0"));
  validateNullConstant(*c1c0->childByName("c1c0c1"), *BIGINT());
  validateNullConstant(*c1->childByName("c1c1"), *c1c1Type);
}

TEST_F(HiveConnectorTest, makeScanSpec_duplicateSubfields) {
  auto c0Type = MAP(BIGINT(), MAP(BIGINT(), BIGINT()));
  auto c1Type = MAP(VARCHAR(), MAP(BIGINT(), BIGINT()));
  auto rowType = ROW({{"c0", c0Type}, {"c1", c1Type}});
  auto scanSpec = makeScanSpec(
      rowType,
      groupSubfields(makeSubfields(
          {"c0[10][1]", "c0[10][2]", "c1[\"foo\"][1]", "c1[\"foo\"][2]"})),
      {},
      nullptr,
      {},
      {},
      pool_.get());
  auto* c0 = scanSpec->childByName("c0");
  ASSERT_EQ(c0->children().size(), 2);
  auto* c1 = scanSpec->childByName("c1");
  ASSERT_EQ(c1->children().size(), 2);
}

// For TEXTFILE, partition key is not included in data columns.
TEST_F(HiveConnectorTest, makeScanSpec_filterPartitionKey) {
  auto rowType = ROW({{"c0", BIGINT()}});
  SubfieldFilters filters;
  filters.emplace(Subfield("ds"), exec::equal("2023-10-13"));
  auto scanSpec = makeScanSpec(
      rowType, {}, filters, rowType, {{"ds", nullptr}}, {}, pool_.get());
  ASSERT_TRUE(scanSpec->childByName("c0")->projectOut());
  ASSERT_FALSE(scanSpec->childByName("ds")->projectOut());
}

TEST_F(HiveConnectorTest, extractFiltersFromRemainingFilter) {
  core::QueryCtx queryCtx;
  exec::SimpleExpressionEvaluator evaluator(&queryCtx, pool_.get());
  auto rowType = ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), DECIMAL(20, 0)});

  auto expr = parseExpr("not (c0 > 0 or c1 > 0)", rowType);
  SubfieldFilters filters;
  double sampleRate = 1;
  auto remaining = extractFiltersFromRemainingFilter(
      expr, &evaluator, false, filters, sampleRate);
  ASSERT_FALSE(remaining);
  ASSERT_EQ(sampleRate, 1);
  ASSERT_EQ(filters.size(), 2);
  ASSERT_GT(filters.count(Subfield("c0")), 0);
  ASSERT_GT(filters.count(Subfield("c1")), 0);

  expr = parseExpr("not (c0 > 0 or c1 > c0)", rowType);
  filters.clear();
  remaining = extractFiltersFromRemainingFilter(
      expr, &evaluator, false, filters, sampleRate);
  ASSERT_EQ(sampleRate, 1);
  ASSERT_EQ(filters.size(), 1);
  ASSERT_GT(filters.count(Subfield("c0")), 0);
  ASSERT_TRUE(remaining);
  ASSERT_EQ(remaining->toString(), "not(gt(ROW[\"c1\"],ROW[\"c0\"]))");

  expr = parseExpr(
      "not (c2 > 1::decimal(20, 0) or c2 < 0::decimal(20, 0))", rowType);
  filters.clear();
  remaining = extractFiltersFromRemainingFilter(
      expr, &evaluator, false, filters, sampleRate);
  ASSERT_EQ(sampleRate, 1);
  ASSERT_GT(filters.count(Subfield("c2")), 0);
  // Change these once HUGEINT filter merge is fixed.
  ASSERT_TRUE(remaining);
  ASSERT_EQ(
      remaining->toString(), "not(lt(ROW[\"c2\"],cast 0 as DECIMAL(20, 0)))");
}

TEST_F(HiveConnectorTest, prestoTableSampling) {
  core::QueryCtx queryCtx;
  exec::SimpleExpressionEvaluator evaluator(&queryCtx, pool_.get());
  auto rowType = ROW({"c0"}, {BIGINT()});

  auto expr = parseExpr("rand() < 0.5", rowType);
  SubfieldFilters filters;
  double sampleRate = 1;
  auto remaining = extractFiltersFromRemainingFilter(
      expr, &evaluator, false, filters, sampleRate);
  ASSERT_FALSE(remaining);
  ASSERT_EQ(sampleRate, 0.5);
  ASSERT_TRUE(filters.empty());

  expr = parseExpr("c0 > 0 and rand() < 0.5", rowType);
  filters.clear();
  sampleRate = 1;
  remaining = extractFiltersFromRemainingFilter(
      expr, &evaluator, false, filters, sampleRate);
  ASSERT_FALSE(remaining);
  ASSERT_EQ(sampleRate, 0.5);
  ASSERT_EQ(filters.size(), 1);
  ASSERT_GT(filters.count(Subfield("c0")), 0);

  expr = parseExpr("rand() < 0.5 and rand() < 0.5", rowType);
  filters.clear();
  sampleRate = 1;
  remaining = extractFiltersFromRemainingFilter(
      expr, &evaluator, false, filters, sampleRate);
  ASSERT_FALSE(remaining);
  ASSERT_EQ(sampleRate, 0.25);
  ASSERT_TRUE(filters.empty());

  expr = parseExpr("c0 > 0 or rand() < 0.5", rowType);
  filters.clear();
  sampleRate = 1;
  remaining = extractFiltersFromRemainingFilter(
      expr, &evaluator, false, filters, sampleRate);
  ASSERT_TRUE(remaining);
  ASSERT_EQ(*remaining, *expr);
  ASSERT_EQ(sampleRate, 1);
  ASSERT_TRUE(filters.empty());
}

} // namespace
} // namespace facebook::velox::connector::hive

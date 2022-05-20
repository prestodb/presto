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
#pragma once

#include <gtest/gtest.h>
#include "velox/common/caching/SsdCache.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/type/Variant.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/VectorMaker.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::exec::test {
class OperatorTestBase : public testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  OperatorTestBase();
  ~OperatorTestBase() override;

  void SetUp() override;

  static void SetUpTestCase();

  void createDuckDbTable(const std::vector<RowVectorPtr>& data) {
    duckDbQueryRunner_.createTable("tmp", data);
  }

  void createDuckDbTable(
      const std::string& tableName,
      const std::vector<RowVectorPtr>& data) {
    duckDbQueryRunner_.createTable(tableName, data);
  }

  std::shared_ptr<Task> assertQueryOrdered(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql,
      const std::vector<uint32_t>& sortingKeys) {
    return test::assertQuery(plan, duckDbSql, duckDbQueryRunner_, sortingKeys);
  }

  std::shared_ptr<Task> assertQueryOrdered(
      const CursorParameters& params,
      const std::string& duckDbSql,
      const std::vector<uint32_t>& sortingKeys) {
    return test::assertQuery(
        params, [&](auto*) {}, duckDbSql, duckDbQueryRunner_, sortingKeys);
  }

  /// Assumes plan has a single leaf node. All splits are added to that node.
  std::shared_ptr<Task> assertQueryOrdered(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>& splits,
      const std::string& duckDbSql,
      const std::vector<uint32_t>& sortingKeys) {
    return assertQuery(plan, splits, duckDbSql, sortingKeys);
  }

  std::shared_ptr<Task> assertQuery(
      const CursorParameters& params,
      const std::string& duckDbSql) {
    return test::assertQuery(
        params, [&](exec::Task* /*task*/) {}, duckDbSql, duckDbQueryRunner_);
  }

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::string& duckDbSql) {
    return test::assertQuery(plan, duckDbSql, duckDbQueryRunner_);
  }

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const RowVectorPtr& expectedResults) {
    return test::assertQuery(plan, {expectedResults});
  }

  /// Assumes plan has a single leaf node. All splits are added to that node.
  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>&
          connectorSplits,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

  /// Assumes plan has a single leaf node. All splits are added to that node.
  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      std::vector<exec::Split>&& splits,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

  std::shared_ptr<Task> assertQuery(
      const core::PlanNodePtr& plan,
      std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>&& splits,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

  static RowTypePtr makeRowType(std::vector<TypePtr>&& types) {
    return velox::test::VectorMaker::rowType(
        std::forward<std::vector<TypePtr>&&>(types));
  }

  static std::shared_ptr<core::FieldAccessTypedExpr> toFieldExpr(
      const std::string& name,
      const RowTypePtr& rowType);

  std::shared_ptr<const core::ITypedExpr> parseExpr(
      const std::string& text,
      RowTypePtr rowType);

  DuckDbQueryRunner duckDbQueryRunner_;

  // Used as default MappedMemory. Created on first use.
  static std::shared_ptr<cache::AsyncDataCache> asyncDataCache_;
};
} // namespace facebook::velox::exec::test

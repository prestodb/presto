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
      const std::shared_ptr<const core::PlanNode>& plan,
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

  std::shared_ptr<Task> assertQueryOrdered(
      const std::shared_ptr<const core::PlanNode>& plan,
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
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::string& duckDbSql) {
    return test::assertQuery(plan, duckDbSql, duckDbQueryRunner_);
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const RowVectorPtr& expectedResults) {
    return test::assertQuery(plan, {expectedResults});
  }

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>&
          connectorSplits,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

  std::shared_ptr<Task> assertQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      std::vector<exec::Split>&& splits,
      const std::string& duckDbSql,
      std::optional<std::vector<uint32_t>> sortingKeys = std::nullopt);

  RowVectorPtr getResults(std::shared_ptr<const core::PlanNode> planNode);

  RowVectorPtr getResults(const CursorParameters& params);

  static std::shared_ptr<const RowType> makeRowType(
      std::vector<std::shared_ptr<const Type>>&& types) {
    return velox::test::VectorMaker::rowType(
        std::forward<std::vector<std::shared_ptr<const Type>>&&>(types));
  }

  static std::shared_ptr<core::FieldAccessTypedExpr> toFieldExpr(
      const std::string& name,
      const std::shared_ptr<const RowType>& rowType);

  std::shared_ptr<const core::ITypedExpr> parseExpr(
      const std::string& text,
      std::shared_ptr<const RowType> rowType);

  DuckDbQueryRunner duckDbQueryRunner_;

  // Parametrized subclasses set this to choose the cache code path.
  bool useAsyncCache_{true};

  // Used as default MappedMemory if 'useAsyncCache_' is true. Created on first
  // use.
  static std::shared_ptr<cache::AsyncDataCache> asyncDataCache_;
};
} // namespace facebook::velox::exec::test

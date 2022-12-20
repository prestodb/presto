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

#include "velox/core/Context.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"

using namespace ::facebook::velox;
using namespace ::facebook::velox::core;

namespace {
std::shared_ptr<QueryCtx> getSpillQueryCtx(
    bool spillEnabled,
    bool aggrSpillEnabled,
    bool joinSpillEnabled,
    bool orderBySpillEnabled) {
  std::unordered_map<std::string, std::string> configData({
      {QueryConfig::kSpillEnabled, spillEnabled ? "true" : "false"},
      {QueryConfig::kAggregationSpillEnabled,
       aggrSpillEnabled ? "true" : "false"},
      {QueryConfig::kJoinSpillEnabled, joinSpillEnabled ? "true" : "false"},
      {QueryConfig::kOrderBySpillEnabled,
       orderBySpillEnabled ? "true" : "false"},
  });
  return std::make_shared<QueryCtx>(
      nullptr, std::make_shared<MemConfig>(configData));
}
}; // namespace

TEST(TestPlanFragment, basic) {
  // Create a small node tree: orderBy <- tableScan.
  auto rowType = ROW({"name1"}, {BIGINT()});

  std::shared_ptr<connector::ConnectorTableHandle> tableHandle;
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  auto tableScan =
      std::make_shared<TableScanNode>("2", rowType, tableHandle, assignments);

  std::vector<FieldAccessTypedExprPtr> sortingKeys{nullptr};
  std::vector<SortOrder> sortingOrders{{true, true}};

  auto orderBy = std::make_shared<OrderByNode>(
      "-1", sortingKeys, sortingOrders, false, tableScan);

  PlanFragment fragmentNoSpill{tableScan};
  PlanFragment fragmentOrderBySpill{orderBy};

  // Create several query contexts with a variety of combination of spilling
  // options.
  auto queryCtxAllEnabled = getSpillQueryCtx(true, true, true, true);
  auto queryCtxDisabled = getSpillQueryCtx(false, true, true, true);
  auto queryCtxAllDisabled = getSpillQueryCtx(false, false, false, false);
  auto queryCtxAggrEnabled = getSpillQueryCtx(true, true, false, false);
  auto queryCtxOrderByEnabled = getSpillQueryCtx(true, false, false, true);

  // No matter the spilling flags there is nothing to spill in this plan
  // fragment.
  EXPECT_FALSE(fragmentNoSpill.canSpill(queryCtxAllEnabled->queryConfig()));
  EXPECT_FALSE(fragmentNoSpill.canSpill(queryCtxDisabled->queryConfig()));
  EXPECT_FALSE(fragmentNoSpill.canSpill(queryCtxAllDisabled->queryConfig()));

  // We can only spill from the 'order by' in this plan fragment, so we are
  // looking for the specific flags enabled.
  EXPECT_TRUE(fragmentOrderBySpill.canSpill(queryCtxAllEnabled->queryConfig()));
  EXPECT_FALSE(fragmentOrderBySpill.canSpill(queryCtxDisabled->queryConfig()));
  EXPECT_FALSE(
      fragmentOrderBySpill.canSpill(queryCtxAllDisabled->queryConfig()));
  EXPECT_FALSE(
      fragmentOrderBySpill.canSpill(queryCtxAggrEnabled->queryConfig()));
  EXPECT_TRUE(
      fragmentOrderBySpill.canSpill(queryCtxOrderByEnabled->queryConfig()));
}

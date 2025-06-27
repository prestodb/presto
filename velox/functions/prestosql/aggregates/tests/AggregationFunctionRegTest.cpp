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

#include "velox/exec/Aggregate.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

namespace facebook::velox::aggregate::test {

class AggregationFunctionRegTest : public testing::Test {
 protected:
  void clearAndCheckRegistry() {
    exec::aggregateFunctions().withWLock([&](auto& functionsMap) {
      functionsMap.clear();
      EXPECT_EQ(0, functionsMap.size());
    });
  }
};

TEST_F(AggregationFunctionRegTest, prefix) {
  // Remove all functions and check for no entries.
  clearAndCheckRegistry();

  // Register without prefix and memorize function maps.
  aggregate::prestosql::registerAllAggregateFunctions();
  const auto aggrFuncMapBase = exec::aggregateFunctions().copy();

  // Remove all functions and check for no entries.
  clearAndCheckRegistry();

  // Register with prefix and check all functions have the prefix.
  const std::string prefix{"test.abc_schema."};
  aggregate::prestosql::registerAllAggregateFunctions(prefix);
  exec::aggregateFunctions().withRLock([&](const auto& aggrFuncMap) {
    for (const auto& entry : aggrFuncMap) {
      EXPECT_EQ(prefix, entry.first.substr(0, prefix.size()));
      EXPECT_EQ(1, aggrFuncMapBase.count(entry.first.substr(prefix.size())));
    }
  });
}

TEST_F(AggregationFunctionRegTest, orderSensitive) {
  // Remove all functions and check for no entries.
  clearAndCheckRegistry();

  std::set<std::string> nonOrderSensitiveFunctions = {
      "sum",
      "avg",
      "min",
      "max",
      "count",
      "bool_and",
      "bool_or",
      "bitwise_and_agg",
      "bitwise_or_agg",
      "every",
      "checksum",
      "count_if",
      "geometric_mean",
      "histogram",
      "reduce_agg"};
  aggregate::prestosql::registerAllAggregateFunctions();
  for (const auto& entry : nonOrderSensitiveFunctions) {
    ASSERT_FALSE(exec::getAggregateFunctionMetadata(entry).orderSensitive);
  }

  // Test some but not all order sensitive functions
  std::set<std::string> orderSensitiveFunctions = {
      "array_agg",
      "arbitrary",
      "any_value",
      "map_agg",
      "map_union",
      "set_agg",
      "tdigest_agg"};
  for (const auto& entry : orderSensitiveFunctions) {
    ASSERT_TRUE(exec::getAggregateFunctionMetadata(entry).orderSensitive);
  }
}

TEST_F(AggregationFunctionRegTest, prestoSupportedSignatures) {
  // Remove all functions and check for no entries.
  clearAndCheckRegistry();

  // Register without prefix and all signatures
  aggregate::prestosql::registerAllAggregateFunctions("", true, false);
  auto aggrFuncMapBase = exec::aggregateFunctions().copy();
  EXPECT_EQ(aggrFuncMapBase[kBitwiseAnd].signatures.size(), 4);
  EXPECT_EQ(aggrFuncMapBase[kBitwiseOr].signatures.size(), 4);
  EXPECT_EQ(aggrFuncMapBase[kBitwiseXor].signatures.size(), 4);

  // Register without prefix and only those signatures that are supported by
  // Presto.
  clearAndCheckRegistry();
  aggregate::prestosql::registerAllAggregateFunctions("", true, true);
  aggrFuncMapBase = exec::aggregateFunctions().copy();
  EXPECT_EQ(aggrFuncMapBase[kBitwiseAnd].signatures.size(), 1);
  EXPECT_EQ(aggrFuncMapBase[kBitwiseOr].signatures.size(), 1);
  EXPECT_EQ(aggrFuncMapBase[kBitwiseXor].signatures.size(), 1);

  EXPECT_EQ(aggrFuncMapBase[kTDigestAgg].signatures.size(), 3);

  // Revert to previous state.
  clearAndCheckRegistry();
}

TEST_F(AggregationFunctionRegTest, companionFunction) {
  // Remove all functions and check for no entries.
  clearAndCheckRegistry();

  aggregate::prestosql::registerAllAggregateFunctions();
  const auto aggregates = {"approx_distinct", "count", "sum"};
  const auto companionFunctions = {
      "approx_distinct_merge", "approx_distinct_partial"};

  for (const auto& function : aggregates) {
    ASSERT_FALSE(
        exec::getAggregateFunctionMetadata(function).companionFunction);
  }
  for (const auto& function : companionFunctions) {
    ASSERT_TRUE(exec::getAggregateFunctionMetadata(function).companionFunction);
  }
}

} // namespace facebook::velox::aggregate::test

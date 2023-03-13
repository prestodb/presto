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
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

namespace facebook::velox::aggregate::test {

class AggregationFunctionRegTest : public testing::Test {};

TEST_F(AggregationFunctionRegTest, prefix) {
  // Remove all functions and check for no entries.
  exec::aggregateFunctions().clear();
  EXPECT_EQ(0, exec::aggregateFunctions().size());

  // Register without prefix and memorize function maps.
  aggregate::prestosql::registerAllAggregateFunctions();
  const auto aggrFuncMapBase = exec::aggregateFunctions();

  // Remove all functions and check for no entries.
  exec::aggregateFunctions().clear();
  EXPECT_EQ(0, exec::aggregateFunctions().size());

  // Register with prefix and check all functions have the prefix.
  const std::string prefix{"test.abc_schema."};
  aggregate::prestosql::registerAllAggregateFunctions(prefix);
  auto& aggrFuncMap = exec::aggregateFunctions();
  for (const auto& entry : aggrFuncMap) {
    EXPECT_EQ(prefix, entry.first.substr(0, prefix.size()));
    EXPECT_EQ(1, aggrFuncMapBase.count(entry.first.substr(prefix.size())));
  }
}

} // namespace facebook::velox::aggregate::test

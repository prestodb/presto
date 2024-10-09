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

#include "velox/functions/sparksql/aggregates/Register.h"

#include <gtest/gtest.h>

#include "velox/exec/tests/AggregateRegistryTestUtil.h"

using namespace facebook::velox::exec;

namespace facebook::velox::functions::aggregate::sparksql::test {

class RegisterTest : public testing::Test {
 public:
  RegisterTest() {
    registerAggregateFunc("aggregate_func");
    registerAggregateFunc("Aggregate_Func_Alias");
  }
};

TEST_F(RegisterTest, listAggregateFunctions) {
  auto functions = listAggregateFunctionNames();
  EXPECT_EQ(functions.size(), 2);
  std::sort(functions.begin(), functions.end());

  EXPECT_EQ(functions[0], "aggregate_func");
  EXPECT_EQ(functions[1], "aggregate_func_alias");
}

} // namespace facebook::velox::functions::aggregate::sparksql::test

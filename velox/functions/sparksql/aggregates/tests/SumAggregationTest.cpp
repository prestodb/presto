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

#include "velox/functions/lib/aggregates/tests/SumTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {
class SumAggregationTest : public SumTestBase {
 protected:
  void SetUp() override {
    SumTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(SumAggregationTest, overflow) {
  SumTestBase::testAggregateOverflow<int64_t, int64_t, int64_t>("spark_sum");
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test

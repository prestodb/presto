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
#include "velox/common/base/VeloxException.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

class IntegerFunctionsTest : public FunctionBaseTest {};
TEST_F(IntegerFunctionsTest, xxHash64FunctionBigInt) {
  const auto xxhash64 = [&](std::optional<int64_t> value) {
    return evaluateOnce<int64_t>("xxhash64_internal(c0)", BIGINT(), value);
  };

  EXPECT_EQ(std::nullopt, xxhash64(std::nullopt));
  EXPECT_EQ(-6977822845260490347, xxhash64(1));
  EXPECT_EQ(3803688792395291579, xxhash64(0));
  EXPECT_EQ(-5379971487550586029, xxhash64(42));
  EXPECT_EQ(6443982544387243708, xxhash64(-100));
  // int64_t max
  EXPECT_EQ(-40307683044198644, xxhash64(9223372036854775807));
  EXPECT_EQ(310259422537775556, xxhash64(-9223372036854775807));
}

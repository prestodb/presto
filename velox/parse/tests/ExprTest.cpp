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
#include <folly/container/F14Map.h>
#include <gtest/gtest.h>
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/parse/IExpr.h"

namespace facebook::velox::core {
namespace {

class ExprTest : public testing::Test {};

ExprPtr parse(const std::string& sql) {
  return duckdb::parseExpr(sql, {});
}

TEST_F(ExprTest, hashMap) {
  folly::F14FastMap<ExprPtr, int, IExprHash, IExprEqual> exprs;

  {
    auto expr1 = parse("a + 1");
    auto expr2 = parse("a + b");
    auto expr3 = parse("12");
    auto expr4 = parse("a + b");

    exprs.emplace(expr1, 1);
    exprs.emplace(expr2, 2);
    exprs.emplace(expr3, 3);
    exprs.emplace(expr4, 4);

    ASSERT_EQ(3, exprs.size());

    EXPECT_EQ(exprs.at(expr1), 1);
    EXPECT_EQ(exprs.at(expr2), 2);
    EXPECT_EQ(exprs.at(expr3), 3);
    EXPECT_EQ(exprs.at(expr4), 2);
  }

  {
    auto expr1 = parse("cast(a as bigint) + 1");
    auto expr2 = parse("cast(a as int) + 1");
    auto expr3 = parse("cast(a as bigint) + 1");

    exprs.emplace(expr1, 5);
    exprs.emplace(expr2, 6);
    exprs.emplace(expr3, 7);

    ASSERT_EQ(5, exprs.size());

    EXPECT_EQ(exprs.at(expr1), 5);
    EXPECT_EQ(exprs.at(expr2), 6);
    EXPECT_EQ(exprs.at(expr3), 5);
  }
}

} // namespace
} // namespace facebook::velox::core

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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanConsistencyChecker.h"

namespace facebook::velox::core {

namespace {
class PlanConsistencyCheckerTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TypedExprPtr Lit(Variant value) {
  auto type = value.inferType();
  return std::make_shared<ConstantTypedExpr>(std::move(type), std::move(value));
}

TypedExprPtr Col(TypePtr type, std::string name) {
  return std::make_shared<FieldAccessTypedExpr>(
      std::move(type), std::move(name));
}

TEST_F(PlanConsistencyCheckerTest, filter) {
  auto valuesNode =
      std::make_shared<ValuesNode>("0", std::vector<RowVectorPtr>{});

  auto projectNode = std::make_shared<ProjectNode>(
      "2",
      std::vector<std::string>{"a", "b", "c"},
      std::vector<TypedExprPtr>{Lit(true), Lit(1), Lit(0.1)},
      valuesNode);

  auto filterNode =
      std::make_shared<FilterNode>("1", Col(BOOLEAN(), "a"), projectNode);
  ASSERT_NO_THROW(PlanConsistencyChecker::check(filterNode));

  // Wrong type.
  filterNode =
      std::make_shared<FilterNode>("1", Col(BOOLEAN(), "b"), projectNode);

  VELOX_ASSERT_THROW(
      PlanConsistencyChecker::check(filterNode),
      "Wrong type of input column: b, BOOLEAN vs. INTEGER");

  // Wrong name.
  filterNode =
      std::make_shared<FilterNode>("1", Col(BOOLEAN(), "x"), projectNode);

  VELOX_ASSERT_THROW(
      PlanConsistencyChecker::check(filterNode), "Field not found: x");
}

TEST_F(PlanConsistencyCheckerTest, project) {
  auto valuesNode =
      std::make_shared<ValuesNode>("0", std::vector<RowVectorPtr>{});

  auto projectNode = std::make_shared<ProjectNode>(
      "2",
      std::vector<std::string>{"a", "b", "c"},
      std::vector<TypedExprPtr>{Lit(true), Lit(1), Lit(0.1)},
      valuesNode);
  ASSERT_NO_THROW(PlanConsistencyChecker::check(projectNode));

  // Duplicate output name.
  projectNode = std::make_shared<ProjectNode>(
      "2",
      std::vector<std::string>{"a", "a", "c"},
      std::vector<TypedExprPtr>{Lit(true), Lit(1), Lit(0.1)},
      valuesNode);

  VELOX_ASSERT_THROW(
      PlanConsistencyChecker::check(projectNode), "Duplicate output column: a");

  // Wrong column name.
  projectNode = std::make_shared<ProjectNode>(
      "2",
      std::vector<std::string>{"a", "a", "c"},
      std::vector<TypedExprPtr>{Lit(true), Col(REAL(), "x"), Lit(0.1)},
      valuesNode);

  VELOX_ASSERT_THROW(
      PlanConsistencyChecker::check(projectNode), "Field not found: x");
}

} // namespace
} // namespace facebook::velox::core

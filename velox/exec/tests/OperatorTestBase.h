/*
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
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/QueryAssertions.h"
#include "velox/type/Variant.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/VectorMaker.h"

namespace facebook::velox::exec::test {
class OperatorTestBase : public testing::Test {
 protected:
  OperatorTestBase();
  ~OperatorTestBase() override;

  static void SetUpTestCase();

  void createDuckDbTable(const std::vector<RowVectorPtr>& data) {
    duckDbQueryRunner_.createTable("tmp", data);
  }

  std::shared_ptr<Task> assertQueryOrdered(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::string& duckDbSql,
      const std::vector<uint32_t>& sortingKeys) {
    return test::assertQuery(plan, duckDbSql, duckDbQueryRunner_, sortingKeys);
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

  static std::shared_ptr<const RowType> makeRowType(
      std::vector<std::shared_ptr<const Type>>&& types) {
    return velox::test::VectorMaker::rowType(
        std::forward<std::vector<std::shared_ptr<const Type>>&&>(types));
  }

  RowVectorPtr makeRowVector(const std::vector<VectorPtr>& children) {
    return vectorMaker_.rowVector(children);
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*row*/)> valueAt,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    return vectorMaker_.flatVector<T>(size, valueAt, isNullAt);
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatVector(const std::vector<T>& data) {
    return vectorMaker_.flatVector<T>(data);
  }

  FlatVectorPtr<StringView> makeFlatVector(
      const std::vector<std::string>& data) {
    return vectorMaker_.flatVector(data);
  }

  template <typename T, int TupleIndex, typename TupleType>
  FlatVectorPtr<T> makeFlatVector(const std::vector<TupleType>& data) {
    return vectorMaker_.flatVector<T, TupleIndex, TupleType>(data);
  }

  template <typename T>
  FlatVectorPtr<T> makeNullableFlatVector(
      const std::vector<std::optional<T>>& data) {
    return vectorMaker_.flatVectorNullable(data);
  }

  FlatVectorPtr<StringView> makeNullableFlatVector(
      const std::vector<std::optional<std::string>>& data) {
    return vectorMaker_.flatVectorNullable(data);
  }

  template <typename T>
  ArrayVectorPtr makeArrayVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<T(vector_size_t /* row */, vector_size_t /* idx */)>
          valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr) {
    return vectorMaker_.arrayVector<T>(size, sizeAt, valueAt, isNullAt);
  }

  template <typename TKey, typename TValue>
  MapVectorPtr makeMapVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<TKey(vector_size_t /* idx */)> keyAt,
      std::function<TValue(vector_size_t /* idx */)> valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr,
      std::function<bool(vector_size_t /*row */)> valueIsNullAt = nullptr) {
    return vectorMaker_.mapVector(
        size, sizeAt, keyAt, valueAt, isNullAt, valueIsNullAt);
  }

  // Helper function for comparing vector results
  template <typename T1, typename T2>
  bool
  compareValues(const T1& a, const std::optional<T2>& b, std::string& error) {
    bool result = (a == b.value());
    if (!result) {
      error = " " + std::to_string(a) + " vs " + std::to_string(b.value());
    } else {
      error = "";
    }
    return result;
  }

  bool compareValues(
      const StringView& a,
      const std::optional<std::string>& b,
      std::string& error) {
    bool result = (a.getString() == b.value());
    if (!result) {
      error = " " + a.getString() + " vs " + b.value();
    } else {
      error = "";
    }
    return result;
  }

  static std::function<bool(vector_size_t /*row*/)> nullEvery(
      int n,
      int startingFrom = 0) {
    return velox::test::VectorMaker::nullEvery(n, startingFrom);
  }

  static std::shared_ptr<core::FieldAccessTypedExpr> toFieldExpr(
      const std::string& name,
      const std::shared_ptr<const RowType>& rowType);

  std::shared_ptr<const core::ITypedExpr> parseExpr(
      const std::string& text,
      std::shared_ptr<const RowType> rowType);

  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  DuckDbQueryRunner duckDbQueryRunner_;
  velox::test::VectorMaker vectorMaker_{pool_.get()};
};
} // namespace facebook::velox::exec::test

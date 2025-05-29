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

#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/ResultVerifier.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/QuantileDigest.h"

namespace facebook::velox::exec::test {

class QDigestAggResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return true;
  }

  bool supportsVerify() override {
    return false;
  }

  void initialize(
      const std::vector<RowVectorPtr>& /*input*/,
      const std::vector<core::ExprPtr>& /*projections*/,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    keys_ = groupingKeys;
    resultName_ = aggregateName;
    argumentTypeKind_ = aggregate.call->inputs()[0]->type()->kind();
  }

  void initializeWindow(
      const std::vector<RowVectorPtr>& /*input*/,
      const std::vector<core::ExprPtr>& /*projections*/,
      const std::vector<std::string>& /*partitionByKeys*/,
      const std::vector<SortingKeyAndOrder>& /*sortingKeysAndOrders*/,
      const core::WindowNode::Function& function,
      const std::string& /*frame*/,
      const std::string& windowName) override {
    keys_ = {"row_number"};
    resultName_ = windowName;
    argumentTypeKind_ = function.functionCall->inputs()[0]->type()->kind();
  }

  bool compare(const RowVectorPtr& result, const RowVectorPtr& altResult)
      override {
    VELOX_CHECK_EQ(result->size(), altResult->size());

    auto projection = keys_;
    projection.push_back(resultName_);

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto builder = PlanBuilder(planNodeIdGenerator).values({result});
    if (!keys_.empty()) {
      builder = builder.orderBy(keys_, false);
    }
    auto sortByKeys = builder.project(projection).planNode();
    auto sortedResult =
        AssertQueryBuilder(sortByKeys).copyResults(result->pool());

    builder = PlanBuilder(planNodeIdGenerator).values({altResult});
    if (!keys_.empty()) {
      builder = builder.orderBy(keys_, false);
    }
    sortByKeys = builder.project(projection).planNode();
    auto sortedAltResult =
        AssertQueryBuilder(sortByKeys).copyResults(altResult->pool());

    VELOX_CHECK_EQ(sortedResult->size(), sortedAltResult->size());
    auto size = sortedResult->size();
    for (auto i = 0; i < size; i++) {
      auto resultIsNull = sortedResult->childAt(resultName_)->isNullAt(i);
      auto altResultIsNull = sortedAltResult->childAt(resultName_)->isNullAt(i);
      if (resultIsNull || altResultIsNull) {
        VELOX_CHECK(resultIsNull && altResultIsNull);
        continue;
      }

      auto resultValue = sortedResult->childAt(resultName_)
                             ->as<SimpleVector<StringView>>()
                             ->valueAt(i);
      auto altResultValue = sortedAltResult->childAt(resultName_)
                                ->as<SimpleVector<StringView>>()
                                ->valueAt(i);
      if (resultValue == altResultValue) {
        continue;
      } else {
        if (argumentTypeKind_ == TypeKind::BIGINT) {
          checkEquivalentQDigest<int64_t>(resultValue, altResultValue);
        } else if (argumentTypeKind_ == TypeKind::DOUBLE) {
          checkEquivalentQDigest<double>(resultValue, altResultValue);
        } else if (argumentTypeKind_ == TypeKind::REAL) {
          checkEquivalentQDigest<float>(resultValue, altResultValue);
        } else {
          VELOX_UNSUPPORTED("Unsupported argument type");
        }
      }
    }
    return true;
  }

  bool verify(const RowVectorPtr& /*result*/) override {
    VELOX_UNSUPPORTED();
  }

  void reset() override {
    keys_.clear();
    resultName_.clear();
  }

 private:
  template <typename T>
  void checkEquivalentQDigest(
      const StringView& result,
      const StringView& altResult) {
    functions::qdigest::QuantileDigest<T, std::allocator<T>> resultDigest{
        std::allocator<T>(), result.data()};
    functions::qdigest::QuantileDigest<T, std::allocator<T>> altResultDigest{
        std::allocator<T>(), altResult.data()};

    for (auto quantile : kQuantiles) {
      variant resultValue = resultDigest.estimateQuantile(quantile);
      variant altResultValue = altResultDigest.estimateQuantile(quantile);
      VELOX_CHECK(
          resultValue.equalsWithEpsilon(altResultValue),
          "{} vs. {}",
          resultValue.value<T>(),
          altResultValue.value<T>());
    }
  }

  static constexpr double kQuantiles[] = {
      0.01,
      0.05,
      0.1,
      0.25,
      0.50,
      0.75,
      0.9,
      0.95,
      0.99,
  };

  std::vector<std::string> keys_;
  std::string resultName_;
  TypeKind argumentTypeKind_;
};

} // namespace facebook::velox::exec::test

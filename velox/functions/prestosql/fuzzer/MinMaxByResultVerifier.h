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

#include <algorithm>
#include <string>

#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/ResultVerifier.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::exec::test {

// For aggregation calls min_by/max_by(x, y) and min_by/max_by(x, y, n),
// MinMaxByResultVerifier works by computing an expected list of x-value-list
// bucketed by the y values and checking the actual result values fall in the
// expected list of buckets in order.
// For example: for SELECT min_by(x, y, 2)
//                  FROM (VALUES (2, 'b'), (1, 'b'), (3, 'a')) t(x, y),
// the result verifier computes an expected list [[3], [2, 1]]. Suppose the
// actual result is [3, 1], the verifier then checks that the result value 3
// matches the first expected bucket [3] and the result value 1 falls in the
// second expected bucket [2, 1]. The verifier reports an error if an actual
// result value is not found in the first unexhausted expected bucket.
//
// min_by/max_by(x, y) and min_by/max_by(x, y, n) can return NULL in three
// situations:
// (1) all x in the group are masked out,
// (2) all y in the group are NULL,
// (3) for min_by/max_by(x, y), one of x associated with min/max y is NULL.
class MinMaxByResultVerifier : public ResultVerifier {
 public:
  explicit MinMaxByResultVerifier(bool minBy) : minBy_{minBy} {}

  bool supportsCompare() override {
    return false;
  }

  bool supportsVerify() override {
    return true;
  }

  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override;

  bool compare(
      const RowVectorPtr& /*result*/,
      const RowVectorPtr& /*altResult*/) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override;

  void reset() override {
    expected_.reset();
    groupingKeys_.clear();
    name_.clear();
    aggregateTypeSql_.clear();
    minMaxByN_ = false;
  }

 private:
  // Returns a vector of strings that is a combination of op1 and op2. This
  // method doesn't remove duplicates.
  std::vector<std::string> combine(
      const std::vector<std::string>& op1,
      const std::vector<std::string>& op2);

  // Returns true if the array in 'vector' at 'index' contains a null.
  bool containsNull(const ArrayVector* vector, vector_size_t index);

  // Returns the index of the first element that hasn't matched before in
  // 'expectedBucketElements' at [currentBucketOffset, currentBucketOffset +
  // currentBucketSize) that equals to the value in actualElements at
  // actualIndex. Each element in expectedBucketElements can match with an
  // actual element only once, tracked by 'memo'.
  int32_t getElementIndexInBucketWithMemo(
      const VectorPtr& actualElements,
      vector_size_t actualIndex,
      const VectorPtr& expectedBucketElements,
      vector_size_t currentBucketOffset,
      vector_size_t currentBucketSize,
      SelectivityVector& memo);

  std::string extractYColumnName(
      const core::AggregationNode::Aggregate& aggregate);

  std::string makeArrayAggCall(
      const core::AggregationNode::Aggregate& aggregate);

  RowVectorPtr expected_;
  std::vector<std::string> groupingKeys_;
  std::string name_;
  std::string aggregateTypeSql_;
  bool minBy_;
  bool minMaxByN_{false};
};

} // namespace facebook::velox::exec::test

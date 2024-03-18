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

#include <string>

#include "velox/core/PlanNode.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec::test {

/// Verifies aggregation results either directly or by comparing with results
/// from a logically equivalent plan or reference DB.
///
/// Can be used to sort results of array_agg before comparing (uses 'compare'
/// API) or verify approx_distinct by comparing its results with results of
/// count(distinct) (uses 'verify' API).
class ResultVerifier {
 public:
  virtual ~ResultVerifier() = default;

  /// Returns true if 'compare' API is supported. The verifier must support
  /// either 'compare' or 'verify' API. If both are supported, 'compare' API is
  /// used and 'verify' API is ignored.
  virtual bool supportsCompare() = 0;

  /// Return true if 'verify' API is support. The verifier must support either
  /// 'compare' or 'verify' API.
  virtual bool supportsVerify() = 0;

  /// Called once before possibly multiple calls to 'compare' or 'verify' APIs
  /// to specify the input data, grouping keys (may be empty), the aggregate
  /// function and the name of the column that will store aggregate function
  /// results.
  ///
  /// Can be used by array_distinct verifier to compute count(distinct) once and
  /// re-use its results for multiple 'verify' calls.
  virtual void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) = 0;

  /// Called once on a window operation before possibly multiple calls to the
  /// 'compare' or 'verify' APIs. to specify the input data, window partition-by
  /// keys, the window function, the window frame, and the name of the column
  /// that will store the window function results.
  virtual void initializeWindow(
      const std::vector<RowVectorPtr>& /*input*/,
      const std::vector<std::string>& /*partitionByKeys*/,
      const core::WindowNode::Function& /*function*/,
      const std::string& /*frame*/,
      const std::string& /*windowName*/) {
    VELOX_NYI();
  }

  /// Compares results of two logically equivalent Velox plans or a Velox plan
  /// and a reference DB query.
  ///
  /// 'initialize' must be called first. 'compare' may be called multiple times
  /// after single 'initialize' call.
  virtual bool compare(
      const RowVectorPtr& result,
      const RowVectorPtr& otherResult) = 0;

  /// Verifies results of a Velox plan or reference DB query.
  ///
  /// 'initialize' must be called first. 'verify' may be called multiple times
  /// after single 'initialize' call.
  virtual bool verify(const RowVectorPtr& result) = 0;

  /// Clears internal state after possibly multiple calls to 'compare' and
  /// 'verify'. 'initialize' must be called again after 'reset' to allow calling
  /// 'compare' or 'verify' again.
  virtual void reset() = 0;
};

} // namespace facebook::velox::exec::test

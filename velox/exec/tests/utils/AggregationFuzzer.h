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

#include "velox/exec/Aggregate.h"
#include "velox/exec/tests/utils/ReferenceQueryRunner.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

static constexpr const std::string_view kPlanNodeFileName = "plan_nodes";

class InputGenerator {
 public:
  virtual ~InputGenerator() = default;

  /// Generates function inputs of the specified types. May return an empty list
  /// to let Fuzzer use randomly-generated inputs.
  virtual std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) = 0;

  /// Called after generating all inputs for a given fuzzer iteration.
  virtual void reset() = 0;
};

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

/// Runs the aggregation fuzzer.
/// @param signatureMap Map of all aggregate function signatures.
/// @param seed Random seed - Pass the same seed for reproducibility.
/// @param orderDependentFunctions Map of functions that depend on order of
/// input.
/// @param planPath Path to persisted plan information. If this is
/// supplied, fuzzer will only verify the plans.
/// @param referenceQueryRunner Reference query runner for results
/// verification.
void aggregateFuzzer(
    AggregateFunctionSignatureMap signatureMap,
    size_t seed,
    const std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>&
        orderDependentFunctions,
    const std::unordered_map<std::string, std::shared_ptr<InputGenerator>>&
        customInputGenerators,
    VectorFuzzer::Options::TimestampPrecision timestampPrecision,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    const std::optional<std::string>& planPath,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);
} // namespace facebook::velox::exec::test

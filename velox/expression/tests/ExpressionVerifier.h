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

#include "velox/core/ITypedExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::test {

using exec::test::ReferenceQueryRunner;
using facebook::velox::fuzzer::InputRowMetadata;

struct ExpressionVerifierOptions {
  bool disableConstantFolding{false};
  std::string reproPersistPath;
  bool persistAndRunOnce{false};
};

class ExpressionVerifier {
 public:
  // File names used to persist data required for reproducing a failed test
  // case.
  static constexpr const std::string_view kInputVectorFileNamePrefix =
      "input_vector";
  static constexpr const std::string_view
      kInputSelectivityVectorFileNamePrefix = "input_selectivity_vector";
  static constexpr const std::string_view kInputRowMetadataFileName =
      "input_row_metadata";
  static constexpr const std::string_view kResultVectorFileName =
      "result_vector";
  static constexpr const std::string_view kExpressionSqlFileName = "sql";
  static constexpr const std::string_view kComplexConstantsFileName =
      "complex_constants";

  ExpressionVerifier(
      core::ExecCtx* execCtx,
      ExpressionVerifierOptions options,
      std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner)
      : execCtx_(execCtx),
        options_(options),
        referenceQueryRunner_{referenceQueryRunner} {}

  enum class VerificationState {
    kVerifiedAgainstReference = 0,
    kBothPathsThrow = 1,
    kReferencePathUnsupported = 2,
  };
  // Executes expressions using common path (all evaluation
  // optimizations) and compares the result with either the simplified path or a
  // reference query runner. This execution is done for each input test cases
  // where the ExprSet for the common and simplified is reused between test
  // cases to simulate batches being processed via a ProjectFilter operator.
  // Returns:
  // A vector of ResultOrError objects, one for each InputTestCase. Each result
  // contains:
  //  - result of evaluating the expressions if both paths succeeded and
  //  returned the exact same vectors.
  //  - exception thrown by the common path if both paths failed with compatible
  //  exceptions. Throws otherwise (incompatible exceptions or different
  //  results).
  //  - a verification state indicating if the result was verified against the
  //  reference DB.
  std::pair<std::vector<fuzzer::ResultOrError>, std::vector<VerificationState>>
  verify(
      const std::vector<core::TypedExprPtr>& plans,
      const std::vector<fuzzer::InputTestCase>& inputTestCases,
      VectorPtr&& resultVector,
      bool canThrow,
      const InputRowMetadata& inputRowMetadata = {});

 private:
  // Utility method used to serialize the relevant data required to repro a
  // crash.
  void persistReproInfo(
      const std::vector<fuzzer::InputTestCase>& inputTestCases,
      const InputRowMetadata& inputRowMetadata,
      const VectorPtr& resultVector,
      const std::string& sql,
      const std::vector<VectorPtr>& complexConstants);

  // Utility method that calls persistReproInfo to save data and sql if
  // options_.reproPersistPath is set and is not persistAndRunOnce. Do nothing
  // otherwise.
  void persistReproInfoIfNeeded(
      const std::vector<fuzzer::InputTestCase>& inputTestCases,
      const InputRowMetadata& inputRowMetadata,
      const VectorPtr& resultVector,
      const std::string& sql,
      const std::vector<VectorPtr>& complexConstants);

 private:
  core::ExecCtx* execCtx_;
  const ExpressionVerifierOptions options_;

  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner_;
};

// Finds the minimum common subexpression which fails for a plan should it
// exist.
void computeMinimumSubExpression(
    ExpressionVerifier&& minimalVerifier,
    VectorFuzzer& fuzzer,
    const std::vector<core::TypedExprPtr>& plans,
    const std::vector<fuzzer::InputTestCase>& inputTestCases,
    const InputRowMetadata& inputRowMetadata);
} // namespace facebook::velox::test

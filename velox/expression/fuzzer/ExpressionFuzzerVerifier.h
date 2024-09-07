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
#include "velox/expression/Expr.h"
#include "velox/expression/fuzzer/ExpressionFuzzer.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DECLARE_int32(velox_fuzzer_max_level_of_nesting);

namespace facebook::velox::fuzzer {

// A tool that utilizes ExpressionFuzzer, VectorFuzzer and ExpressionVerfier to
// generate random expressions and verify the correctness of the results. It
// works by:
///
///  1. Taking an initial set of available function signatures.
///  2. Generating a random expression tree based on the available function
///     signatures.
///  3. Generating a random set of input data (vector), with a variety of
///     encodings and data layouts.
///  4. Executing the expression using the common and simplified eval paths, and
///     asserting results are the exact same.
///  5. Rinse and repeat.
///

class ExpressionFuzzerVerifier {
 public:
  struct Options;

  ExpressionFuzzerVerifier(
      const FunctionSignatureMap& signatureMap,
      size_t initialSeed,
      const Options& options,
      const std::unordered_map<std::string, std::shared_ptr<ArgGenerator>>&
          argGenerators);

  // This function starts the test that is performed by the
  // ExpressionFuzzerVerifier which is generating random expressions and
  // verifying them.
  void go();

  struct Options {
    // Number of expressions to generate and execute.
    int32_t steps = 10;

    // For how long it should run (in seconds). If zero it executes exactly
    // --steps iterations and exits.
    int32_t durationSeconds = 0;

    // The number of elements on each generated vector.
    int32_t batchSize = 100;

    // Retry failed expressions by wrapping it using a try() statement.
    bool retryWithTry = false;

    // Automatically seeks minimum failed subexpression on result mismatch
    bool findMinimalSubexpression = false;

    // Disable constant folding in the common evaluation path.
    bool disableConstantFolding = false;

    // Directory path for persistence of data and SQL when fuzzer fails for
    // future reproduction. Empty string disables this feature.
    std::string reproPersistPath = "";

    // Persist repro info before evaluation and only run one iteration.
    // This is to rerun with the seed number and persist repro info upon a
    // crash failure. Only effective if repro_persist_path is set.
    bool persistAndRunOnce = false;

    // Specifies the probability with which columns in the input row vector will
    // be selected to be wrapped in lazy encoding (expressed as double from 0 to
    // 1).
    double lazyVectorGenerationRatio = 0.0;

    // This sets an upper limit on the number of expression trees to generate
    // per step. These trees would be executed in the same ExprSet and can
    // re-use already generated columns and subexpressions (if re-use is
    // enabled).
    int32_t maxExpressionTreesPerStep = 1;

    VectorFuzzer::Options vectorFuzzerOptions;

    ExpressionFuzzer::Options expressionFuzzerOptions;

    std::unordered_map<std::string, std::string> queryConfigs;
  };

 private:
  struct ExprUsageStats {
    // Num of times the expression was randomly selected.
    int numTimesSelected = 0;
    // Num of rows processed by the expression.
    int numProcessedRows = 0;
  };

  void seed(size_t seed);

  void reSeed();

  // A utility class used to keep track of stats relevant to the fuzzer.
  class ExprStatsListener : public exec::ExprSetListener {
   public:
    explicit ExprStatsListener(
        std::unordered_map<std::string, ExprUsageStats>& exprNameToStats)
        : exprNameToStats_(exprNameToStats) {}

    void onCompletion(
        const std::string& /*uuid*/,
        const exec::ExprSetCompletionEvent& event) override {
      for (auto& [funcName, stats] : event.stats) {
        auto itr = exprNameToStats_.find(funcName);
        if (itr == exprNameToStats_.end()) {
          // Skip expressions like FieldReference and ConstantExpr
          continue;
        }
        itr->second.numProcessedRows += stats.numProcessedRows;
      }
    }

    // A no-op since we cannot tie errors directly to functions where they
    // occurred.
    void onError(vector_size_t /*numRows*/, const std::string& /*queryId*/)
        override {}

   private:
    std::unordered_map<std::string, ExprUsageStats>& exprNameToStats_;
  };

  /// Randomize initial result vector data to test for correct null and data
  /// setting in functions.
  RowVectorPtr generateResultVectors(std::vector<core::TypedExprPtr>& plans);

  /// Executes two steps:
  /// #1. Retries executing the expression in `plan` by wrapping it in a `try()`
  ///     clause and expecting it not to throw an exception.
  /// #2. Re-execute the expression only on rows that produced non-NULL values
  ///     in the previous step.
  ///
  /// Throws in case any of these steps fail.
  void retryWithTry(
      std::vector<core::TypedExprPtr> plans,
      const RowVectorPtr& rowVector,
      const VectorPtr& resultVectors,
      const std::vector<int>& columnsToWrapInLazy);

  /// If --duration_sec > 0, check if we expired the time budget. Otherwise,
  /// check if we expired the number of iterations (--steps).
  template <typename T>
  bool isDone(size_t i, T startTime) const;

  /// Called at the end of a successful fuzzer run. It logs the top and bottom
  /// 10 functions based on the num of rows processed by them. Also logs a full
  /// list of all functions sorted in descending order by the num of times they
  /// were selected by the fuzzer. Every logged function contains the
  /// information in the following format which can be easily exported to a
  /// spreadsheet for further analysis: functionName numTimesSelected
  /// proportionOfTimesSelected numProcessedRows.
  void logStats();

  // Randomly pick columns from the input row vector to wrap in lazy.
  // Negative column indices represent lazy vectors that have been preloaded
  // before feeding them to the evaluator. This list is sorted on the absolute
  // value of the entries.
  std::vector<int> generateLazyColumnIds(
      const RowVectorPtr& rowVector,
      VectorFuzzer& vectorFuzzer);

  const Options options_;

  FuzzerGenerator rng_;

  size_t currentSeed_{0};

  std::shared_ptr<core::QueryCtx> queryCtx_;

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::deprecatedAddDefaultLeafMemoryPool()};

  core::ExecCtx execCtx_;

  test::ExpressionVerifier verifier_;

  std::shared_ptr<VectorFuzzer> vectorFuzzer_;

  std::shared_ptr<ExprStatsListener> statListener_;

  std::unordered_map<std::string, ExprUsageStats> exprNameToStats_;

  /// The expression fuzzer that is used to fuzz the expression in the test.
  ExpressionFuzzer expressionFuzzer_;
};

} // namespace facebook::velox::fuzzer

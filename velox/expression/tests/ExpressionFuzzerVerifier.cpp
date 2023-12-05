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

#include "velox/expression/tests/ExpressionFuzzerVerifier.h"

#include <boost/random/uniform_int_distribution.hpp>
#include <glog/logging.h>
#include <exception>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/ReverseSignatureBinder.h"
#include "velox/expression/tests/ExpressionFuzzer.h"

DEFINE_int32(steps, 10, "Number of expressions to generate and execute.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_bool(
    retry_with_try,
    false,
    "Retry failed expressions by wrapping it using a try() statement.");

DEFINE_bool(
    find_minimal_subexpression,
    false,
    "Automatically seeks minimum failed subexpression on result mismatch");

DEFINE_bool(
    disable_constant_folding,
    false,
    "Disable constant-folding in the common evaluation path.");

DEFINE_string(
    repro_persist_path,
    "",
    "Directory path for persistence of data and SQL when fuzzer fails for "
    "future reproduction. Empty string disables this feature.");

DEFINE_bool(
    persist_and_run_once,
    false,
    "Persist repro info before evaluation and only run one iteration. "
    "This is to rerun with the seed number and persist repro info upon a "
    "crash failure. Only effective if repro_persist_path is set.");

DEFINE_double(
    lazy_vector_generation_ratio,
    0.0,
    "Specifies the probability with which columns in the input row "
    "vector will be selected to be wrapped in lazy encoding "
    "(expressed as double from 0 to 1).");

DEFINE_int32(
    max_expression_trees_per_step,
    1,
    "This sets an upper limit on the number of expression trees to generate "
    "per step. These trees would be executed in the same ExprSet and can "
    "re-use already generated columns and subexpressions (if re-use is "
    "enabled).");

// The flags bellow are used to initialize ExpressionFuzzer::options.
DEFINE_int32(
    velox_fuzzer_max_level_of_nesting,
    10,
    "Max levels of expression nesting. The default value is 10 and minimum is 1.");

DEFINE_int32(
    max_num_varargs,
    5,
    "The maximum number of variadic arguments fuzzer will generate for "
    "functions that accept variadic arguments. Fuzzer will generate up to "
    "max_num_varargs arguments for the variadic list in addition to the "
    "required arguments by the function.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null constant to the plan, or null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_bool(
    enable_variadic_signatures,
    false,
    "Enable testing of function signatures with variadic arguments.");

DEFINE_bool(
    enable_dereference,
    false,
    "Allow fuzzer to generate random expressions with dereference and row_constructor functions.");

DEFINE_bool(
    velox_fuzzer_enable_complex_types,
    false,
    "Enable testing of function signatures with complex argument or return types.");

DEFINE_bool(
    velox_fuzzer_enable_column_reuse,
    false,
    "Enable generation of expressions where one input column can be "
    "used by multiple subexpressions");

DEFINE_bool(
    velox_fuzzer_enable_expression_reuse,
    false,
    "Enable generation of expressions that re-uses already generated "
    "subexpressions.");

DEFINE_string(
    assign_function_tickets,
    "",
    "Comma separated list of function names and their tickets in the format "
    "<function_name>=<tickets>. Every ticket represents an opportunity for "
    "a function to be chosen from a pool of candidates. By default, "
    "every function has one ticket, and the likelihood of a function "
    "being picked can be increased by allotting it more tickets. Note "
    "that in practice, increasing the number of tickets does not "
    "proportionally increase the likelihood of selection, as the selection "
    "process involves filtering the pool of candidates by a required "
    "return type so not all functions may compete against the same number "
    "of functions at every instance. Number of tickets must be a positive "
    "integer. Example: eq=3,floor=5");

namespace facebook::velox::test {

namespace {

using exec::SignatureBinder;

VectorFuzzer::Options getVectorFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_batch_size;
  opts.stringVariableLength = true;
  opts.stringLength = 100;
  opts.nullRatio = FLAGS_null_ratio;
  return opts;
}

ExpressionFuzzer::Options getExpressionFuzzerOptions() {
  ExpressionFuzzer::Options opts;
  opts.maxLevelOfNesting = FLAGS_velox_fuzzer_max_level_of_nesting;
  opts.maxNumVarArgs = FLAGS_max_num_varargs;
  opts.enableVariadicSignatures = FLAGS_enable_variadic_signatures;
  opts.enableDereference = FLAGS_enable_dereference;
  opts.enableComplexTypes = FLAGS_velox_fuzzer_enable_complex_types;
  opts.enableColumnReuse = FLAGS_velox_fuzzer_enable_column_reuse;
  opts.enableExpressionReuse = FLAGS_velox_fuzzer_enable_expression_reuse;
  opts.functionTickets = FLAGS_assign_function_tickets;
  opts.nullRatio = FLAGS_null_ratio;
  return opts;
}

// Randomly pick columns from the input row vector to wrap in lazy.
// Negative column indices represent lazy vectors that have been preloaded
// before feeding them to the evaluator. This list is sorted on the absolute
// value of the entries.
std::vector<int> generateLazyColumnIds(
    const RowVectorPtr& rowVector,
    VectorFuzzer& vectorFuzzer) {
  std::vector<int> columnsToWrapInLazy;
  if (FLAGS_lazy_vector_generation_ratio > 0) {
    for (int idx = 0; idx < rowVector->childrenSize(); idx++) {
      VELOX_CHECK_NOT_NULL(rowVector->childAt(idx));
      if (vectorFuzzer.coinToss(FLAGS_lazy_vector_generation_ratio)) {
        columnsToWrapInLazy.push_back(
            vectorFuzzer.coinToss(0.8) ? idx : -1 * idx);
      }
    }
  }
  return columnsToWrapInLazy;
}

/// Returns row numbers for non-null rows among all children in'data' or null
/// if all rows are null.
BufferPtr extractNonNullIndices(const RowVectorPtr& data) {
  DecodedVector decoded;
  SelectivityVector nonNullRows(data->size());

  for (auto& child : data->children()) {
    decoded.decode(*child);
    auto* rawNulls = decoded.nulls();
    if (rawNulls) {
      nonNullRows.deselectNulls(rawNulls, 0, data->size());
    }
    if (!nonNullRows.hasSelections()) {
      return nullptr;
    }
  }

  BufferPtr indices = allocateIndices(nonNullRows.end(), data->pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t cnt = 0;
  nonNullRows.applyToSelected(
      [&](vector_size_t row) { rawIndices[cnt++] = row; });
  VELOX_CHECK_GT(cnt, 0);
  indices->setSize(cnt * sizeof(vector_size_t));
  return indices;
}

/// Wraps child vectors of the specified 'rowVector' in dictionary using
/// specified 'indices'. Returns new RowVector created from the wrapped vectors.
RowVectorPtr wrapChildren(
    const BufferPtr& indices,
    const RowVectorPtr& rowVector) {
  auto size = indices->size() / sizeof(vector_size_t);

  std::vector<VectorPtr> newInputs;
  for (const auto& child : rowVector->children()) {
    newInputs.push_back(
        BaseVector::wrapInDictionary(nullptr, indices, size, child));
  }

  return std::make_shared<RowVector>(
      rowVector->pool(), rowVector->type(), nullptr, size, newInputs);
}
} // namespace

ExpressionFuzzerVerifier::ExpressionFuzzerVerifier(
    const FunctionSignatureMap& signatureMap,
    size_t initialSeed)
    : verifier_(
          &execCtx_,
          {FLAGS_disable_constant_folding,
           FLAGS_repro_persist_path,
           FLAGS_persist_and_run_once}),
      vectorFuzzer_(std::make_shared<VectorFuzzer>(
          getVectorFuzzerOptions(),
          execCtx_.pool())),
      expressionFuzzer_(
          signatureMap,
          initialSeed,
          vectorFuzzer_,
          getExpressionFuzzerOptions()) {
  seed(initialSeed);

  // Init stats and register listener.
  for (auto& name : expressionFuzzer_.supportedFunctions()) {
    exprNameToStats_.insert({name, ExprUsageStats()});
  }
  statListener_ = std::make_shared<ExprStatsListener>(exprNameToStats_);
  if (!exec::registerExprSetListener(statListener_)) {
    LOG(WARNING) << "Listener should only be registered once.";
  }
}

void ExpressionFuzzerVerifier::reSeed() {
  seed(rng_());
}

void ExpressionFuzzerVerifier::seed(size_t seed) {
  currentSeed_ = seed;
  expressionFuzzer_.seed(currentSeed_);
  vectorFuzzer_->reSeed(currentSeed_);
  rng_.seed(currentSeed_);
}

template <typename T>
bool ExpressionFuzzerVerifier::isDone(size_t i, T startTime) const {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

void ExpressionFuzzerVerifier::logStats() {
  std::vector<std::pair<std::string, ExprUsageStats>> entries;
  uint64_t totalSelections = 0;
  for (auto& elem : exprNameToStats_) {
    totalSelections += elem.second.numTimesSelected;
    entries.push_back(elem);
  }

  // sort by numProcessedRows
  std::sort(entries.begin(), entries.end(), [](auto& left, auto& right) {
    return left.second.numProcessedRows > right.second.numProcessedRows;
  });
  int maxEntriesLimit = std::min<size_t>(10, entries.size());
  LOG(INFO) << "==============================> Top " << maxEntriesLimit
            << " by number of rows processed";
  LOG(INFO)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (int i = 0; i < maxEntriesLimit; i++) {
    LOG(INFO) << entries[i].first << " " << entries[i].second.numTimesSelected
              << " " << std::fixed << std::setprecision(2)
              << (entries[i].second.numTimesSelected * 100.00) / totalSelections
              << "% " << entries[i].second.numProcessedRows;
  }

  LOG(INFO) << "==============================> Bottom " << maxEntriesLimit
            << " by number of rows processed";
  LOG(INFO)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (int i = 0; i < maxEntriesLimit; i++) {
    int idx = entries.size() - 1 - i;
    LOG(INFO) << entries[idx].first << " "
              << entries[idx].second.numTimesSelected << " " << std::fixed
              << std::setprecision(2)
              << (entries[idx].second.numTimesSelected * 100.00) /
            totalSelections
              << "% " << entries[idx].second.numProcessedRows;
  }

  // sort by numTimesSelected
  std::sort(entries.begin(), entries.end(), [](auto& left, auto& right) {
    return left.second.numTimesSelected > right.second.numTimesSelected;
  });

  LOG(INFO) << "==============================> All stats sorted by number "
               "of times the function was chosen";
  LOG(INFO)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (auto& elem : entries) {
    LOG(INFO) << elem.first << " " << elem.second.numTimesSelected << " "
              << std::fixed << std::setprecision(2)
              << (elem.second.numTimesSelected * 100.00) / totalSelections
              << "% " << elem.second.numProcessedRows;
  }
}

// Generates a row vector with child vectors corresponding to the same type as
// the return type of the expression trees in 'plans'. These are used as
// pre-allocated result vectors to be passed during expression evaluation.
RowVectorPtr ExpressionFuzzerVerifier::generateResultVectors(
    std::vector<core::TypedExprPtr>& plans) {
  std::vector<VectorPtr> results;
  std::vector<std::shared_ptr<const Type>> resultTypes;
  size_t vectorSize = vectorFuzzer_->getOptions().vectorSize;
  for (auto& plan : plans) {
    results.push_back(
        vectorFuzzer_->coinToss(0.5) ? vectorFuzzer_->fuzzFlat(plan->type())
                                     : nullptr);
    resultTypes.push_back(plan->type());
  }
  auto rowType = ROW(std::move(resultTypes));
  return std::make_shared<RowVector>(
      execCtx_.pool(), rowType, BufferPtr(nullptr), vectorSize, results);
}

void ExpressionFuzzerVerifier::retryWithTry(
    std::vector<core::TypedExprPtr> plans,
    const RowVectorPtr& rowVector,
    const VectorPtr& resultVector,
    const std::vector<int>& columnsToWrapInLazy) {
  // Wrap each expression tree with 'try'.
  std::vector<core::TypedExprPtr> tryPlans;
  for (auto& plan : plans) {
    tryPlans.push_back(std::make_shared<core::CallTypedExpr>(
        plan->type(), std::vector<core::TypedExprPtr>{plan}, "try"));
  }

  RowVectorPtr tryResult;

  // The function throws if anything goes wrong.
  try {
    tryResult =
        verifier_
            .verify(
                tryPlans,
                rowVector,
                resultVector ? BaseVector::copy(*resultVector) : nullptr,
                false, // canThrow
                columnsToWrapInLazy)
            .result;
  } catch (const std::exception& e) {
    if (FLAGS_find_minimal_subexpression) {
      computeMinimumSubExpression(
          {&execCtx_, {false, ""}},
          *vectorFuzzer_,
          plans,
          rowVector,
          columnsToWrapInLazy);
    }
    throw;
  }

  // Re-evaluate the original expression on rows that didn't produce an
  // error (i.e. returned non-NULL results when evaluated with TRY).
  BufferPtr noErrorIndices = extractNonNullIndices(tryResult);

  if (noErrorIndices != nullptr) {
    auto noErrorRowVector = wrapChildren(noErrorIndices, rowVector);

    LOG(INFO) << "Retrying original expression on " << noErrorRowVector->size()
              << " rows without errors";

    try {
      verifier_.verify(
          plans,
          noErrorRowVector,
          resultVector ? BaseVector::copy(*resultVector)
                             ->slice(0, noErrorRowVector->size())
                       : nullptr,
          false, // canThrow
          columnsToWrapInLazy);
    } catch (const std::exception& e) {
      if (FLAGS_find_minimal_subexpression) {
        computeMinimumSubExpression(
            {&execCtx_, {false, ""}},
            *vectorFuzzer_,
            plans,
            noErrorRowVector,
            columnsToWrapInLazy);
      }
      throw;
    }
  }
}

void ExpressionFuzzerVerifier::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.")
  VELOX_CHECK_GT(
      FLAGS_max_expression_trees_per_step,
      0,
      "--max_expression_trees_per_step needs to be greater than zero.")

  auto startTime = std::chrono::system_clock::now();
  size_t i = 0;
  size_t numFailed = 0;

  while (!isDone(i, startTime)) {
    LOG(INFO) << "==============================> Started iteration " << i
              << " (seed: " << currentSeed_ << ")";

    // Generate multiple expression trees and input data vectors. They can
    // re-use columns and share sub-expressions if the appropriate flag is set.
    int numExpressionTrees = boost::random::uniform_int_distribution<int>(
        1, FLAGS_max_expression_trees_per_step)(rng_);
    auto [expressions, inputType, selectionStats] =
        expressionFuzzer_.fuzzExpressions(numExpressionTrees);

    for (auto& [funcName, count] : selectionStats) {
      exprNameToStats_[funcName].numTimesSelected += count;
    }

    std::vector<core::TypedExprPtr> plans = std::move(expressions);

    auto rowVector = vectorFuzzer_->fuzzInputRow(inputType);

    auto columnsToWrapInLazy = generateLazyColumnIds(rowVector, *vectorFuzzer_);

    auto resultVectors = generateResultVectors(plans);
    ResultOrError result;

    try {
      result = verifier_.verify(
          plans,
          rowVector,
          resultVectors ? BaseVector::copy(*resultVectors) : nullptr,
          true, // canThrow
          columnsToWrapInLazy);
    } catch (const std::exception& e) {
      if (FLAGS_find_minimal_subexpression) {
        computeMinimumSubExpression(
            {&execCtx_, {false, ""}},
            *vectorFuzzer_,
            plans,
            rowVector,
            columnsToWrapInLazy);
      }
      throw;
    }

    if (result.exceptionPtr) {
      ++numFailed;
    }

    // If both paths threw compatible exceptions, we add a try() function to
    // the expression's root and execute it again. This time the expression
    // cannot throw.
    if (result.exceptionPtr && FLAGS_retry_with_try) {
      LOG(INFO)
          << "Both paths failed with compatible exceptions. Retrying expression using try().";
      retryWithTry(plans, rowVector, resultVectors, columnsToWrapInLazy);
    }

    LOG(INFO) << "==============================> Done with iteration " << i;
    reSeed();
    ++i;
  }
  logStats();

  LOG(ERROR) << "Total iterations: " << i;
  LOG(ERROR) << "Total failed: " << numFailed;
}

} // namespace facebook::velox::test

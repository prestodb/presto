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

#include "velox/expression/fuzzer/ExpressionFuzzerVerifier.h"

#include <boost/random/uniform_int_distribution.hpp>
#include <glog/logging.h>
#include <exception>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/ReverseSignatureBinder.h"
#include "velox/expression/fuzzer/ExpressionFuzzer.h"

namespace facebook::velox::fuzzer {

namespace {

using exec::SignatureBinder;

/// Returns row numbers for non-null rows among all children in'data' or null
/// if all rows are null.
BufferPtr extractNonNullIndices(const RowVectorPtr& data) {
  DecodedVector decoded;
  SelectivityVector nonNullRows(data->size());

  for (auto& child : data->children()) {
    decoded.decode(*child);
    auto* rawNulls = decoded.nulls(nullptr);
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
    size_t initialSeed,
    const ExpressionFuzzerVerifier::Options& options,
    const std::unordered_map<std::string, std::shared_ptr<ArgGenerator>>&
        argGenerators)
    : options_(options),
      queryCtx_(core::QueryCtx::create(
          nullptr,
          core::QueryConfig(options_.queryConfigs))),
      execCtx_({pool_.get(), queryCtx_.get()}),
      verifier_(
          &execCtx_,
          {options_.disableConstantFolding,
           options_.reproPersistPath,
           options_.persistAndRunOnce},
          options_.expressionFuzzerOptions.referenceQueryRunner),
      vectorFuzzer_(std::make_shared<VectorFuzzer>(
          options_.vectorFuzzerOptions,
          execCtx_.pool())),
      expressionFuzzer_(
          signatureMap,
          initialSeed,
          vectorFuzzer_,
          options_.expressionFuzzerOptions,
          argGenerators),
      referenceQueryRunner_{
          options_.expressionFuzzerOptions.referenceQueryRunner} {
  filesystems::registerLocalFileSystem();
  exec::test::registerHiveConnector({});

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

std::vector<int> ExpressionFuzzerVerifier::generateLazyColumnIds(
    const RowVectorPtr& rowVector,
    VectorFuzzer& vectorFuzzer) {
  std::vector<int> columnsToWrapInLazy;
  if (options_.lazyVectorGenerationRatio > 0) {
    for (int idx = 0; idx < rowVector->childrenSize(); idx++) {
      VELOX_CHECK_NOT_NULL(rowVector->childAt(idx));
      if (vectorFuzzer.coinToss(options_.lazyVectorGenerationRatio)) {
        columnsToWrapInLazy.push_back(
            vectorFuzzer.coinToss(0.8) ? idx : -1 * idx);
      }
    }
  }
  return columnsToWrapInLazy;
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
  if (options_.durationSeconds > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= options_.durationSeconds;
  }
  return i >= options_.steps;
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

  ResultOrError tryResult;

  // The function throws if anything goes wrong except
  // UNSUPPORTED_INPUT_UNCATCHABLE errors.
  try {
    tryResult = verifier_.verify(
        tryPlans,
        rowVector,
        resultVector ? BaseVector::copy(*resultVector) : nullptr,
        false, // canThrow
        columnsToWrapInLazy);
  } catch (const std::exception&) {
    if (options_.findMinimalSubexpression) {
      test::computeMinimumSubExpression(
          {&execCtx_, {false, ""}, referenceQueryRunner_},
          *vectorFuzzer_,
          plans,
          rowVector,
          columnsToWrapInLazy);
    }
    throw;
  }
  if (tryResult.unsupportedInputUncatchableError) {
    LOG(INFO)
        << "Retry with try fails to find minimal subexpression due to UNSUPPORTED_INPUT_UNCATCHABLE error.";
    return;
  }

  // Re-evaluate the original expression on rows that didn't produce an
  // error (i.e. returned non-NULL results when evaluated with TRY).
  BufferPtr noErrorIndices = extractNonNullIndices(tryResult.result);

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
    } catch (const std::exception&) {
      if (options_.findMinimalSubexpression) {
        test::computeMinimumSubExpression(
            {&execCtx_, {false, ""}, referenceQueryRunner_},
            *vectorFuzzer_,
            plans,
            noErrorRowVector,
            columnsToWrapInLazy);
      }
      throw;
    }
  }
}

RowVectorPtr ExpressionFuzzerVerifier::fuzzInputWithRowNumber(
    VectorFuzzer& fuzzer,
    const RowTypePtr& type) {
  auto rowVector = fuzzer.fuzzInputRow(type);
  auto names = type->names();
  names.push_back("row_number");

  auto& children = rowVector->children();
  velox::test::VectorMaker vectorMaker{pool_.get()};
  children.push_back(vectorMaker.flatVector<int64_t>(
      rowVector->size(), [&](auto row) { return row; }));

  return vectorMaker.rowVector(names, children);
}

void ExpressionFuzzerVerifier::go() {
  VELOX_CHECK(
      options_.steps > 0 || options_.durationSeconds > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");
  VELOX_CHECK_GT(
      options_.maxExpressionTreesPerStep,
      0,
      "--max_expression_trees_per_step needs to be greater than zero.");

  auto startTime = std::chrono::system_clock::now();
  size_t i = 0;
  size_t numFailed = 0;

  // TODO: some expression will throw exception for NaN input, eg: IN predicate
  // for floating point. remove this constraint once that are fixed
  auto vectorOptions = vectorFuzzer_->getOptions();
  vectorOptions.dataSpec = {false, false};
  vectorFuzzer_->setOptions(vectorOptions);
  while (!isDone(i, startTime)) {
    LOG(INFO) << "==============================> Started iteration " << i
              << " (seed: " << currentSeed_ << ")";

    // Generate multiple expression trees and input data vectors. They can
    // re-use columns and share sub-expressions if the appropriate flag is set.
    int numExpressionTrees = boost::random::uniform_int_distribution<int>(
        1, options_.maxExpressionTreesPerStep)(rng_);
    auto [expressions, inputType, selectionStats] =
        expressionFuzzer_.fuzzExpressions(numExpressionTrees);
    // Project a row number column in the output to enable epsilon-comparison
    // for floating-point columns and make investigation of failures easier.
    expressions.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "row_number"));

    for (auto& [funcName, count] : selectionStats) {
      exprNameToStats_[funcName].numTimesSelected += count;
    }

    std::vector<core::TypedExprPtr> plans = std::move(expressions);

    auto rowVector = fuzzInputWithRowNumber(*vectorFuzzer_, inputType);

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
    } catch (const std::exception&) {
      if (options_.findMinimalSubexpression) {
        test::computeMinimumSubExpression(
            {&execCtx_, {false, ""}, referenceQueryRunner_},
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
    // cannot throw. Expressions that throw UNSUPPORTED_INPUT_UNCATCHABLE errors
    // are not supported.
    if (result.exceptionPtr && options_.retryWithTry &&
        !result.unsupportedInputUncatchableError) {
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

} // namespace facebook::velox::fuzzer

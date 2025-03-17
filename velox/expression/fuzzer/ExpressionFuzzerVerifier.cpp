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
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/ReverseSignatureBinder.h"
#include "velox/expression/fuzzer/ExpressionFuzzer.h"

namespace facebook::velox::fuzzer {

namespace {

using exec::SignatureBinder;

/// Returns all non-null rows among all children in 'data'.
SelectivityVector extractNonNullRows(const RowVectorPtr& data) {
  DecodedVector decoded;
  SelectivityVector nonNullRows(data->size());

  for (auto& child : data->children()) {
    decoded.decode(*child);
    auto* rawNulls = decoded.nulls(nullptr);
    if (rawNulls) {
      nonNullRows.deselectNulls(rawNulls, 0, data->size());
    }
  }
  return nonNullRows;
}
} // namespace

ExpressionFuzzerVerifier::ExpressionFuzzerVerifier(
    const FunctionSignatureMap& signatureMap,
    size_t initialSeed,
    const ExpressionFuzzerVerifier::Options& options,
    const std::unordered_map<std::string, std::shared_ptr<ArgTypesGenerator>>&
        argTypesGenerators,
    const std::unordered_map<std::string, std::shared_ptr<ArgValuesGenerator>>&
        argValuesGenerators)
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
          argTypesGenerators,
          argValuesGenerators),
      referenceQueryRunner_{
          options_.expressionFuzzerOptions.referenceQueryRunner} {
  filesystems::registerLocalFileSystem();
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  exec::test::registerHiveConnector({});
  dwrf::registerDwrfWriterFactory();

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

std::pair<std::vector<InputTestCase>, InputRowMetadata>
ExpressionFuzzerVerifier::generateInput(
    const RowTypePtr& rowType,
    VectorFuzzer& vectorFuzzer,
    const std::vector<AbstractInputGeneratorPtr>& inputGenerators) {
  // Randomly pick to generate one or two input rows.
  std::vector<InputTestCase> inputs;
  int numInputs = vectorFuzzer.coinToss(0.5) ? 1 : 2;
  // Generate the metadata for the input row.
  InputRowMetadata metadata;
  for (int idx = 0; idx < rowType->size(); ++idx) {
    if (options_.commonDictionaryWrapRatio > 0 &&
        vectorFuzzer.coinToss(options_.commonDictionaryWrapRatio)) {
      metadata.columnsToWrapInCommonDictionary.push_back(idx);
    }
    if (options_.lazyVectorGenerationRatio > 0 &&
        vectorFuzzer.coinToss(options_.lazyVectorGenerationRatio)) {
      metadata.columnsToWrapInLazy.push_back(
          vectorFuzzer.coinToss(0.8) ? idx : -1 * idx);
    }
  }
  // Generate the input row.
  for (int inputIdx = 0; inputIdx < numInputs; ++inputIdx) {
    std::vector<VectorPtr> children;
    children.reserve(rowType->size() + 1);
    for (auto i = 0; i < rowType->size(); ++i) {
      const auto& inputGenerator =
          inputGenerators.size() > i ? inputGenerators[i] : nullptr;
      if (std::binary_search(
              metadata.columnsToWrapInCommonDictionary.begin(),
              metadata.columnsToWrapInCommonDictionary.end(),
              i)) {
        // These will be wrapped in common dictionary later.
        if (vectorFuzzer.getOptions().allowConstantVector &&
            vectorFuzzer.coinToss(0.2)) {
          children.push_back(
              vectorFuzzer.fuzzConstant(rowType->childAt(i), inputGenerator));
        } else {
          children.push_back(
              vectorFuzzer.fuzzFlat(rowType->childAt(i), inputGenerator));
        }
      } else {
        children.push_back(
            vectorFuzzer.fuzz(rowType->childAt(i), inputGenerator));
      }
    }

    vector_size_t vecSize = vectorFuzzer.getOptions().vectorSize;

    // Modify the input row if needed based on the metadata.
    if (metadata.columnsToWrapInCommonDictionary.size() < 2) {
      // Avoid wrapping in common dictionary if there is only one column.
      metadata.columnsToWrapInCommonDictionary.clear();
    } else {
      auto commonIndices = vectorFuzzer.fuzzIndices(vecSize, vecSize);
      auto commonNulls = vectorFuzzer.fuzzNulls(vecSize);

      for (auto colIdx : metadata.columnsToWrapInCommonDictionary) {
        auto& child = children[colIdx];
        VELOX_CHECK_NOT_NULL(child);
        child = BaseVector::wrapInDictionary(
            commonNulls, commonIndices, vecSize, child);
      }
    }
    // Append row number column to the input row.
    auto names = rowType->names();
    names.push_back("row_number");

    velox::test::VectorMaker vectorMaker{pool_.get()};
    children.push_back(vectorMaker.flatVector<int64_t>(
        vecSize, [&](auto row) { return row; }));

    // Finally create the input row.
    RowVectorPtr rowVector = vectorMaker.rowVector(names, children);
    inputs.push_back({rowVector, SelectivityVector(vecSize)});
  }
  // Return the input rows and the metadata.
  return {inputs, metadata};
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
  LOG(ERROR) << "==============================> Top " << maxEntriesLimit
             << " by number of rows processed";
  LOG(ERROR)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (int i = 0; i < maxEntriesLimit; i++) {
    LOG(ERROR) << entries[i].first << " " << entries[i].second.numTimesSelected
               << " " << std::fixed << std::setprecision(2)
               << (entries[i].second.numTimesSelected * 100.00) /
            totalSelections
               << "% " << entries[i].second.numProcessedRows;
  }

  LOG(ERROR) << "==============================> Bottom " << maxEntriesLimit
             << " by number of rows processed";
  LOG(ERROR)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (int i = 0; i < maxEntriesLimit; i++) {
    int idx = entries.size() - 1 - i;
    LOG(ERROR) << entries[idx].first << " "
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

  LOG(ERROR) << "==============================> All stats sorted by number "
                "of times the function was chosen";
  LOG(ERROR)
      << "Format: functionName numTimesSelected proportionOfTimesSelected "
         "numProcessedRows";
  for (auto& elem : entries) {
    LOG(ERROR) << elem.first << " " << elem.second.numTimesSelected << " "
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
    std::vector<fuzzer::InputTestCase> inputsToRetry,
    const VectorPtr& resultVector,
    const InputRowMetadata& inputRowMetadata) {
  // Wrap each expression tree with 'try'.
  std::vector<core::TypedExprPtr> tryPlans;
  for (auto& plan : plans) {
    tryPlans.push_back(std::make_shared<core::CallTypedExpr>(
        plan->type(), std::vector<core::TypedExprPtr>{plan}, "try"));
  }

  std::vector<ResultOrError> tryResults;
  std::vector<test::ExpressionVerifier::VerificationState>
      tryVerificationStates;

  // The function throws if anything goes wrong except
  // UNSUPPORTED_INPUT_UNCATCHABLE errors.
  try {
    std::tie(tryResults, tryVerificationStates) = verifier_.verify(
        tryPlans,
        inputsToRetry,
        resultVector ? BaseVector::copy(*resultVector) : nullptr,
        false, // canThrow
        inputRowMetadata);
  } catch (const std::exception&) {
    if (options_.findMinimalSubexpression) {
      test::computeMinimumSubExpression(
          {&execCtx_, {false, ""}, referenceQueryRunner_},
          *vectorFuzzer_,
          plans,
          inputsToRetry,
          inputRowMetadata);
    }
    throw;
  }

  std::vector<fuzzer::InputTestCase> inputsToRetryWithoutErrors;
  for (int i = 0; i < tryResults.size(); ++i) {
    auto& tryResult = tryResults[i];
    if (tryResult.unsupportedInputUncatchableError) {
      LOG(INFO)
          << "Retry with try fails to find minimal subexpression due to UNSUPPORTED_INPUT_UNCATCHABLE error.";
      return;
    }
    // Re-evaluate the original expression on rows that didn't produce an
    // error (i.e. returned non-NULL results when evaluated with TRY).
    inputsToRetry[i].activeRows = extractNonNullRows(tryResult.result);
    if (inputsToRetry[i].activeRows.hasSelections()) {
      inputsToRetryWithoutErrors.push_back(std::move(inputsToRetry[i]));
    }
  }

  if (!inputsToRetryWithoutErrors.empty()) {
    LOG(INFO) << "Retrying original expression on rows without errors";

    try {
      verifier_.verify(
          plans,
          inputsToRetryWithoutErrors,
          resultVector ? BaseVector::copy(*resultVector) : nullptr,
          false, // canThrow
          inputRowMetadata);
    } catch (const std::exception&) {
      if (options_.findMinimalSubexpression) {
        test::computeMinimumSubExpression(
            {&execCtx_, {false, ""}, referenceQueryRunner_},
            *vectorFuzzer_,
            plans,
            inputsToRetryWithoutErrors,
            inputRowMetadata);
      }
      throw;
    }
  }
}

void ExpressionFuzzerVerifier::go() {
  VELOX_CHECK(
      options_.steps > 0 || options_.durationSeconds > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");
  VELOX_CHECK_GT(
      options_.maxExpressionTreesPerStep,
      0,
      "--max_expression_trees_per_step needs to be greater than zero.");

  if (expressionFuzzer_.supportedFunctions().empty()) {
    LOG(WARNING) << "No functions to fuzz.";
    return;
  }

  auto startTime = std::chrono::system_clock::now();
  size_t i = 0;
  size_t totalTestCases = 0;
  size_t numFailed = 0;
  size_t numVerified = 0;
  size_t numReferenceUnsupported = 0;

  // TODO: some expression will throw exception for NaN input, eg: IN
  // predicate for floating point. remove this constraint once that are fixed
  auto vectorOptions = vectorFuzzer_->getOptions();
  vectorOptions.dataSpec = {false, false};
  vectorFuzzer_->setOptions(vectorOptions);
  while (!isDone(i, startTime)) {
    LOG(INFO) << "==============================> Started iteration " << i
              << " (seed: " << currentSeed_ << ")";

    // Generate multiple expression trees and input data vectors. They can
    // re-use columns and share sub-expressions if the appropriate flag is
    // set.
    int numExpressionTrees = boost::random::uniform_int_distribution<int>(
        1, options_.maxExpressionTreesPerStep)(rng_);
    auto [expressions, inputType, inputGenerators, selectionStats] =
        expressionFuzzer_.fuzzExpressions(numExpressionTrees);
    // Project a row number column in the output to enable epsilon-comparison
    // for floating-point columns and make investigation of failures easier.
    expressions.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "row_number"));

    for (auto& [funcName, count] : selectionStats) {
      exprNameToStats_[funcName].numTimesSelected += count;
    }

    std::vector<core::TypedExprPtr> plans = std::move(expressions);

    auto [inputTestCases, inputRowMetadata] =
        generateInput(inputType, *vectorFuzzer_, inputGenerators);
    totalTestCases += inputTestCases.size();
    for (auto j = 0; j < inputTestCases.size(); ++j) {
      VLOG(1) << "Input test case " << j << ": ";
      exec::test::logVectors({inputTestCases[j].inputVector});
    }

    auto resultVectors = generateResultVectors(plans);
    std::vector<fuzzer::ResultOrError> results;
    std::vector<test::ExpressionVerifier::VerificationState> verificationStates;

    try {
      std::tie(results, verificationStates) = verifier_.verify(
          plans,
          inputTestCases,
          resultVectors ? BaseVector::copy(*resultVectors) : nullptr,
          true, // canThrow
          inputRowMetadata);
    } catch (const std::exception&) {
      if (options_.findMinimalSubexpression) {
        test::computeMinimumSubExpression(
            {&execCtx_, {false, ""}, referenceQueryRunner_},
            *vectorFuzzer_,
            plans,
            inputTestCases,
            inputRowMetadata);
      }
      throw;
    }

    // If both paths threw compatible exceptions, we add a try() function to
    // the expression's root and execute it again. This time the expressions
    // cannot throw. Expressions that throw UNSUPPORTED_INPUT_UNCATCHABLE
    // errors are not supported.
    std::vector<fuzzer::InputTestCase> inputsToRetry;
    bool anyInputsThrewButRetryable = false;
    for (int j = 0; j < results.size(); j++) {
      auto& result = results[j];
      if (result.exceptionPtr) {
        if (!result.unsupportedInputUncatchableError && options_.retryWithTry) {
          anyInputsThrewButRetryable = true;
          inputsToRetry.push_back(inputTestCases[j]);
        }
      } else {
        // If we re-try then also run these inputs to ensure the conditions
        // during test run stay close to original, that is, multiple inputs are
        // executed.
        inputsToRetry.push_back(inputTestCases[j]);
      }

      auto& verificationState = verificationStates[j];
      switch (verificationState) {
        case test::ExpressionVerifier::VerificationState::
            kVerifiedAgainstReference:
          ++numVerified;
          break;
        case test::ExpressionVerifier::VerificationState::
            kReferencePathUnsupported:
          ++numReferenceUnsupported;
          break;
        case test::ExpressionVerifier::VerificationState::kBothPathsThrow:
          ++numFailed;
          break;
      }
    }
    if (anyInputsThrewButRetryable) {
      LOG(INFO)
          << "Both paths failed with compatible exceptions. Retrying expression using try().";
      retryWithTry(plans, inputsToRetry, resultVectors, inputRowMetadata);
    }

    LOG(INFO) << "==============================> Done with iteration " << i;
    reSeed();
    ++i;
  }
  logStats();

  LOG(ERROR) << "Total test cases: " << totalTestCases;
  LOG(ERROR) << "Total test cases verified in the reference DB: "
             << numVerified;
  LOG(ERROR) << "Total test cases failed in both Velox and the reference DB: "
             << numFailed;
  LOG(ERROR) << "Total test cases unsupported in the reference DB: "
             << numReferenceUnsupported;
}

} // namespace facebook::velox::fuzzer

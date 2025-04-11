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

#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/common/base/Fs.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/expression/Expr.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

using exec::test::ReferenceQueryErrorCode;

namespace {
void logInputs(const std::vector<fuzzer::InputTestCase>& inputTestCases) {
  int testCaseIdx = 0;
  for (const auto& [rowVector, rows] : inputTestCases) {
    VLOG(1) << "Input test case " << testCaseIdx++ << ": "
            << rowVector->childrenSize() << " vectors as input:";
    VLOG(1) << rowVector->childrenSize() << " vectors as input:";
    for (const auto& child : rowVector->children()) {
      VLOG(1) << "\t" << child->toString(/*recursive=*/true);
    }

    VLOG(1) << "RowVector contents (" << rowVector->type()->toString() << "):";

    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      VLOG(1) << "\tAt " << i << ": " << rowVector->toString(i);
    }
    VLOG(1) << "Rows to verify: " << rows.toString(rows.end());
  }
}

core::PlanNodePtr makeProjectionPlan(
    const RowVectorPtr& input,
    const std::vector<core::TypedExprPtr>& projections) {
  std::vector<std::string> names{projections.size()};
  for (auto i = 0; i < names.size(); ++i) {
    names[i] = fmt::format("p{}", i);
  }
  std::vector<RowVectorPtr> inputs{input};
  return std::make_shared<core::ProjectNode>(
      "project",
      names,
      projections,
      std::make_shared<core::ValuesNode>("values", inputs));
}

bool defaultNullRowsSkipped(const exec::ExprSet& exprSet) {
  auto stats = exprSet.stats();
  for (auto iter = stats.begin(); iter != stats.end(); ++iter) {
    if (iter->second.defaultNullRowsSkipped) {
      return true;
    }
  }
  return false;
}

// Returns a RowVector with only the rows specified in 'rows'. Does not maintain
// encodings.
RowVectorPtr reduceToSelectedRows(
    const RowVectorPtr& rowVector,
    const SelectivityVector& rows) {
  if (rows.isAllSelected()) {
    return rowVector;
  }
  BufferPtr indices = allocateIndices(rows.end(), rowVector->pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t cnt = 0;
  rows.applyToSelected([&](vector_size_t row) { rawIndices[cnt++] = row; });
  VELOX_CHECK_GT(cnt, 0);
  indices->setSize(cnt * sizeof(vector_size_t));
  // Top level row vector is not expected to be encoded, therefore we copy
  // instead of wrapping in the indices.
  RowVectorPtr reducedVector = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(rowVector->type(), cnt, rowVector->pool()));
  SelectivityVector rowsToCopy(cnt);
  reducedVector->copy(rowVector.get(), rowsToCopy, rawIndices);
  return reducedVector;
}

// Returns the expression set used by common expression eval path.
exec::ExprSet createExprSetCommon(
    const std::vector<core::TypedExprPtr>& plans,
    core::ExecCtx* execCtx,
    bool disableConstantFolding) {
  if (disableConstantFolding) {
    exec::ExprSet exprSetCommon(
        plans, execCtx, /*enableConstantFolding=*/false);
    return exprSetCommon;
  }

  // When constant folding is enabled, the expression evaluation can throw
  // VeloxRuntimeError of UNSUPPORTED_INPUT_UNCATCHABLE, which is allowed in
  // the fuzzer test.
  try {
    exec::ExprSet exprSetCommon(plans, execCtx, /*enableConstantFolding=*/true);
    return exprSetCommon;
  } catch (const VeloxException& e) {
    if (e.errorCode() != error_code::kUnsupportedInputUncatchable) {
      LOG(ERROR)
          << "ExprSet: Exceptions other than VeloxRuntimeError of UNSUPPORTED_INPUT_UNCATCHABLE are not allowed.";
      throw;
    }
    LOG(WARNING)
        << "ExprSet: Disabling constant folding to avoid VeloxRuntimeError of UNSUPPORTED_INPUT_UNCATCHABLE during evaluation.";
    exec::ExprSet exprSetCommon(
        plans, execCtx, /*enableConstantFolding=*/false);
    return exprSetCommon;
  } catch (...) {
    LOG(ERROR)
        << "ExprSet: Exceptions other than VeloxRuntimeError of UNSUPPORTED_INPUT_UNCATCHABLE are not allowed.";
    throw;
  }
}

} // namespace

std::pair<
    std::vector<fuzzer::ResultOrError>,
    std::vector<ExpressionVerifier::VerificationState>>
ExpressionVerifier::verify(
    const std::vector<core::TypedExprPtr>& plans,
    const std::vector<fuzzer::InputTestCase>& inputTestCases,
    VectorPtr&& resultVector,
    bool canThrow,
    const InputRowMetadata& inputRowMetadata) {
  for (int i = 0; i < plans.size(); ++i) {
    LOG(INFO) << "Executing expression " << i << " : " << plans[i]->toString();
  }
  logInputs(inputTestCases);

  // Store data and expression in case of reproduction.
  VectorPtr copiedResult;
  std::string sql;

  // Complex constants that aren't all expressible in sql
  std::vector<VectorPtr> complexConstants;
  // Deep copy to preserve the initial state of result vector.
  if (!options_.reproPersistPath.empty()) {
    if (resultVector) {
      copiedResult = BaseVector::copy(*resultVector);
    }
    // Disabling constant folding in order to preserve the original expression
    try {
      auto exprs = exec::ExprSet(plans, execCtx_, false).exprs();
      for (int i = 0; i < exprs.size(); ++i) {
        if (i > 0) {
          sql += ", ";
        }
        sql += exprs[i]->toSql(&complexConstants);
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to generate SQL: " << e.what();
      sql = "<failed to generate>";
    }
    if (options_.persistAndRunOnce) {
      persistReproInfo(
          inputTestCases,
          inputRowMetadata,
          copiedResult,
          sql,
          complexConstants);
    }
  }

  std::vector<fuzzer::ResultOrError> results;
  std::vector<VerificationState> verificationStates;
  // Share ExpressionSet between consecutive iterations to simulate its usage in
  // FilterProject.
  exec::ExprSet exprSetCommon =
      createExprSetCommon(plans, execCtx_, options_.disableConstantFolding);
  exec::ExprSetSimplified exprSetSimplified(plans, execCtx_);

  int testCaseItr = 0;
  for (auto [rowVector, rows] : inputTestCases) {
    LOG(INFO) << "Executing test case: " << testCaseItr++;
    // Execute expression plan using both common and simplified evals.
    std::vector<VectorPtr> commonEvalResult;
    std::vector<VectorPtr> simplifiedEvalResult;
    if (resultVector) {
      VELOX_CHECK(resultVector->encoding() == VectorEncoding::Simple::ROW);
      auto resultRowVector = resultVector->asUnchecked<RowVector>();
      auto children = resultRowVector->children();
      commonEvalResult.resize(children.size());
      simplifiedEvalResult.resize(children.size());
      for (int i = 0; i < children.size(); ++i) {
        commonEvalResult[i] = children[i];
      }
    }
    std::exception_ptr exceptionCommonPtr;
    std::exception_ptr exceptionSimplifiedPtr;

    VLOG(1) << "Starting common eval execution.";

    // Execute with common expression eval path. Some columns of the input row
    // vector will be wrapped in lazy as specified in 'columnsToWrapInLazy'.

    // Whether UNSUPPORTED_INPUT_UNCATCHABLE error is thrown from either the
    // common or simplified evaluation path. This error is allowed to thrown
    // only from one evaluation path because it is VeloxRuntimeError, hence
    // cannot be suppressed by default nulls.
    bool unsupportedInputUncatchableError{false};
    // Whether default null behavior takes place in the common evaluation path.
    // If so, errors from Presto are allowed because Presto doesn't suppress
    // error by default nulls.
    bool defaultNull{false};
    try {
      auto inputRowVector = rowVector;
      VectorPtr copiedInput;
      inputRowVector = VectorFuzzer::fuzzRowChildrenToLazy(
          rowVector, inputRowMetadata.columnsToWrapInLazy);
      if (inputRowMetadata.columnsToWrapInLazy.empty()) {
        // Copy loads lazy vectors so only do this when there are no lazy
        // inputs.
        copiedInput = BaseVector::copy(*inputRowVector);
      }

      exec::EvalCtx evalCtxCommon(
          execCtx_, &exprSetCommon, inputRowVector.get());
      exprSetCommon.eval(
          0,
          exprSetCommon.size(),
          true /*initialize*/,
          rows,
          evalCtxCommon,
          commonEvalResult);
      defaultNull = defaultNullRowsSkipped(exprSetCommon);

      if (copiedInput) {
        // Flatten the input vector as an optimization if its very deeply
        // nested.
        fuzzer::compareVectors(
            copiedInput,
            BaseVector::copy(*inputRowVector),
            "Copy of original input",
            "Input after common",
            rows);
      }
      LOG(INFO) << "Common eval succeeded.";
    } catch (const VeloxException& e) {
      LOG(INFO) << "Common eval failed.";
      if (e.errorCode() == error_code::kUnsupportedInputUncatchable) {
        unsupportedInputUncatchableError = true;
      } else if (!(canThrow && e.isUserError())) {
        if (!canThrow) {
          LOG(ERROR)
              << "Common eval wasn't supposed to throw, but it did. Aborting.";
        } else if (!e.isUserError()) {
          LOG(ERROR)
              << "Common eval: VeloxRuntimeErrors other than UNSUPPORTED_INPUT_UNCATCHABLE error are not allowed.";
        }
        persistReproInfoIfNeeded(
            inputTestCases,
            inputRowMetadata,
            copiedResult,
            sql,
            complexConstants);
        throw;
      }
      exceptionCommonPtr = std::current_exception();
    } catch (...) {
      LOG(ERROR)
          << "Common eval: Exceptions other than VeloxUserError or VeloxRuntimeError of UNSUPPORTED_INPUT_UNCATCHABLE are not allowed.";
      persistReproInfoIfNeeded(
          inputTestCases,
          inputRowMetadata,
          copiedResult,
          sql,
          complexConstants);
      throw;
    }

    VLOG(1) << "Starting reference eval execution.";

    if (referenceQueryRunner_ != nullptr) {
      VLOG(1) << "Execute with reference DB.";
      auto inputRowVector = reduceToSelectedRows(rowVector, rows);
      auto projectionPlan = makeProjectionPlan(inputRowVector, plans);
      auto referenceResultOrError =
          computeReferenceResults(projectionPlan, referenceQueryRunner_.get());

      auto referenceEvalResult = referenceResultOrError.first;

      if (referenceResultOrError.second !=
          ReferenceQueryErrorCode::kReferenceQueryUnsupported) {
        bool exceptionReference =
            (referenceResultOrError.second !=
             ReferenceQueryErrorCode::kSuccess);
        try {
          // Compare results or exceptions (if any). Fail if anything is
          // different.
          if (exceptionCommonPtr || exceptionReference) {
            // Throws in case only one evaluation path throws exception.
            // Otherwise, return false to signal that the expression failed.
            if (exceptionCommonPtr && exceptionReference) {
              verificationStates.push_back(VerificationState::kBothPathsThrow);
            } else {
              verificationStates.push_back(
                  VerificationState::kReferencePathUnsupported);
              if (!(defaultNull &&
                    referenceQueryRunner_->runnerType() ==
                        ReferenceQueryRunner::RunnerType::kPrestoQueryRunner)) {
                LOG(ERROR) << fmt::format(
                    "Only {} path threw exception",
                    exceptionCommonPtr ? "common" : "reference");
                if (exceptionCommonPtr) {
                  std::rethrow_exception(exceptionCommonPtr);
                } else {
                  auto referenceSql =
                      referenceQueryRunner_->toSql(projectionPlan);
                  VELOX_FAIL(
                      "Reference path throws for query: {}", *referenceSql);
                }
              }
            }
          } else {
            // Throws in case output is different.
            VELOX_CHECK_EQ(commonEvalResult.size(), plans.size());
            VELOX_CHECK(referenceEvalResult.has_value());

            std::vector<TypePtr> types;
            for (auto i = 0; i < commonEvalResult.size(); ++i) {
              types.push_back(commonEvalResult[i]->type());
            }
            auto commonEvalResultRow = std::make_shared<RowVector>(
                execCtx_->pool(),
                ROW(std::move(types)),
                nullptr,
                commonEvalResult[0]->size(),
                commonEvalResult);
            commonEvalResultRow =
                reduceToSelectedRows(commonEvalResultRow, rows);
            VELOX_CHECK(
                exec::test::assertEqualResults(
                    referenceEvalResult.value(),
                    projectionPlan->outputType(),
                    {commonEvalResultRow}),
                "Velox and reference DB results don't match");
            LOG(INFO) << "Verified results against reference DB";
            verificationStates.push_back(
                VerificationState::kVerifiedAgainstReference);
          }
        } catch (...) {
          persistReproInfoIfNeeded(
              inputTestCases,
              inputRowMetadata,
              copiedResult,
              sql,
              complexConstants);
          throw;
        }
      } else {
        LOG(INFO) << "Reference DB doesn't support this query";
        verificationStates.push_back(
            VerificationState::kReferencePathUnsupported);
      }
    } else {
      VLOG(1) << "Execute with simplified expression eval path.";
      try {
        exec::EvalCtx evalCtxSimplified(
            execCtx_, &exprSetSimplified, rowVector.get());

        auto copy = BaseVector::copy(*rowVector);
        exprSetSimplified.eval(
            0,
            exprSetSimplified.size(),
            true /*initialize*/,
            rows,
            evalCtxSimplified,
            simplifiedEvalResult);

        // Flatten the input vector as an optimization if its very deeply
        // nested.
        fuzzer::compareVectors(
            copy,
            BaseVector::copy(*rowVector),
            "Copy of original input",
            "Input after simplified",
            rows);
      } catch (const VeloxException& e) {
        if (e.errorCode() == error_code::kUnsupportedInputUncatchable) {
          unsupportedInputUncatchableError = true;
        } else if (!e.isUserError()) {
          LOG(ERROR)
              << "Simplified eval: VeloxRuntimeErrors other than UNSUPPORTED_INPUT_UNCATCHABLE error are not allowed.";
          persistReproInfoIfNeeded(
              inputTestCases,
              inputRowMetadata,
              copiedResult,
              sql,
              complexConstants);
          throw;
        }
        exceptionSimplifiedPtr = std::current_exception();
      } catch (...) {
        LOG(ERROR)
            << "Simplified eval: Exceptions other than VeloxUserError or VeloxRuntimeError with UNSUPPORTED_INPUT are not allowed.";
        persistReproInfoIfNeeded(
            inputTestCases,
            inputRowMetadata,
            copiedResult,
            sql,
            complexConstants);
        throw;
      }

      try {
        // Compare results or exceptions (if any). Fail if anything is
        // different.
        if (exceptionCommonPtr || exceptionSimplifiedPtr) {
          // UNSUPPORTED_INPUT_UNCATCHABLE errors are VeloxRuntimeErrors that
          // cannot
          // be suppressed by default NULLs. So it may happen that only one of
          // the common and simplified path throws this error. In this case, we
          // do not compare the exceptions.
          if (!unsupportedInputUncatchableError) {
            // Throws in case exceptions are not compatible. If they are
            // compatible, return false to signal that the expression failed.
            fuzzer::compareExceptions(
                exceptionCommonPtr, exceptionSimplifiedPtr);
          }
          results.push_back(
              {nullptr,
               exceptionCommonPtr ? exceptionCommonPtr : exceptionSimplifiedPtr,
               unsupportedInputUncatchableError});
          verificationStates.push_back(VerificationState::kBothPathsThrow);
          continue;
        } else {
          // Throws in case output is different.
          VELOX_CHECK_EQ(commonEvalResult.size(), plans.size());
          VELOX_CHECK_EQ(simplifiedEvalResult.size(), plans.size());
          for (int i = 0; i < plans.size(); ++i) {
            fuzzer::compareVectors(
                commonEvalResult[i],
                simplifiedEvalResult[i],
                "common path results ",
                "simplified path results",
                rows);
          }
          verificationStates.push_back(
              VerificationState::kVerifiedAgainstReference);
        }
      } catch (...) {
        persistReproInfoIfNeeded(
            inputTestCases,
            inputRowMetadata,
            copiedResult,
            sql,
            complexConstants);
        throw;
      }
    }

    if (!options_.reproPersistPath.empty() && options_.persistAndRunOnce) {
      // A guard to make sure it runs only once with persistAndRunOnce flag
      // turned on. It shouldn't reach here normally since the flag is used to
      // persist repro info for crash failures. But if it hasn't crashed by now,
      // we still don't want another iteration.
      LOG(WARNING)
          << "Iteration succeeded with --persist_and_run_once flag enabled "
             "(expecting crash failure)";
      exit(0);
    }

    if (exceptionCommonPtr) {
      results.push_back(
          {nullptr, exceptionCommonPtr, unsupportedInputUncatchableError});
    } else {
      results.push_back(
          {VectorMaker(commonEvalResult[0]->pool()).rowVector(commonEvalResult),
           nullptr,
           unsupportedInputUncatchableError});
    }
  }
  VELOX_CHECK_EQ(results.size(), verificationStates.size());
  return std::make_pair(results, verificationStates);
}

void ExpressionVerifier::persistReproInfoIfNeeded(
    const std::vector<fuzzer::InputTestCase>& inputTestCases,
    const InputRowMetadata& inputRowMetadata,
    const VectorPtr& resultVector,
    const std::string& sql,
    const std::vector<VectorPtr>& complexConstants) {
  if (options_.reproPersistPath.empty()) {
    LOG(INFO) << "Skipping persistence because repro path is empty.";
  } else if (!options_.persistAndRunOnce) {
    persistReproInfo(
        inputTestCases, inputRowMetadata, resultVector, sql, complexConstants);
  }
}

void ExpressionVerifier::persistReproInfo(
    const std::vector<fuzzer::InputTestCase>& inputTestCases,
    const InputRowMetadata& inputRowMetadata,
    const VectorPtr& resultVector,
    const std::string& sql,
    const std::vector<VectorPtr>& complexConstants) {
  std::vector<std::string> inputPaths;
  std::vector<std::string> inputSelectivityVectorPaths;
  std::string inputRowMetadataPath;
  std::string resultPath;
  std::string sqlPath;
  std::string complexConstantsPath;

  const auto basePath = options_.reproPersistPath.c_str();
  if (!common::generateFileDirectory(basePath)) {
    return;
  }

  // Create a new directory
  auto dirPath = common::generateTempFolderPath(basePath, "expressionVerifier");
  if (!dirPath.has_value()) {
    LOG(INFO) << "Failed to create directory for persisting repro info.";
    return;
  }
  // Saving input test cases
  for (int i = 0; i < inputTestCases.size(); i++) {
    auto filePath = fmt::format(
        "{}/{}_{}", dirPath->c_str(), kInputVectorFileNamePrefix, i);
    try {
      saveVectorToFile(inputTestCases[i].inputVector.get(), filePath.c_str());
      inputPaths.push_back(filePath);
    } catch (std::exception& e) {
      inputPaths.clear();
      inputPaths.push_back(e.what());
      break;
    }

    filePath = fmt::format(
        "{}/{}_{}", dirPath->c_str(), kInputSelectivityVectorFileNamePrefix, i);
    try {
      saveSelectivityVectorToFile(
          inputTestCases[i].activeRows, filePath.c_str());
      inputSelectivityVectorPaths.push_back(filePath);
    } catch (std::exception& e) {
      inputSelectivityVectorPaths.clear();
      inputSelectivityVectorPaths.push_back(e.what());
      break;
    }
  }

  // Saving the list of column indices that are to be wrapped in lazy.
  if (!inputRowMetadata.empty()) {
    inputRowMetadataPath =
        fmt::format("{}/{}", dirPath->c_str(), kInputRowMetadataFileName);
    try {
      inputRowMetadata.saveToFile(inputRowMetadataPath.c_str());
    } catch (std::exception& e) {
      inputRowMetadataPath = e.what();
    }
  }

  // Saving result vector
  if (resultVector) {
    resultPath = fmt::format("{}/{}", dirPath->c_str(), kResultVectorFileName);
    try {
      saveVectorToFile(resultVector.get(), resultPath.c_str());
    } catch (std::exception& e) {
      resultPath = e.what();
    }
  }

  // Saving sql
  sqlPath = fmt::format("{}/{}", dirPath->c_str(), kExpressionSqlFileName);
  try {
    saveStringToFile(sql, sqlPath.c_str());
  } catch (std::exception& e) {
    sqlPath = e.what();
  }
  // Saving complex constants
  if (!complexConstants.empty()) {
    complexConstantsPath =
        fmt::format("{}/{}", dirPath->c_str(), kComplexConstantsFileName);
    try {
      saveVectorToFile(
          VectorMaker(complexConstants[0]->pool())
              .rowVector(complexConstants)
              .get(),
          complexConstantsPath.c_str());
    } catch (std::exception& e) {
      complexConstantsPath = e.what();
    }
  }

  std::stringstream ss;
  ss << "Persisted input: --fuzzer_repro_path " << dirPath.value();
  ss << " --input_paths " << boost::algorithm::join(inputPaths, ",")
     << " --input_selectivity_vector_paths "
     << boost::algorithm::join(inputSelectivityVectorPaths, ",");
  if (resultVector) {
    ss << " --result_path " << resultPath;
  }
  ss << " --sql_path " << sqlPath;
  if (!inputRowMetadata.empty()) {
    ss << " --input_row_metadata_path " << inputRowMetadataPath;
  }
  if (!complexConstants.empty()) {
    ss << " --complex_constant_path " << complexConstantsPath;
  }
  LOG(INFO) << ss.str();
}

namespace {
class MinimalSubExpressionFinder {
 public:
  MinimalSubExpressionFinder(
      ExpressionVerifier&& verifier,
      VectorFuzzer& vectorFuzzer)
      : verifier_(verifier), vectorFuzzer_(vectorFuzzer) {}

  // Tries subexpressions of plan until finding the minimal failing subtree.
  void findMinimalExpression(
      core::TypedExprPtr plan,
      const std::vector<fuzzer::InputTestCase>& inputTestCases,
      const InputRowMetadata& inputRowMetadata) {
    if (verifyWithResults(plan, inputTestCases, inputRowMetadata)) {
      errorExit("Retry should have failed");
    }
    bool minimalFound =
        findMinimalRecursive(plan, inputTestCases, inputRowMetadata);
    if (minimalFound) {
      errorExit("Found minimal failing expression.");
    } else {
      errorExit("Only the top level expression failed!");
    }
  }

 private:
  // Central point for failure exit.
  void errorExit(const std::string& text) {
    VELOX_FAIL(text);
  }

  // Verifies children of 'plan'. If all succeed, sets minimalFound to
  // true and reruns 'plan' wth and without lazy vectors. Set
  // breakpoint inside this to debug failures.
  bool findMinimalRecursive(
      core::TypedExprPtr plan,
      const std::vector<fuzzer::InputTestCase>& inputTestCases,
      const InputRowMetadata& inputRowMetadata) {
    bool anyFailed = false;
    for (auto& input : plan->inputs()) {
      if (!verifyWithResults(input, inputTestCases, inputRowMetadata)) {
        anyFailed = true;
        bool minimalFound =
            findMinimalRecursive(input, inputTestCases, inputRowMetadata);
        if (minimalFound) {
          return true;
        }
      }
    }
    if (!anyFailed) {
      LOG(INFO) << "Failed with all children succeeding: " << plan->toString();
      // Re-running the minimum failed. Put breakpoint here to debug.
      verifyWithResults(plan, inputTestCases, inputRowMetadata);
      if (!inputRowMetadata.columnsToWrapInLazy.empty()) {
        LOG(INFO) << "Trying without lazy:";
        if (verifyWithResults(plan, inputTestCases, {})) {
          LOG(INFO) << "Minimal failure succeeded without lazy vectors";
        }
      }
      LOG(INFO) << fmt::format(
          "Found minimal failing subexpression: {}", plan->toString());
      return true;
    }

    return false;
  }

  // Verifies a 'plan' against a 'rowVector' with and without pre-existing
  // contents in result vector.
  bool verifyWithResults(
      core::TypedExprPtr plan,
      const std::vector<fuzzer::InputTestCase>& inputTestCases,
      const InputRowMetadata& inputRowMetadata) {
    VectorPtr result;
    LOG(INFO) << "Running with empty results vector :" << plan->toString();
    bool emptyResult =
        verifyPlan(plan, inputTestCases, inputRowMetadata, result);
    LOG(INFO) << "Running with non empty vector :" << plan->toString();
    result = vectorFuzzer_.fuzzFlat(plan->type());
    bool filledResult =
        verifyPlan(plan, inputTestCases, inputRowMetadata, result);
    if (emptyResult != filledResult) {
      LOG(ERROR) << fmt::format(
          "Different results for empty vs populated ! Empty result = {} filledResult = {}",
          emptyResult,
          filledResult);
    }
    return filledResult && emptyResult;
  }

  // Runs verification on a plan with a provided result vector.
  // Returns true if the verification is successful.
  bool verifyPlan(
      core::TypedExprPtr plan,
      const std::vector<fuzzer::InputTestCase>& inputTestCases,
      const InputRowMetadata& inputRowMetadata,
      VectorPtr results) {
    // Turn off unnecessary logging.
    FLAGS_minloglevel = 2;
    bool success = true;

    try {
      verifier_.verify(
          {plan},
          inputTestCases,
          results ? BaseVector::copy(*results) : nullptr,
          true, // canThrow
          inputRowMetadata);
    } catch (const std::exception&) {
      success = false;
    }
    FLAGS_minloglevel = 0;
    return success;
  }

  ExpressionVerifier verifier_;
  VectorFuzzer& vectorFuzzer_;
};
} // namespace

void computeMinimumSubExpression(
    ExpressionVerifier&& minimalVerifier,
    VectorFuzzer& fuzzer,
    const std::vector<core::TypedExprPtr>& plans,
    const std::vector<fuzzer::InputTestCase>& inputTestCases,
    const InputRowMetadata& inputRowMetadata) {
  auto finder = MinimalSubExpressionFinder(std::move(minimalVerifier), fuzzer);
  if (plans.size() > 1) {
    LOG(INFO)
        << "Found more than one expression, minimal subexpression might not work"
           " in cases where bugs are due to side effects when evaluating multiple"
           " expressions.";
  }
  for (auto plan : plans) {
    LOG(INFO) << "============================================";
    LOG(INFO) << "Finding minimal subexpression for plan:" << plan->toString();
    finder.findMinimalExpression(plan, inputTestCases, inputRowMetadata);
    LOG(INFO) << "============================================";
  }
}

} // namespace facebook::velox::test

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
#include "velox/expression/Expr.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::test {

namespace {

void printRowVector(const RowVectorPtr& rowVector) {
  LOG(INFO) << "RowVector contents (" << rowVector->type()->toString() << "):";

  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    LOG(INFO) << "\tAt " << i << ": " << rowVector->toString(i);
  }
}

void compareExceptions(
    const VeloxException& left,
    const VeloxException& right) {
  // Error messages sometimes differ; check at least error codes.
  // Since the common path may peel the input encoding off, whereas the
  // simplified path flatten input vectors, the common and the simplified
  // paths may evaluate the input rows in different orders and hence throw
  // different exceptions depending on which bad input they come across
  // first. We have seen this happen for the format_datetime Presto function
  // that leads to unmatched error codes UNSUPPORTED vs. INVALID_ARGUMENT.
  // Therefore, we intentionally relax the comparision here.
  VELOX_CHECK(
      left.errorCode() == right.errorCode() ||
      (left.errorCode() == "INVALID_ARGUMENT" &&
       right.errorCode() == "UNSUPPORTED") ||
      (right.errorCode() == "INVALID_ARGUMENT" &&
       left.errorCode() == "UNSUPPORTED"));
  VELOX_CHECK_EQ(left.errorSource(), right.errorSource());
  VELOX_CHECK_EQ(left.exceptionName(), right.exceptionName());
  if (left.message() != right.message()) {
    LOG(WARNING) << "Two different VeloxExceptions were thrown:\n\t"
                 << left.message() << "\nand\n\t" << right.message();
  }
}

void compareExceptions(
    std::exception_ptr commonPtr,
    std::exception_ptr simplifiedPtr) {
  // If we don't have two exceptions, fail.
  if (!commonPtr || !simplifiedPtr) {
    if (!commonPtr) {
      LOG(ERROR) << "Only simplified path threw exception:";
      std::rethrow_exception(simplifiedPtr);
    }
    LOG(ERROR) << "Only common path threw exception:";
    std::rethrow_exception(commonPtr);
  }

  // Otherwise, make sure the exceptions are the same.
  try {
    std::rethrow_exception(commonPtr);
  } catch (const VeloxException& ve1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const VeloxException& ve2) {
      compareExceptions(ve1, ve2);
      return;
    } catch (const std::exception& e2) {
      LOG(WARNING) << "Two different exceptions were thrown:\n\t"
                   << ve1.message() << "\nand\n\t" << e2.what();
    }
  } catch (const std::exception& e1) {
    try {
      std::rethrow_exception(simplifiedPtr);
    } catch (const std::exception& e2) {
      if (e1.what() != e2.what()) {
        LOG(WARNING) << "Two different std::exceptions were thrown:\n\t"
                     << e1.what() << "\nand\n\t" << e2.what();
      }
      return;
    }
  }
  VELOX_FAIL("Got two incompatible exceptions.");
}

void compareVectors(const VectorPtr& left, const VectorPtr& right) {
  VELOX_CHECK_EQ(left->size(), right->size());

  // Print vector contents if in verbose mode.
  size_t vectorSize = left->size();
  if (VLOG_IS_ON(1)) {
    LOG(INFO) << "== Result contents (common vs. simple): ";
    for (auto i = 0; i < vectorSize; i++) {
      LOG(INFO) << "At " << i << ": [" << left->toString(i) << " vs "
                << right->toString(i) << "]";
    }
    LOG(INFO) << "===================";
  }

  for (auto i = 0; i < vectorSize; i++) {
    VELOX_CHECK(
        left->equalValueAt(right.get(), i, i),
        "Different results at idx '{}': '{}' vs. '{}'",
        i,
        left->toString(i),
        right->toString(i));
  }
  LOG(INFO) << "All results match.";
}

} // namespace

bool ExpressionVerifier::verify(
    const core::TypedExprPtr& plan,
    const RowVectorPtr& rowVector,
    VectorPtr&& resultVector,
    bool canThrow) {
  LOG(INFO) << "Executing expression: " << plan->toString();

  if (rowVector) {
    LOG(INFO) << rowVector->childrenSize() << " vectors as input:";
    for (const auto& child : rowVector->children()) {
      LOG(INFO) << "\t" << child->toString(/*recursive=*/true);
    }

    if (VLOG_IS_ON(1)) {
      printRowVector(rowVector);
    }
  }

  // Store data and expression in case of reproduction.
  VectorPtr copiedResult;
  std::string sql;
  // Deep copy to preserve the initial state of result vector.
  if (!options_.reproPersistPath.empty()) {
    if (resultVector) {
      copiedResult = BaseVector::copy(*resultVector);
    }
    std::vector<core::TypedExprPtr> typedExprs = {plan};
    // Disabling constant folding in order to preserver the original
    // expression
    sql =
        exec::ExprSet(std::move(typedExprs), execCtx_, false).expr(0)->toSql();
  }

  // Execute expression plan using both common and simplified evals.
  std::vector<VectorPtr> commonEvalResult(1);
  std::vector<VectorPtr> simplifiedEvalResult(1);

  commonEvalResult[0] = resultVector;

  std::exception_ptr exceptionCommonPtr;
  std::exception_ptr exceptionSimplifiedPtr;

  VLOG(1) << "Starting common eval execution.";
  SelectivityVector rows{rowVector ? rowVector->size() : 1};

  // Execute with common expression eval path.
  try {
    exec::ExprSet exprSetCommon(
        {plan}, execCtx_, !options_.disableConstantFolding);
    exec::EvalCtx evalCtxCommon(execCtx_, &exprSetCommon, rowVector.get());

    try {
      exprSetCommon.eval(rows, evalCtxCommon, commonEvalResult);
    } catch (...) {
      if (!canThrow) {
        LOG(ERROR)
            << "Common eval wasn't supposed to throw, but it did. Aborting.";
        throw;
      }
      exceptionCommonPtr = std::current_exception();
    }
  } catch (...) {
    exceptionCommonPtr = std::current_exception();
  }

  VLOG(1) << "Starting simplified eval execution.";

  // Execute with simplified expression eval path.
  try {
    exec::ExprSetSimplified exprSetSimplified({plan}, execCtx_);
    exec::EvalCtx evalCtxSimplified(
        execCtx_, &exprSetSimplified, rowVector.get());

    try {
      exprSetSimplified.eval(rows, evalCtxSimplified, simplifiedEvalResult);
    } catch (...) {
      if (!canThrow) {
        LOG(ERROR)
            << "Simplified eval wasn't supposed to throw, but it did. Aborting.";
        throw;
      }
      exceptionSimplifiedPtr = std::current_exception();
    }
  } catch (...) {
    exceptionSimplifiedPtr = std::current_exception();
  }

  try {
    // Compare results or exceptions (if any). Fail is anything is different.
    if (exceptionCommonPtr || exceptionSimplifiedPtr) {
      // Throws in case exceptions are not compatible. If they are compatible,
      // return false to signal that the expression failed.
      compareExceptions(exceptionCommonPtr, exceptionSimplifiedPtr);
      return false;
    } else {
      // Throws in case output is different.
      compareVectors(commonEvalResult.front(), simplifiedEvalResult.front());
    }
  } catch (...) {
    if (!options_.reproPersistPath.empty()) {
      persistReproInfo(rowVector, copiedResult, sql);
    }
    throw;
  }
  return true;
}

void ExpressionVerifier::persistReproInfo(
    const VectorPtr& inputVector,
    const VectorPtr& resultVector,
    const std::string& sql) {
  std::string inputPath;
  std::string resultPath;
  std::string sqlPath;

  const auto basePath = options_.reproPersistPath.c_str();
  // Saving input vector
  auto inputPathOpt = generateFilePath(basePath, "vector");
  if (!inputPathOpt.has_value()) {
    inputPath = "Failed to create file for saving input vector.";
  } else {
    inputPath = inputPathOpt.value();
    try {
      saveVectorToFile(inputVector.get(), inputPath.c_str());
    } catch (std::exception& e) {
      inputPath = e.what();
    }
  }

  // Saving result vector
  if (resultVector) {
    auto resultPathOpt = generateFilePath(basePath, "vector");
    if (!resultPathOpt.has_value()) {
      resultPath = "Failed to create file for saving result vector.";
    } else {
      resultPath = resultPathOpt.value();
      try {
        saveVectorToFile(resultVector.get(), resultPath.c_str());
      } catch (std::exception& e) {
        resultPath = e.what();
      }
    }
  }

  // Saving sql
  auto sqlPathOpt = generateFilePath(basePath, "sql");
  if (!sqlPathOpt.has_value()) {
    sqlPath = "Failed to create file for saving SQL.";
  } else {
    sqlPath = sqlPathOpt.value();
    try {
      saveStringToFile(sql, sqlPath.c_str());
    } catch (std::exception& e) {
      resultPath = e.what();
    }
  }

  std::stringstream ss;
  ss << "Persisted input at '" << inputPath;
  if (resultVector) {
    ss << "' and result at '" << resultPath;
  }
  ss << "' and sql at '" << sqlPath << "'";
  LOG(INFO) << ss.str();
}

} // namespace facebook::velox::test

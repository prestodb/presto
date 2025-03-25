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

#include <gtest/gtest.h>

#include "velox/common/base/Fs.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/expression/tests/ExpressionRunner.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/QueryPlanner.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::test {

namespace {
/// Creates a RowVector from a list of child vectors. Uses _col0, _col1,..
/// auto-generated names for the RowType.
RowVectorPtr createRowVector(
    const std::vector<VectorPtr>& vectors,
    vector_size_t size,
    memory::MemoryPool* pool) {
  auto n = vectors.size();

  std::vector<std::string> names;
  names.reserve(n);
  std::vector<TypePtr> types;
  types.reserve(n);
  for (auto i = 0; i < n; ++i) {
    names.push_back(fmt::format("_col{}", i));
    types.push_back(vectors[i]->type());
  }

  return std::make_shared<RowVector>(
      pool, ROW(std::move(names), std::move(types)), nullptr, size, vectors);
}

RowVectorPtr evaluateAndPrintResults(
    exec::ExprSet& exprSet,
    const RowVectorPtr& data,
    const SelectivityVector& rows,
    core::ExecCtx& execCtx) {
  exec::EvalCtx evalCtx(&execCtx, &exprSet, data.get());

  std::vector<VectorPtr> results(1);
  exprSet.eval(rows, evalCtx, results);

  // Print the results.
  auto rowResult = createRowVector(results, rows.size(), execCtx.pool());
  std::cout << "Result: " << rowResult->type()->toString() << std::endl;
  exec::test::printResults(rowResult, std::cout);
  return rowResult;
}

// Adjusts the number of rows to be evaluated to be at most numRows.
SelectivityVector adjustRows(
    vector_size_t numRows,
    const SelectivityVector& rows) {
  if (numRows == 0 || numRows >= rows.countSelected()) {
    return rows;
  }
  SelectivityVector adjustedRows(rows.end(), false);
  for (int i = 0; i < rows.end() && numRows > 0; i++) {
    if (rows.isValid(i)) {
      adjustedRows.setValid(i, true);
      numRows--;
    }
  }
  return adjustedRows;
}

void saveResults(
    const RowVectorPtr& results,
    const std::string& directoryPath,
    const std::string& fileName) {
  auto path =
      common::generateTempFilePath(directoryPath.c_str(), fileName.c_str());
  VELOX_CHECK(
      path.has_value(),
      "Failed to create file for saving result vector in {} directory.",
      directoryPath);

  saveVectorToFile(results.get(), path.value().c_str());
  LOG(INFO) << "Saved the results in " << path.value() << ". "
            << results->size() << " rows: " << results->type()->toString();
}
} // namespace

std::vector<core::TypedExprPtr> ExpressionRunner::parseSql(
    const std::string& sql,
    const TypePtr& inputType,
    memory::MemoryPool* pool,
    const VectorPtr& complexConstants) {
  auto exprs = parse::parseMultipleExpressions(sql, {});

  std::vector<core::TypedExprPtr> typedExprs;
  typedExprs.reserve(exprs.size());
  for (const auto& expr : exprs) {
    typedExprs.push_back(
        core::Expressions::inferTypes(expr, inputType, pool, complexConstants));
  }
  return typedExprs;
}

// splits up strings into a vector of strings from a comma separated string
// e.g. "a,b,c" -> ["a", "b", "c"]
std::vector<std::string> split(const std::string& s) {
  std::vector<std::string> result;
  std::stringstream ss(s);
  while (ss.good()) {
    std::string substr;
    getline(ss, substr, ',');
    result.push_back(substr);
  }
  return result;
}

// Make all children of the input row vector at 'indices' wrapped in the same
// dictionary Buffers. These children are assumed to have already been wrapped
// in the same dictionary but through separate Buffers. Making them wrapped in
// the same Buffers is necessary to trigger peeling.
RowVectorPtr replicateCommonDictionaryLayer(
    RowVectorPtr inputVector,
    std::vector<int> indices) {
  if (inputVector == nullptr || indices.size() < 2) {
    return inputVector;
  }
  std::vector<VectorPtr> children = inputVector->children();
  auto firstEncodedChild = children[indices[0]];
  VELOX_CHECK_EQ(
      firstEncodedChild->encoding(), VectorEncoding::Simple::DICTIONARY);
  auto commonDictionaryIndices = firstEncodedChild->wrapInfo();
  auto commonNulls = firstEncodedChild->nulls();
  for (auto i = 1; i < indices.size(); i++) {
    auto& child = children[indices[i]];
    VELOX_CHECK_EQ(child->encoding(), VectorEncoding::Simple::DICTIONARY);
    child = BaseVector::wrapInDictionary(
        commonNulls,
        commonDictionaryIndices,
        child->size(),
        child->valueVector());
  }
  return std::make_shared<RowVector>(
      inputVector->pool(),
      inputVector->type(),
      inputVector->nulls(),
      inputVector->size(),
      children);
}

// Applies modifications to the input test cases based on the input row
// metadata, which includes making sure specific columns are wrapped in common
// dictionary and/or wrapped in a lazy shim layer.
void applyModificationsToInput(
    std::vector<fuzzer::InputTestCase>& inputTestCases,
    const InputRowMetadata& inputRowMetadata) {
  for (auto& testCase : inputTestCases) {
    auto& inputVector = testCase.inputVector;
    inputVector = replicateCommonDictionaryLayer(
        inputVector, inputRowMetadata.columnsToWrapInCommonDictionary);
    inputVector = VectorFuzzer::fuzzRowChildrenToLazy(
        inputVector, inputRowMetadata.columnsToWrapInLazy);
  }
}

void ExpressionRunner::run(
    const std::string& inputPaths,
    const std::string& inputSelectivityVectorPaths,
    const std::string& sql,
    const std::string& complexConstantsPath,
    const std::string& resultPath,
    const std::string& mode,
    vector_size_t numRows,
    const std::string& storeResultPath,
    const std::string& inputRowMetadataPath,
    std::shared_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner,
    bool findMinimalSubExpression,
    bool useSeperatePoolForInput) {
  VELOX_CHECK(!sql.empty());
  auto memoryManager = memory::memoryManager();
  auto queryCtx = core::QueryCtx::create();
  std::shared_ptr<memory::MemoryPool> deserializerPool{
      memoryManager->addLeafPool()};
  std::shared_ptr<memory::MemoryPool> pool = useSeperatePoolForInput
      ? memoryManager->addLeafPool("exprEval")
      : deserializerPool;
  core::ExecCtx execCtx{pool.get(), queryCtx.get()};

  fuzzer::InputRowMetadata inputRowMetadata;
  if (!inputRowMetadataPath.empty()) {
    inputRowMetadata = fuzzer::InputRowMetadata::restoreFromFile(
        inputRowMetadataPath.c_str(), pool.get());
  }

  std::vector<fuzzer::InputTestCase> inputTestCases;
  if (inputPaths.empty()) {
    inputTestCases.push_back(
        {std::make_shared<RowVector>(
             deserializerPool.get(),
             ROW({}),
             nullptr,
             1,
             std::vector<VectorPtr>{}),
         SelectivityVector(1)});
  } else {
    std::vector<std::string> inputPathsList = split(inputPaths);
    std::vector<std::string> inputSelectivityPaths =
        split(inputSelectivityVectorPaths);
    for (int i = 0; i < inputPathsList.size(); i++) {
      auto vector = restoreVectorFromFile(
          inputPathsList[i].c_str(), deserializerPool.get());
      auto inputVector = std::dynamic_pointer_cast<RowVector>(vector);
      VELOX_CHECK_NOT_NULL(
          inputVector,
          "Input vector is not a RowVector: {}",
          vector->toString());
      VELOX_CHECK_GT(inputVector->size(), 0, "Input vector must not be empty.");
      if (inputSelectivityPaths.size() > i) {
        inputTestCases.push_back(
            {inputVector,
             restoreSelectivityVectorFromFile(
                 inputSelectivityPaths[i].c_str())});
      } else {
        inputTestCases.push_back(
            {inputVector, SelectivityVector(inputVector->size(), true)});
      }
    }
    applyModificationsToInput(inputTestCases, inputRowMetadata);
  }

  VELOX_CHECK(inputTestCases.size() > 0);
  auto inputRowType = inputTestCases[0].inputVector->type();

  parse::registerTypeResolver();

  if (mode == "query") {
    core::DuckDbQueryPlanner planner{pool.get()};
    std::vector<RowVectorPtr> inputVectors;
    for (auto& testCase : inputTestCases) {
      inputVectors.push_back(testCase.inputVector);
    }
    if (inputRowType->size()) {
      LOG(INFO) << "Registering input vector as table t: "
                << inputRowType->toString();
      planner.registerTable("t", inputVectors);
    }

    auto plan = planner.plan(sql);
    auto results = exec::test::AssertQueryBuilder(plan).copyResults(pool.get());

    // Print the results.
    std::cout << "Result: " << results->type()->toString() << std::endl;
    exec::test::printResults(results, std::cout);

    if (!storeResultPath.empty()) {
      saveResults(results, storeResultPath, "resultVector");
    }
    return;
  }

  VectorPtr complexConstants{nullptr};
  if (!complexConstantsPath.empty()) {
    complexConstants =
        restoreVectorFromFile(complexConstantsPath.c_str(), pool.get());
  }
  auto typedExprs = parseSql(sql, inputRowType, pool.get(), complexConstants);

  VectorPtr resultVector;
  if (!resultPath.empty()) {
    resultVector = restoreVectorFromFile(resultPath.c_str(), pool.get());
  }

  LOG(INFO) << "Evaluating SQL expression(s): " << sql;

  if (mode == "verify") {
    auto verifier =
        test::ExpressionVerifier(&execCtx, {false, ""}, referenceQueryRunner);
    try {
      verifier.verify(
          typedExprs,
          inputTestCases,
          std::move(resultVector),
          true,
          inputRowMetadata);
    } catch (const std::exception&) {
      if (findMinimalSubExpression) {
        VectorFuzzer::Options options;
        VectorFuzzer fuzzer(options, pool.get());
        computeMinimumSubExpression(
            std::move(verifier),
            fuzzer,
            typedExprs,
            inputTestCases,
            inputRowMetadata);
      }
      throw;
    }

  } else if (mode == "common" || mode == "simplified") {
    std::shared_ptr<exec::ExprSet> exprSet = mode == "common"
        ? std::make_shared<exec::ExprSet>(typedExprs, &execCtx)
        : std::make_shared<exec::ExprSetSimplified>(typedExprs, &execCtx);
    for (int i = 0; i < inputTestCases.size(); ++i) {
      auto& testCase = inputTestCases[i];
      SelectivityVector rows = adjustRows(numRows, testCase.activeRows);
      std::cout << "Executing Input " << i << std::endl;
      auto results = evaluateAndPrintResults(
          *exprSet, testCase.inputVector, rows, execCtx);
      if (!storeResultPath.empty()) {
        auto fileName = fmt::format("resultVector_{}", i);
        saveResults(results, storeResultPath, fileName);
      }
    }
  } else {
    VELOX_FAIL("Unknown expression runner mode: [{}].", mode);
  }
}

} // namespace facebook::velox::test

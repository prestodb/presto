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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <folly/Benchmark.h>
#include <optional>
#include "velox/core/PlanNode.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Operator.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/codegen/CodegenCompiledExpressionTransform.h"
#include "velox/experimental/codegen/CodegenLogger.h"
#include "velox/experimental/codegen/code_generator/ExprCodeGenerator.h"
#include "velox/experimental/codegen/utils/resources/ResourcePath.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"

namespace facebook::velox::codegen {

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

class CodegenTestCore {
 protected:
  constexpr static TransformFlags defaultFlags =
      CodegenCompiledExpressionTransform::defaultFlags;
  /// For future use, evaluate expression without creating a plan node.
  /// \tparam SQLType
  /// \param inputRowBatches input  data
  /// \param selectedRows
  /// \param exprString string representation of the expression to evaluate
  /// \return RowPtr results
  template <typename... SQLType>
  std::vector<VectorPtr> evalExpr(
      RowVector& inputRowBatches,
      SelectivityVector& selectedRows,
      const std::vector<std::string>& exprString) {
    auto types = std::vector<std::shared_ptr<const Type>>{
        std::make_shared<SQLType>()...,
    };

    std::vector<std::shared_ptr<const core::ITypedExpr>> typedExprs;
    for (auto& expr : exprString) {
      typedExprs.push_back(makeTypedExpr(expr, inputRowBatches.type()));
    };

    facebook::velox::exec::ExprSet exprSet(
        std::move(typedExprs), execCtx_.get());
    EvalCtx context(execCtx_.get(), &exprSet, &inputRowBatches);
    std::vector<VectorPtr> results(sizeof...(SQLType));
    exprSet.eval(
        0, sizeof...(SQLType) - 1, true, selectedRows, &context, &results);
    return results;
  }

  // Copied from ExprTest.cpp
  std::shared_ptr<const core::ITypedExpr> makeTypedExpr(
      const std::string& text,
      const TypePtr& rowType) {
    auto untyped = parse::parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, pool_.get());
  }

  void init(const std::vector<std::string>& customCompilerOptions = {}) {
    CompilerOptions compilerOptions = defaultCompilerOptions();

    compilerOptions.extraCompileOptions.insert(
        compilerOptions.extraCompileOptions.end(),
        customCompilerOptions.begin(),
        customCompilerOptions.end());

    registerVeloxArithmeticUDFs(udfManager_);
    registerVeloxStringFunctions(udfManager_);

    useSymbolForArithmetic_ = false;
    codegenTransformation_ =
        std::make_unique<CodegenCompiledExpressionTransform>(
            compilerOptions,
            udfManager_,
            useSymbolForArithmetic_,
            eventSequence_);
    pool_ = memory::getDefaultScopedMemoryPool();
    queryCtx_ = core::QueryCtx::createForTest();
    execCtx_ = std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get());

    parse::registerTypeResolver();
    functions::prestosql::registerAllScalarFunctions();
  }

  /// Creates a plan node given filter and a list of projection
  /// \tparam SQLType input row type
  /// \param filterString filter expression
  /// \param projectionString projection expression list
  /// \param names
  /// \param inputVectors
  /// \return projectionNode Pointer
  template <typename... SQLType>
  auto createPlanNodeFromExpr(
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::vector<RowVectorPtr>& inputVectors) {
    auto plan = PlanBuilder().values(inputVectors);
    if (!filterExpr.empty()) {
      plan.filter(filterExpr);
    }
    if (!projectionExprs.empty()) {
      plan.project(projectionExprs);
    }
    return plan.planNode();
  }

  template <typename... SQLType>
  auto createRowType(std::vector<std::string> names) {
    std::vector<std::shared_ptr<const Type>>&& types{
        std::make_shared<SQLType>()...,
    };
    return ROW(std::move(names), std::move(types));
  }

  /// Default compiler options.
  /// \return default testing option.
  CompilerOptions defaultCompilerOptions() {
    std::string packageJson = ResourcePath::getResourcePath();
    auto defaultCompilerOptions =
        compiler_utils::CompilerOptions::fromJsonFile(packageJson);
    return defaultCompilerOptions;
  }

  /// Create input datasets.
  /// \param batches Number of splits
  /// \param rowPerBatch number of rows per split
  /// \param type
  /// \return list of data rows
  std::vector<RowVectorPtr> createRowVector(
      size_t batches,
      size_t rowPerBatch,
      const std::shared_ptr<const Type>& type,
      const std::function<bool(vector_size_t /*index*/)>& isNullAt = nullptr) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < batches; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(type, rowPerBatch, *pool_, isNullAt));
      vectors.push_back(vector);
    }
    return vectors;
  }

  /// Run a given planNode
  /// \param planNode
  /// \param [out] taskCursor a reference to the execution context.
  /// \return list of  Row vector
  std::vector<RowVectorPtr> runQuery(
      const std::shared_ptr<const core::PlanNode>& planNode,
      std::unique_ptr<TaskCursor>& taskCursor) {
    CursorParameters params;
    params.planNode = planNode;

    taskCursor = std::make_unique<TaskCursor>(params);

    std::vector<RowVectorPtr> actualResults;
    while (taskCursor->moveNext()) {
      actualResults.push_back(taskCursor->current());
    };
    return actualResults;
  }

  /// Read optional<NativeType> from a given flat vector
  /// \tparam NativeType
  /// \param rowVector
  /// \param rowIndex
  /// \return optional value
  template <typename NativeType>
  std::optional<NativeType> readVectorOptional(
      const VectorPtr& rowVector,
      size_t rowIndex) {
    if (rowVector->as<SimpleVector<NativeType>>()->isNullAt(rowIndex)) {
      return std::nullopt;
    };
    return rowVector->as<SimpleVector<NativeType>>()->valueAt(rowIndex);
  }

  /// Compares two flat vector of a givent type
  /// \tparam NativeType
  /// \param reference
  /// \param result
  /// \return nullopt if the vector are equal, or an error string otherwise
  template <typename NativeType>
  std::optional<std::string> compareSimpleVector(
      const VectorPtr& reference,
      const VectorPtr& result) {
    // Sanity checks
    VELOX_CHECK(reference->as<SimpleVector<NativeType>>() != nullptr);
    VELOX_CHECK_EQ(reference->size(), result->size());

    bool failed = false;

    // Helper function
    auto printOptional = [](const auto& optional, std::ostream& out) {
      if (optional.has_value()) {
        out << *optional;
      } else {
        out << "null";
      };
    };

    std::stringstream message;
    for (size_t rowIndex = 0; rowIndex < reference->size(); ++rowIndex) {
      auto referenceValue = readVectorOptional<NativeType>(reference, rowIndex);
      auto resultValue = readVectorOptional<NativeType>(result, rowIndex);
      if (referenceValue != resultValue) {
        message << rowIndex << ",\t";
        printOptional(referenceValue, message);
        message << ",\t";
        printOptional(resultValue, message);
        message << ",\n";
        failed = true;
      };
    }
    return failed ? std::make_optional(message.str()) : std::nullopt;
  }

  /// Compares two RowVector given a sequence of native types
  /// \tparam NativeType
  /// \tparam Index
  /// \param reference
  /// \param result
  /// \return
  template <typename... NativeType, size_t... Index>
  bool compareRowVector(
      const RowVectorPtr& reference,
      const RowVectorPtr& result,
      const std::index_sequence<Index...>&) {
    if (reference->size() != result->size()) {
      return true;
    }

    std::array<std::optional<std::string>, sizeof...(NativeType)> errors{
        compareSimpleVector<NativeType>(
            reference->childAt(Index), result->childAt(Index))...,
    };

    bool failed = false;
    for (size_t idx = 0; idx < sizeof...(NativeType); ++idx) {
      if (errors[idx].has_value()) {
        std::cout << fmt::format(
                         "Column {} with name {} failed to compare",
                         idx,
                         std::dynamic_pointer_cast<const RowType>(
                             reference->type())
                             ->nameOf(idx))
                  << std::endl;
        std::cout << errors[idx].value() << std::endl;
        failed = true;
      }
    }
    return failed;
  }

  std::unique_ptr<CodegenCompiledExpressionTransform> codegenTransformation_;
  std::unique_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::unique_ptr<core::ExecCtx> execCtx_;
  std::shared_ptr<core::QueryCtx> queryCtx_;
  UDFManager udfManager_;
  bool useSymbolForArithmetic_;
  NamedSteadyClockEventSequence eventSequence_;
};

class CodegenTestBase : public CodegenTestCore, public testing::Test {
 protected:
  /// Run the  same expressions with and without the codegen and
  /// compare the results
  /// \tparam SQLType
  /// \param filter
  /// \param projection
  /// \param inputRowType
  /// \param testBatches
  /// \param rowPerBatch
  /// \param transform flags
  /// \return void
  template <typename... SQLType>
  auto testExpressionsWithFlags(
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::shared_ptr<const RowType>& inputRowType,
      size_t testBatches,
      size_t rowPerBatch,
      const TransformFlags flags) {
    auto codegenLogger = std::make_shared<codegen::DefaultLogger>(filterExpr);
    // Creating the test plan
    // Create input vectors with 10% nulls each
    auto inputVectors = createRowVector(
        testBatches, rowPerBatch, inputRowType, [](vector_size_t index) {
          return index % 10;
        });

    auto plan = createPlanNodeFromExpr<SQLType...>(
        filterExpr, projectionExprs, inputVectors);

    // Reference execution
    std::unique_ptr<TaskCursor> referenceTaskCursor;
    auto references = runQuery(plan, referenceTaskCursor);

    // Set transform flags
    codegenTransformation_->setTransformFlags(flags);

    // Compiled execution
    codegenLogger->onCompileStart(*(plan));
    auto compiledPlan = codegenTransformation_->transform(*plan);

    // Print timing information
    codegenLogger->onCompileEnd(eventSequence_, *(plan));
    eventSequence_.clear();

    std::unique_ptr<TaskCursor> compiledTaskCursor;
    auto results = runQuery(compiledPlan, compiledTaskCursor);

    ASSERT_EQ(results.size(), testBatches)
        << "expected result size is " << testBatches << " but got "
        << results.size();

    for (size_t batchIdx = 0; batchIdx < testBatches; ++batchIdx) {
      ASSERT_FALSE(
          compareRowVector<typename SQLType::NativeType::NativeType...>(
              references[batchIdx],
              results[batchIdx],
              std::index_sequence_for<SQLType...>()));
    };
  }

  /// Run the  same expressions with and without the codegen and
  /// compare the results with only projection
  /// \tparam SQLType
  /// \param exprStrings
  /// \param inputRowType
  /// \param testBatches
  /// \param rowPerBatch
  /// \return void
  template <typename... SQLType>
  auto testExpressions(
      const std::vector<std::string>& exprStrings,
      const std::shared_ptr<const RowType>& inputRowType,
      size_t testBatches,
      size_t rowPerBatch) {
    testExpressions<SQLType...>(
        "", exprStrings, inputRowType, testBatches, rowPerBatch);
  }

  /// Run the  same expressions with and without the codegen and
  /// compare the results
  /// \tparam SQLType
  /// \param filter
  /// \param projection
  /// \param inputRowType
  /// \param testBatches
  /// \param rowPerBatch
  /// \param transform flags
  /// \return void
  template <typename... SQLType>
  auto testExpressions(
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::shared_ptr<const RowType>& inputRowType,
      size_t testBatches,
      size_t rowPerBatch) {
    testExpressionsWithFlags<SQLType...>(
        filterExpr,
        projectionExprs,
        inputRowType,
        testBatches,
        rowPerBatch,
        defaultFlags);

    /// if it's filter + projection, then also test separated filter and
    /// projection w/ & w/o compiling filter
    if (!filterExpr.empty()) {
      testExpressionsWithFlags<SQLType...>(
          filterExpr,
          projectionExprs,
          inputRowType,
          testBatches,
          rowPerBatch,
          {{0, 0, 1, 1}});
      testExpressionsWithFlags<SQLType...>(
          filterExpr,
          projectionExprs,
          inputRowType,
          testBatches,
          rowPerBatch,
          {{1, 0, 1, 1}});
    }
  }

  virtual void SetUp() override {
    init();
  }
};
}; // namespace facebook::velox::codegen

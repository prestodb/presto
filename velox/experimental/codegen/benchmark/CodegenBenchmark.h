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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <algorithm>
#include "velox/common/base/Exceptions.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/experimental/codegen/CodegenCompiledExpressionTransform.h"
#include "velox/experimental/codegen/benchmark/ValueNodeReplacer.h"
#include "velox/experimental/codegen/tests/CodegenTestBase.h"

namespace facebook::velox::codegen {

// Benchmark utility class
class CodegenBenchmark : public CodegenTestCore {
 public:
  bool enableVectorFunctionCompare = false;

  /// Since the class CodegenBenchmark  manage the creation and registration of
  /// benchmarks we need to make sure that the planNode and other required
  /// objects are still "live" when runbenchmark(). So we use BenchmarkInfo to
  /// store all those objects, and pass references down to each lambda.
  struct BenchmarkInfo {
    std::string name;
    std::string filterExpr; // ""(empty string) -> no filter
    std::vector<std::string> projectionExprs;
    std::shared_ptr<const RowType> inputRowType;
    TransformFlags transformFlags = defaultFlags;

    std::vector<RowVectorPtr> inputVector;

    // reference run
    std::shared_ptr<const core::PlanNode> refPlanNodes;
    std::unique_ptr<TaskCursor> referenceTaskCursors;
    std::vector<RowVectorPtr> referenceResults;

    // compiled run
    std::shared_ptr<const core::PlanNode> compiledPlanNodes;
    std::unique_ptr<TaskCursor> compiledTaskCursors;
    std::vector<RowVectorPtr> compiledResults;

    // generated function run
    std::unique_ptr<GeneratedVectorFunctionBase> filterFunc = nullptr;
    std::unique_ptr<GeneratedVectorFunctionBase> projectionFunc;
    std::vector<RowVectorPtr> generatedFunctionResults;

    std::map<std::string, size_t> argNameToIndex;

    void clearStringEncoding() {
      for (auto& rowVector : inputVector) {
        for (auto& columns : rowVector->children()) {
          if (auto stringVector = columns->asFlatVector<StringView>()) {
            stringVector->invalidateIsAscii();
          }
        }
      }
    }

    /// Given a compiled plan node, this method extracts the IcompiledCall
    /// inside it and assign benchmarkinfo's fields
    /// @param plan
    auto setGeneratedFunctionFromProjection(const core::ProjectNode& plan) {
      auto getCompiledExpression =
          [](const std::shared_ptr<const core::ITypedExpr>& typedExpr) {
            if (auto fieldReference =
                    std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                        typedExpr);
                fieldReference != nullptr) {
              return std::dynamic_pointer_cast<const ICompiledCall>(
                  fieldReference->inputs()[0]);
            };
            return std::shared_ptr<const ICompiledCall>{};
          };

      if (!filterExpr.empty() && transformFlags.compileFilter &&
          !transformFlags.mergeFilter) {
        // has filter, and is compiled separately
        filterFunc = getCompiledExpression(
                         std::dynamic_pointer_cast<const core::FilterNode>(
                             plan.sources()[0])
                             ->filter())
                         ->newInstance();
        filterFunc->setRowType(
            ROW({"selected"}, std::vector<TypePtr>{BOOLEAN()}));
      }

      std::set<std::shared_ptr<const ICompiledCall>> compiledCalls;
      std::transform(
          plan.projections().begin(),
          plan.projections().end(),
          std::inserter(compiledCalls, compiledCalls.begin()),
          getCompiledExpression);

      VELOX_CHECK(
          compiledCalls.size() == 1 && (*compiledCalls.begin()) != nullptr);

      auto compiledCall = (*compiledCalls.begin());
      projectionFunc = compiledCall->newInstance();
      projectionFunc->setRowType(refPlanNodes->outputType());
      auto i = 0;
      for (auto& child : compiledCall->inputs()) {
        argNameToIndex
            [std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(child)
                 ->name()] = i++;
      }
    }
  };

  CodegenBenchmark() {
    CodegenTestCore::init();
    queryCtx = core::QueryCtx::createForTest();
    pool = memory::getDefaultScopedMemoryPool();
    execCtx = std::make_unique<core::ExecCtx>(pool.get(), queryCtx_.get());
  }

  std::vector<BenchmarkInfo> templateBenchmarkInfo;
  std::shared_ptr<core::QueryCtx> queryCtx;
  std::unique_ptr<memory::MemoryPool> pool;
  std::unique_ptr<core::ExecCtx> execCtx;
  std::vector<BenchmarkInfo> benchmarkInfos;
  bool reusePlanNode = true;

  auto findTemplateBenchmarkInfo(
      const std::string& benchmarkName,
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::shared_ptr<const RowType>& inputRowType,
      const TransformFlags& flags) {
    for (auto iterator = templateBenchmarkInfo.begin();
         iterator != templateBenchmarkInfo.end();
         ++iterator) {
      if (benchmarkName == iterator->name and
          filterExpr == iterator->filterExpr and
          projectionExprs == iterator->projectionExprs and
          (*inputRowType) == (*iterator->inputRowType) and
          flags.flagVal == iterator->transformFlags.flagVal) {
        return iterator;
      }
    }
    return templateBenchmarkInfo.end();
  }
  /// Create a new BenchmarkInfo object in append in to the list of
  /// benchmarks
  /// \tparam SQLType return types
  /// \param benchmarkName
  /// \param exprString
  /// \param inputRowType
  /// \param testBatches
  /// \param rowPerBatch
  /// \return index position inside benchmarkInfos
  template <typename... SQLType>
  auto prepareBenchmarkInfo(
      const std::string& benchmarkName,
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::shared_ptr<const RowType>& inputRowType,
      size_t batchCount,
      size_t rowPerBatch,
      const TransformFlags& flags) {
    benchmarkInfos.push_back({});
    auto benchmarkIndex = benchmarkInfos.size() - 1;
    auto& newBenchmarkInfo = benchmarkInfos.back();

    newBenchmarkInfo.name = benchmarkName;

    // Creating the test plan
    // create by default 10% nulls
    auto inputVectors = createRowVector(
        batchCount, rowPerBatch, inputRowType, [](vector_size_t index) {
          return index % 10;
        });

    newBenchmarkInfo.inputVector = inputVectors;

    auto templateBenchmark = findTemplateBenchmarkInfo(
        benchmarkName, filterExpr, projectionExprs, inputRowType, flags);
    if (!reusePlanNode or (templateBenchmark == templateBenchmarkInfo.end())) {
      // First time we see this benchmark, regular path
      newBenchmarkInfo.refPlanNodes = createPlanNodeFromExpr<SQLType...>(
          filterExpr, projectionExprs, inputVectors);
      codegenTransformation_->setTransformFlags(flags);
      newBenchmarkInfo.compiledPlanNodes = codegenTransformation_->transform(
          *benchmarkInfos.back().refPlanNodes);

      templateBenchmarkInfo.push_back({});

      templateBenchmarkInfo.back().name = benchmarkName;
      templateBenchmarkInfo.back().filterExpr = filterExpr;
      templateBenchmarkInfo.back().projectionExprs = projectionExprs;
      templateBenchmarkInfo.back().inputRowType = inputRowType;
      templateBenchmarkInfo.back().transformFlags = flags;

      templateBenchmarkInfo.back().refPlanNodes = newBenchmarkInfo.refPlanNodes;
      templateBenchmarkInfo.back().compiledPlanNodes =
          newBenchmarkInfo.compiledPlanNodes;

    } else {
      VELOX_CHECK(templateBenchmark->refPlanNodes != nullptr);
      VELOX_CHECK(templateBenchmark->compiledPlanNodes != nullptr);
      ValueNodeReplacerTransform valueNodeReplacer(inputVectors);
      newBenchmarkInfo.refPlanNodes =
          valueNodeReplacer.transform(*templateBenchmark->refPlanNodes);
      newBenchmarkInfo.compiledPlanNodes =
          valueNodeReplacer.transform(*templateBenchmark->compiledPlanNodes);
    }

    newBenchmarkInfo.filterExpr = filterExpr;
    // running directly the vector function
    newBenchmarkInfo.setGeneratedFunctionFromProjection(
        *std::dynamic_pointer_cast<const core::ProjectNode>(
            newBenchmarkInfo.compiledPlanNodes));

    return benchmarkIndex;
  }

  /// Returns a tuple of executable lambda functions for benchmarking,
  /// those function can  be registered and run  separately.
  /// @tparam SQLType
  /// @param benchmarkName
  /// @param exprString
  /// @param inputRowType
  /// @param testBatches
  /// @param rowPerBatch
  /// @param flags
  /// @param rowPerBatch when true, the benchmark marker are normalized by  the
  /// nomber of row
  /// @return a pair of lambda functions

  template <typename... SQLType>
  auto createBenchmarkLambda(
      const std::string& benchmarkName,
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::shared_ptr<const RowType>& inputRowType,
      size_t batchCount,
      size_t rowPerBatch,
      const TransformFlags& flags,
      bool perRowNumber) {
    auto benchmarkIndex = prepareBenchmarkInfo<SQLType...>(
        benchmarkName,
        filterExpr,
        projectionExprs,
        inputRowType,
        batchCount,
        rowPerBatch,
        flags);

    auto numberIteration = perRowNumber ? rowPerBatch * batchCount : 1;

    // reference run
    auto referenceLambda = [ this, benchmarkIndex, numberIteration ]() -> auto {
      BenchmarkInfo& info = this->benchmarkInfos[benchmarkIndex];
      info.referenceResults.clear();
      info.clearStringEncoding();
      info.referenceResults = runQuery(
          info.refPlanNodes,
          this->benchmarkInfos[benchmarkIndex].referenceTaskCursors);
      return numberIteration;
    };

    // codegen'd run
    auto compiledLambda = [ this, benchmarkIndex, numberIteration ]() -> auto {
      BenchmarkInfo& info = this->benchmarkInfos[benchmarkIndex];
      info.compiledResults.clear();
      info.compiledResults =
          runQuery(info.compiledPlanNodes, info.compiledTaskCursors);
      return numberIteration;
    };

    // running thee naked ICompiledCall class
    auto generatedFunctionLambda =
        [ this, benchmarkIndex, numberIteration, inputRowType ]() -> auto {
      // We are including in the benchmark both the allocation of results
      // and the selectivity vector building and setting. It's not clear
      // that this is the right thing to do. Maybe we should move row to
      // BenchmarkInfo

      BenchmarkInfo& info = this->benchmarkInfos[benchmarkIndex];
      info.clearStringEncoding();
      SelectivityVector rows(info.inputVector[0]->size(), true);
      info.generatedFunctionResults.clear();
      auto evalBatch =
          [this, &rows, &info, inputRowType](RowVectorPtr& batch) -> VectorPtr {
        // Put the arguments in the correct expected order
        std::vector<VectorPtr> arguments;
        arguments.resize(info.argNameToIndex.size());

        for (auto i = 0; i < inputRowType->size(); i++) {
          if (!info.argNameToIndex.count(inputRowType->nameOf(i))) {
            continue;
          }

          arguments[info.argNameToIndex[inputRowType->nameOf(i)]] =
              batch.get()->children()[i];
        };
        auto context = exec::EvalCtx(execCtx_.get(), nullptr, batch.get());
        VectorPtr batchResult;
        if (info.filterFunc == nullptr) {
          // no filter
          dynamic_cast<facebook::velox::exec::VectorFunction*>(
              info.projectionFunc.get())
              ->apply(
                  rows,
                  arguments, // See D27826068
                  // batch.get()->children(),
                  nullptr,
                  context,
                  batchResult);
          return batchResult;
        }

        // has separately compiled filter
        rows.setAll();
        VectorPtr filterResult;
        FilterEvalCtx filterContext;

        dynamic_cast<facebook::velox::exec::VectorFunction*>(
            info.filterFunc.get())
            ->apply(
                rows,
                arguments, // See D27826068
                // batch.get()->children(),
                nullptr,
                context,
                filterResult);

        auto numOut = facebook::velox::exec::processFilterResults(
            filterResult->as<RowVector>()->childAt(0),
            rows,
            filterContext,
            context.pool());

        if (numOut == 0) {
          // nothing passed filter, no need to evaluate projection
          return nullptr;
        }

        if (numOut != batch->size()) {
          rows.setFromBits(
              filterContext.selectedBits->as<uint64_t>(), batch->size());
        }

        dynamic_cast<facebook::velox::exec::VectorFunction*>(
            info.projectionFunc.get())
            ->apply(
                rows, batch.get()->children(), nullptr, context, batchResult);

        if (numOut != batch->size()) {
          batchResult = facebook::velox::exec::wrap(
              numOut,
              filterContext.selectedIndices,
              std::dynamic_pointer_cast<RowVector>(batchResult));
        }

        return batchResult;
      };

      for (auto& batch : info.inputVector) {
        auto batchResult = evalBatch(batch);
        if (batchResult != nullptr && batchResult->size() > 0) {
          info.generatedFunctionResults.push_back(
              std::dynamic_pointer_cast<RowVector>(batchResult));
        }
      };
      return numberIteration;
    };

    // Compiled time run
    auto compileTimeLambda =
        [ this, benchmarkIndex, numberIteration ]() -> auto {
      auto& info = benchmarkInfos[benchmarkIndex];
      codegenTransformation_->transform(*info.refPlanNodes);
      return numberIteration;
    };

    return std::make_tuple(
        std::move(referenceLambda),
        std::move(compiledLambda),
        std::move(generatedFunctionLambda),
        std::move(compileTimeLambda));
  }

  /// Register a set of benchmarks to be ran with runBenchmark later.
  /// \tparam SQLType
  /// \param benchmarkName
  /// \param filterExpr
  /// \param projectionExprs
  /// \param inputRowType
  /// \param testBatches
  /// \param rowPerBatch
  /// \param flags
  /// \return
  template <typename... SQLType>
  auto benchmarkExpressions(
      const std::string& benchmarkName,
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::shared_ptr<const RowType>& inputRowType,
      size_t numberBatches,
      size_t rowPerBatch,
      const std::vector<TransformFlags>& flagsVec = {defaultFlags},
      bool nomalizeNumbers = true) {
    auto referenceName = fmt::format(
        "{}_{}x{}_reference", benchmarkName, numberBatches, rowPerBatch);

    // the % is used to enable relative benchmarks.
    auto compiledName = fmt::format(
        "%{}_{}x{}_compiled", benchmarkName, numberBatches, rowPerBatch);

    // the % is used to enable relative benchmarks.
    auto vectorFunctionName = fmt::format(
        "%{}_{}x{}_vectorFunc", benchmarkName, numberBatches, rowPerBatch);

    // the % is used to enable relative benchmarks.
    auto compileTimeName = fmt::format(
        "%{}_{}x{}_compileTime", benchmarkName, numberBatches, rowPerBatch);

    for (const auto& flags : flagsVec) {
      auto
          [referenceLambda,
           compiledLambda,
           vectorFunctionLambda,
           compileTimeLambda] =
              createBenchmarkLambda(
                  benchmarkName,
                  filterExpr,
                  projectionExprs,
                  inputRowType,
                  numberBatches,
                  rowPerBatch,
                  flags,
                  nomalizeNumbers);

      // ugly way of printing flag information
      folly::addBenchmark(
          __FILE__,
          fmt::format(
              "compile filter:{},merge:{},defaultnull:{}",
              (int)flags.compileFilter,
              (int)flags.mergeFilter,
              (int)(flags.enableDefaultNullOpt && flags.enableFilterDefaultNull)),
          []() -> unsigned { return 0; });

      // technically we only need to run referenceLambda once, but for the sake
      // of compareResults() we run it every time
      folly::addBenchmark(__FILE__, referenceName, referenceLambda);
      folly::addBenchmark(__FILE__, compiledName, compiledLambda);
      folly::addBenchmark(__FILE__, vectorFunctionName, vectorFunctionLambda);
      folly::addBenchmark(__FILE__, compileTimeName, compileTimeLambda);
    }
  }

  /// Register a set of benchmarks to be ran with runBenchmark later.
  /// with only projection
  /// \tparam SQLType
  /// \param benchmarkName
  /// \param exprString
  /// \param inputRowType
  /// \param testBatches
  /// \param rowPerBatch
  /// \return
  template <typename... SQLType>
  auto benchmarkExpressions(
      const std::string& benchmarkName,
      const std::vector<std::string>& exprString,
      const std::shared_ptr<const RowType>& inputRowType,
      size_t numberBatches,
      size_t rowPerBatch,
      const std::vector<TransformFlags>& flags = {defaultFlags},
      bool nomalizeNumbers = true) {
    benchmarkExpressions<SQLType...>(
        benchmarkName,
        "",
        exprString,
        inputRowType,
        numberBatches,
        rowPerBatch,
        flags,
        nomalizeNumbers);
  }

  /// Register a set of benchmarks with multiple number_batches/row_per_batch
  /// pairs
  /// \tparam SQLType
  /// \param benchmarkName
  /// \param filter
  /// \param projection
  /// \param inputRowType
  /// \param batches
  template <typename... SQLType>
  auto benchmarkExpressionsMult(
      const std::string& benchmarkName,
      const std::string& filterExpr,
      const std::vector<std::string>& projectionExprs,
      const std::shared_ptr<const RowType>& inputRowType,
      const std::vector<std::pair<size_t, size_t>>& batches,
      const std::vector<TransformFlags>& flagsVec = {defaultFlags},
      bool normalizeNumbers = true) {
    for (const auto& batch : batches) {
      benchmarkExpressions<SQLType...>(
          benchmarkName,
          filterExpr,
          projectionExprs,
          inputRowType,
          batch.first,
          batch.second,
          flagsVec,
          normalizeNumbers);
    }
  }

  /// Register a set of projection only benchmarks with multiple
  /// number_batches/row_per_batch pairs
  /// \tparam SQLType
  /// \param benchmarkName
  /// \param exprString
  /// \param inputRowType
  /// \param batches
  template <typename... SQLType>
  auto benchmarkExpressionsMult(
      const std::string& benchmarkName,
      const std::vector<std::string>& exprString,
      const std::shared_ptr<const RowType>& inputRowType,
      const std::vector<std::pair<size_t, size_t>>& batches,
      bool normalizeNumbers = true) {
    benchmarkExpressionsMult<SQLType...>(
        benchmarkName,
        "",
        exprString,
        inputRowType,
        batches,
        {defaultFlags}, // only interested other flags when there's filter
        normalizeNumbers);
  }

  void compareResults() {
    for (const auto& benchmarkInfo : benchmarkInfos) {
      auto& refResult = benchmarkInfo.referenceResults;
      auto& compiledResult = benchmarkInfo.compiledResults;
      auto& vectorFunctionResult = benchmarkInfo.generatedFunctionResults;

      VELOX_CHECK(refResult.size() == compiledResult.size());
      VELOX_CHECK(refResult.size() == vectorFunctionResult.size());

      for (size_t batchIndex = 0; batchIndex < refResult.size(); ++batchIndex) {
        VELOX_CHECK(
            refResult[batchIndex]->size() ==
            compiledResult[batchIndex]->size());

        // Disable untill i move back the fix diff for the input ordering
        if (enableVectorFunctionCompare) {
          VELOX_CHECK(
              refResult[batchIndex]->size() ==
              vectorFunctionResult[batchIndex]->size());
          for (size_t index = 0; index < refResult[batchIndex]->size();
               ++index) {
            VELOX_CHECK(
                refResult[batchIndex]->compare(
                    compiledResult[batchIndex].get(), index, index, {}) == 0);
            VELOX_CHECK(
                refResult[batchIndex]->compare(
                    vectorFunctionResult[batchIndex].get(), index, index, {}) ==
                0);
          }
        }
      }
    };
  }
};

class BenchmarkGTest : public CodegenTestBase {
 public:
  static inline std::vector<std::pair<size_t, size_t>> typicalBatches = {
      //{1, 1000000},
      {10, 100000},
      {100, 10000},
      //{1000, 1000},
  };
  static inline std::vector<TransformFlags> defaultFilterFlags = {
      {{0, 0, 1, 1}}, // not compiling filter (base)
      {{1, 0, 1, 1}}, // compiling filter separately
      {{1, 1, 0, 0}}, // compiling filter merged without default null
      {{1, 1, 1, 1}}, // compiling filter merged with default null
  };
  static inline CodegenBenchmark benchmark;
  static void TearDownTestSuite() {
    folly::runBenchmarks();
    benchmark.compareResults();
  }
};
}

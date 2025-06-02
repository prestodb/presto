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

#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/expression/Expr.h"

#include <cudf/aggregation.hpp>
#include <cudf/reduction.hpp>
#include <cudf/stream_compaction.hpp>

#include <unordered_map>

namespace facebook::velox::cudf_velox {

namespace {

void debugPrintTree(
    const std::shared_ptr<velox::exec::Expr>& expr,
    int indent = 0) {
  std::cout << std::string(indent, ' ') << expr->name() << std::endl;
  for (auto& input : expr->inputs()) {
    debugPrintTree(input, indent + 2);
  }
}
} // namespace

CudfFilterProject::CudfFilterProject(
    int32_t operatorId,
    velox::exec::DriverCtx* driverCtx,
    const velox::exec::FilterProject::Export& info,
    std::vector<velox::exec::IdentityProjection> identityProjections,
    const std::shared_ptr<const core::FilterNode>& filter,
    const std::shared_ptr<const core::ProjectNode>& project)
    : Operator(
          driverCtx,
          project ? project->outputType() : filter->outputType(),
          operatorId,
          project ? project->id() : filter->id(),
          "CudfFilterProject"),
      NvtxHelper(
          nvtx3::rgb{220, 20, 60}, // Crimson
          operatorId,
          fmt::format("[{}]", project ? project->id() : filter->id())),
      hasFilter_(info.hasFilter),
      project_(project),
      filter_(filter) {
  resultProjections_ = *(info.resultProjections);
  identityProjections_ = std::move(identityProjections);
  const auto inputType = project_ ? project_->sources()[0]->outputType()
                                  : filter_->sources()[0]->outputType();

  // convert to AST
  if (cudfDebugEnabled()) {
    int i = 0;
    for (auto expr : info.exprs->exprs()) {
      std::cout << "expr[" << i++ << "] " << expr->toString() << std::endl;
      debugPrintTree(expr);
    }
  }
  std::vector<std::shared_ptr<velox::exec::Expr>> projectExprs;
  if (hasFilter_) {
    // First expr is Filter, rest are Project
    filterEvaluator_ = ExpressionEvaluator({info.exprs->exprs()[0]}, inputType);
    projectExprs = {info.exprs->exprs().begin() + 1, info.exprs->exprs().end()};
  }
  projectEvaluator_ = ExpressionEvaluator(
      hasFilter_ ? projectExprs : info.exprs->exprs(), inputType);
}

void CudfFilterProject::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

RowVectorPtr CudfFilterProject::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  if (allInputProcessed()) {
    return nullptr;
  }
  if (input_->size() == 0) {
    input_.reset();
    return nullptr;
  }

  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input_);
  VELOX_CHECK_NOT_NULL(cudfInput);
  auto stream = cudfInput->stream();
  auto inputTableColumns = cudfInput->release()->release();

  if (hasFilter_) {
    filter(inputTableColumns, stream);
  }
  auto outputColumns = project(inputTableColumns, stream);

  auto outputTable = std::make_unique<cudf::table>(std::move(outputColumns));
  stream.synchronize();
  auto const numColumns = outputTable->num_columns();
  auto const size = outputTable->num_rows();
  if (cudfDebugEnabled()) {
    std::cout << "cudfProject Output: " << size << " rows, " << numColumns
              << " columns " << std::endl;
  }

  auto cudfOutput = std::make_shared<CudfVector>(
      input_->pool(), outputType_, size, std::move(outputTable), stream);
  input_.reset();
  if (numColumns == 0 or size == 0) {
    return nullptr;
  }
  return cudfOutput;
}

void CudfFilterProject::filter(
    std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
    rmm::cuda_stream_view stream) {
  // Evaluate the Filter
  auto filterColumns = filterEvaluator_.compute(
      inputTableColumns, stream, cudf::get_current_device_resource_ref());
  auto filterColumn = filterColumns[0]->view();
  // is all true in filter_column
  auto isAllTrue = cudf::reduce(
      filterColumn,
      *cudf::make_all_aggregation<cudf::reduce_aggregation>(),
      cudf::data_type(cudf::type_id::BOOL8),
      stream,
      cudf::get_current_device_resource_ref());
  using ScalarType = cudf::scalar_type_t<bool>;
  auto result = static_cast<ScalarType*>(isAllTrue.get());
  // If filter is not all true, apply the filter
  if (!(result->is_valid(stream) && result->value(stream))) {
    // Apply the Filter
    auto filterTable =
        std::make_unique<cudf::table>(std::move(inputTableColumns));
    auto filteredTable =
        cudf::apply_boolean_mask(*filterTable, filterColumn, stream);
    inputTableColumns = filteredTable->release();
  }
}

std::vector<std::unique_ptr<cudf::column>> CudfFilterProject::project(
    std::vector<std::unique_ptr<cudf::column>>& inputTableColumns,
    rmm::cuda_stream_view stream) {
  auto columns = projectEvaluator_.compute(
      inputTableColumns, stream, cudf::get_current_device_resource_ref());

  // Rearrange columns to match outputType_
  std::vector<std::unique_ptr<cudf::column>> outputColumns(outputType_->size());
  // computed resultProjections
  for (int i = 0; i < resultProjections_.size(); i++) {
    VELOX_CHECK_NOT_NULL(columns[i]);
    outputColumns[resultProjections_[i].outputChannel] = std::move(columns[i]);
  }

  // Count occurrences of each inputChannel, and move columns if they occur only
  // once
  std::unordered_map<column_index_t, int> inputChannelCount;
  for (const auto& identity : identityProjections_) {
    inputChannelCount[identity.inputChannel]++;
  }

  // identityProjections (input to output copy)
  for (auto const& identity : identityProjections_) {
    VELOX_CHECK_NOT_NULL(inputTableColumns[identity.inputChannel]);
    if (inputChannelCount[identity.inputChannel] == 1) {
      // Move the column if it occurs only once
      outputColumns[identity.outputChannel] =
          std::move(inputTableColumns[identity.inputChannel]);
    } else {
      // Otherwise, copy the column and decrement the count
      outputColumns[identity.outputChannel] = std::make_unique<cudf::column>(
          *inputTableColumns[identity.inputChannel],
          stream,
          cudf::get_current_device_resource_ref());
    }
    VELOX_CHECK_GT(inputChannelCount[identity.inputChannel], 0);
    inputChannelCount[identity.inputChannel]--;
  }

  return outputColumns;
}

bool CudfFilterProject::allInputProcessed() {
  return !input_;
}

bool CudfFilterProject::isFinished() {
  return noMoreInput_ && allInputProcessed();
}

} // namespace facebook::velox::cudf_velox

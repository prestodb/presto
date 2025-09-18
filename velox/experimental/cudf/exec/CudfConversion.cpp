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

#include "velox/experimental/cudf/exec/CudfConversion.h"
#include "velox/experimental/cudf/exec/NvtxHelper.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/vector/ComplexVector.h"

#include <cudf/copying.hpp>
#include <cudf/table/table.hpp>
#include <cudf/utilities/default_stream.hpp>

namespace facebook::velox::cudf_velox {

namespace {
// Concatenate multiple RowVectors into a single RowVector.
// Copied from AggregationFuzzer.cpp.
RowVectorPtr mergeRowVectors(
    const std::vector<RowVectorPtr>& results,
    velox::memory::MemoryPool* pool) {
  VELOX_NVTX_FUNC_RANGE();
  vector_size_t totalCount = 0;
  for (const auto& result : results) {
    totalCount += result->size();
  }
  auto copy =
      BaseVector::create<RowVector>(results[0]->type(), totalCount, pool);
  auto copyCount = 0;
  for (const auto& result : results) {
    copy->copy(result.get(), copyCount, 0, result->size());
    copyCount += result->size();
  }
  return copy;
}

cudf::size_type preferredGpuBatchSizeRows(
    const facebook::velox::core::QueryConfig& queryConfig) {
  constexpr cudf::size_type kDefaultGpuBatchSizeRows = 100000;
  const auto batchSize = queryConfig.get<int32_t>(
      CudfFromVelox::kGpuBatchSizeRows, kDefaultGpuBatchSizeRows);
  VELOX_CHECK_GT(batchSize, 0, "velox.cudf.gpu_batch_size_rows must be > 0");
  VELOX_CHECK_LE(
      batchSize,
      std::numeric_limits<vector_size_t>::max(),
      "velox.cudf.gpu_batch_size_rows must be <= max(vector_size_t)");
  return batchSize;
}
} // namespace

CudfFromVelox::CudfFromVelox(
    int32_t operatorId,
    RowTypePtr outputType,
    exec::DriverCtx* driverCtx,
    std::string planNodeId)
    : exec::Operator(
          driverCtx,
          outputType,
          operatorId,
          planNodeId,
          "CudfFromVelox"),
      NvtxHelper(
          nvtx3::rgb{255, 140, 0}, // Orange
          operatorId,
          fmt::format("[{}]", planNodeId)) {}

void CudfFromVelox::addInput(RowVectorPtr input) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  if (input->size() > 0) {
    // Materialize lazy vectors
    for (auto& child : input->children()) {
      child->loadedVector();
    }
    input->loadedVector();

    // Accumulate inputs
    inputs_.push_back(input);
    currentOutputSize_ += input->size();
  }
}

RowVectorPtr CudfFromVelox::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  const auto targetOutputSize =
      preferredGpuBatchSizeRows(operatorCtx_->driverCtx()->queryConfig());

  finished_ = noMoreInput_ && inputs_.empty();

  if (finished_ or
      (currentOutputSize_ < targetOutputSize and not noMoreInput_) or
      inputs_.empty()) {
    return nullptr;
  }

  // Select inputs that don't exceed the max vector size limit
  std::vector<RowVectorPtr> selectedInputs;
  vector_size_t totalSize = 0;
  auto const maxVectorSize = std::numeric_limits<vector_size_t>::max();

  for (const auto& input : inputs_) {
    if (totalSize + input->size() <= maxVectorSize) {
      selectedInputs.push_back(input);
      totalSize += input->size();
    } else {
      break;
    }
  }

  // Combine selected RowVectors into a single RowVector
  auto input = mergeRowVectors(selectedInputs, inputs_[0]->pool());

  // Remove processed inputs
  inputs_.erase(inputs_.begin(), inputs_.begin() + selectedInputs.size());
  currentOutputSize_ -= totalSize;

  // Early return if no input
  if (input->size() == 0) {
    return nullptr;
  }

  // Get a stream from the global stream pool
  auto stream = cudfGlobalStreamPool().get_stream();

  // Convert RowVector to cudf table
  auto tbl = with_arrow::toCudfTable(input, input->pool(), stream);

  stream.synchronize();

  VELOX_CHECK_NOT_NULL(tbl);

  // Return a CudfVector that owns the cudf table
  const auto size = tbl->num_rows();
  return std::make_shared<CudfVector>(
      input->pool(), outputType_, size, std::move(tbl), stream);
}

void CudfFromVelox::close() {
  cudf::get_default_stream().synchronize();
  exec::Operator::close();
  inputs_.clear();
}

CudfToVelox::CudfToVelox(
    int32_t operatorId,
    RowTypePtr outputType,
    exec::DriverCtx* driverCtx,
    std::string planNodeId)
    : exec::Operator(
          driverCtx,
          outputType,
          operatorId,
          planNodeId,
          "CudfToVelox"),
      NvtxHelper(
          nvtx3::rgb{148, 0, 211}, // Purple
          operatorId,
          fmt::format("[{}]", planNodeId)) {}

bool CudfToVelox::isPassthroughMode() const {
  return operatorCtx_->driverCtx()->queryConfig().get<bool>(
      kPassthroughMode, true);
}

void CudfToVelox::addInput(RowVectorPtr input) {
  // Accumulate inputs
  if (input->size() > 0) {
    auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
    VELOX_CHECK_NOT_NULL(cudfInput);
    inputs_.push_back(std::move(cudfInput));
  }
}

std::optional<uint64_t> CudfToVelox::averageRowSize() {
  if (!averageRowSize_) {
    averageRowSize_ =
        inputs_.front()->estimateFlatSize() / inputs_.front()->size();
  }
  return averageRowSize_;
}

RowVectorPtr CudfToVelox::getOutput() {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  if (finished_ || inputs_.empty()) {
    finished_ = noMoreInput_ && inputs_.empty();
    return nullptr;
  }

  // Get the target batch size
  const auto targetBatchSize = outputBatchRows(averageRowSize());
  auto stream = inputs_.front()->stream();

  // Process single input directly in these cases:
  // 1. In passthrough mode
  // 2. If we only have one input and it's smaller than or equal to the target
  // batch size
  if (isPassthroughMode() ||
      (inputs_.size() == 1 && inputs_.front()->size() <= targetBatchSize)) {
    std::unique_ptr<cudf::table> tbl = inputs_.front()->release();
    inputs_.pop_front();

    VELOX_CHECK_NOT_NULL(tbl);
    if (tbl->num_rows() == 0) {
      finished_ = noMoreInput_ && inputs_.empty();
      return nullptr;
    }
    RowVectorPtr output =
        with_arrow::toVeloxColumn(tbl->view(), pool(), "", stream);
    stream.synchronize();
    finished_ = noMoreInput_ && inputs_.empty();
    output->setType(outputType_);
    return output;
  }

  // Calculate how many tables we need to concatenate to reach the target batch
  // size and collect them in a vector
  std::vector<CudfVectorPtr> selectedInputs;
  vector_size_t totalSize = 0;

  while (!inputs_.empty() && totalSize < targetBatchSize) {
    auto& input = inputs_.front();
    if (totalSize + input->size() <= targetBatchSize) {
      totalSize += input->size();
      selectedInputs.push_back(std::move(input));
      inputs_.pop_front();
    } else {
      // If the next input would exceed targetBatchSize,
      // we need to split it and only take what we need
      auto cudfTableView = input->getTableView();
      auto partitions = std::vector<cudf::size_type>{
          static_cast<cudf::size_type>(targetBatchSize - totalSize)};
      auto tableSplits = cudf::split(cudfTableView, partitions);

      // Create new CudfVector from the first part
      auto firstPart = std::make_unique<cudf::table>(tableSplits[0], stream);
      auto firstPartSize = firstPart->num_rows();
      auto firstPartVector = std::make_shared<CudfVector>(
          pool(), input->type(), firstPartSize, std::move(firstPart), stream);

      // Create new CudfVector from the second part
      auto secondPart = std::make_unique<cudf::table>(tableSplits[1], stream);
      auto secondPartSize = secondPart->num_rows();
      auto secondPartVector = std::make_shared<CudfVector>(
          pool(), input->type(), secondPartSize, std::move(secondPart), stream);

      // Replace the original input with the second part
      input = std::move(secondPartVector);

      // Add the first part to selectedInputs
      selectedInputs.push_back(std::move(firstPartVector));
      totalSize += firstPartSize;
      break;
    }
  }

  finished_ = noMoreInput_ && inputs_.empty();

  // If we have no inputs to process, return nullptr
  if (selectedInputs.empty()) {
    return nullptr;
  }

  // Concatenate the selected tables on the GPU
  auto resultTable = getConcatenatedTable(selectedInputs, outputType_, stream);

  // Convert the concatenated table to a RowVector
  const auto size = resultTable->num_rows();
  VELOX_CHECK_NOT_NULL(resultTable);
  if (size == 0) {
    return nullptr;
  }

  RowVectorPtr output =
      with_arrow::toVeloxColumn(resultTable->view(), pool(), "", stream);
  stream.synchronize();
  finished_ = noMoreInput_ && inputs_.empty();
  output->setType(outputType_);
  return output;
}

void CudfToVelox::close() {
  exec::Operator::close();
  inputs_.clear();
}

} // namespace facebook::velox::cudf_velox

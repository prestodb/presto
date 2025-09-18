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

#include "velox/experimental/cudf/exec/CudfOrderBy.h"
#include "velox/experimental/cudf/exec/NvtxHelper.h"
#include "velox/experimental/cudf/exec/Utilities.h"

#include <cudf/sorting.hpp>

namespace facebook::velox::cudf_velox {

CudfOrderBy::CudfOrderBy(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::OrderByNode>& orderByNode)
    : exec::Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "CudfOrderBy"),
      NvtxHelper(
          nvtx3::rgb{64, 224, 208}, // Turquoise
          operatorId,
          fmt::format("[{}]", orderByNode->id())),
      orderByNode_(orderByNode) {
  sortKeys_.reserve(orderByNode->sortingKeys().size());
  columnOrder_.reserve(orderByNode->sortingKeys().size());
  nullOrder_.reserve(orderByNode->sortingKeys().size());
  for (int i = 0; i < orderByNode->sortingKeys().size(); ++i) {
    const auto channel =
        exec::exprToChannel(orderByNode->sortingKeys()[i].get(), outputType_);
    VELOX_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    sortKeys_.push_back(channel);
    auto const& sortingOrder = orderByNode->sortingOrders()[i];
    columnOrder_.push_back(
        sortingOrder.isAscending() ? cudf::order::ASCENDING
                                   : cudf::order::DESCENDING);
    nullOrder_.push_back(
        (sortingOrder.isNullsFirst() ^ !sortingOrder.isAscending())
            ? cudf::null_order::BEFORE
            : cudf::null_order::AFTER);
  }
}

void CudfOrderBy::addInput(RowVectorPtr input) {
  // Accumulate inputs
  if (input->size() > 0) {
    auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
    VELOX_CHECK_NOT_NULL(cudfInput);
    inputs_.push_back(std::move(cudfInput));
  }
}

void CudfOrderBy::noMoreInput() {
  exec::Operator::noMoreInput();

  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  if (inputs_.empty()) {
    return;
  }

  auto stream = cudfGlobalStreamPool().get_stream();
  auto tbl = getConcatenatedTable(inputs_, outputType_, stream);

  // Release input data after synchronizing
  stream.synchronize();
  inputs_.clear();

  VELOX_CHECK_NOT_NULL(tbl);

  auto keys = tbl->view().select(sortKeys_);
  auto values = tbl->view();
  auto result =
      cudf::sort_by_key(values, keys, columnOrder_, nullOrder_, stream);
  auto const size = result->num_rows();
  outputTable_ = std::make_shared<CudfVector>(
      pool(), outputType_, size, std::move(result), stream);
}

RowVectorPtr CudfOrderBy::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }
  finished_ = noMoreInput_;
  return outputTable_;
}

void CudfOrderBy::close() {
  exec::Operator::close();
  // Release stored inputs
  // Release cudf memory resources
  inputs_.clear();
  outputTable_.reset();
}
} // namespace facebook::velox::cudf_velox

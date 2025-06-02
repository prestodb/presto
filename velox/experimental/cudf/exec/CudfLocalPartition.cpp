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

#include "velox/experimental/cudf/exec/CudfLocalPartition.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/exec/Task.h"

#include <cudf/copying.hpp>
#include <cudf/partitioning.hpp>

namespace facebook::velox::cudf_velox {

CudfLocalPartition::CudfLocalPartition(
    int32_t operatorId,
    exec::DriverCtx* ctx,
    const std::shared_ptr<const core::LocalPartitionNode>& planNode)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "CudfLocalPartition"),
      NvtxHelper(
          nvtx3::rgb{255, 215, 0}, // Gold
          operatorId,
          fmt::format("[{}]", planNode->id())),
      queues_{
          ctx->task->getLocalExchangeQueues(ctx->splitGroupId, planNode->id())},
      numPartitions_{queues_.size()} {
  // Following is IMO a hacky way to get the partition key indices. It is to
  // workaround the fact that the partition spec constructs the hash function
  // directly and has no public methods to get the partition key indices.

  // When the operator is of type kRepartition, the partition spec is a string
  // in the format "HASH(key1, key2, ...)"
  // We're going to extract the keys between HASH( and ) and find their indices
  // in the output row type.

  // When operator is of type kGather, we don't need to store any partition key
  // indices because we're going to merge all the incoming streams together.

  // Get partition function specification string
  std::string spec = planNode->partitionFunctionSpec().toString();

  // Only parse keys if it's a hash function
  if (spec.find("HASH(") != std::string::npos) {
    // Extract keys between HASH( and )
    size_t start = spec.find("HASH(") + 5;
    size_t end = spec.find(")", start);
    if (start != std::string::npos && end != std::string::npos) {
      std::string keysStr = spec.substr(start, end - start);

      // Split by comma to get individual keys.
      std::vector<std::string> keys;
      size_t pos = 0;
      while ((pos = keysStr.find(",")) != std::string::npos) {
        std::string key = keysStr.substr(0, pos);
        keys.push_back(key);
        keysStr.erase(0, pos + 1);
      }
      keys.push_back(keysStr); // Add the last key.

      // Find field indices for each key.
      const auto& rowType = planNode->outputType();
      for (const auto& key : keys) {
        auto trimmedKey = key;
        // Trim whitespace
        trimmedKey.erase(0, trimmedKey.find_first_not_of(" "));
        trimmedKey.erase(trimmedKey.find_last_not_of(" ") + 1);

        auto fieldIndex = rowType->getChildIdx(trimmedKey);
        partitionKeyIndices_.push_back(fieldIndex);
      }
    }
  }
  VELOX_CHECK(numPartitions_ == 1 || partitionKeyIndices_.size() > 0);

  // Since we're replacing the LocalPartition with CudfLocalPartition, the
  // number of producers is already set. Adding producer only adds to a counter
  // which we don't have to do again.
  // Normally, this is what we'd have to do:
  // for (auto& queue : queues_) {
  //   queue->addProducer();
  // }
}

void CudfLocalPartition::addInput(RowVectorPtr input) {
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  auto cudfVector = std::dynamic_pointer_cast<CudfVector>(input);
  VELOX_CHECK(cudfVector, "Input must be a CudfVector");
  auto stream = cudfVector->stream();

  if (numPartitions_ > 1) {
    // Use cudf hash partitioning
    auto tableView = cudfVector->getTableView();
    std::vector<cudf::size_type> partitionKeyIndices;
    for (const auto& idx : partitionKeyIndices_) {
      partitionKeyIndices.push_back(static_cast<cudf::size_type>(idx));
    }

    auto [partitionedTable, partitionOffsets] = cudf::hash_partition(
        tableView,
        partitionKeyIndices,
        numPartitions_,
        cudf::hash_id::HASH_MURMUR3,
        cudf::DEFAULT_HASH_SEED,
        stream);

    VELOX_CHECK(partitionOffsets.size() == numPartitions_);
    VELOX_CHECK(partitionOffsets[0] == 0);

    // Erase first element since it's always 0 and we don't need it.
    partitionOffsets.erase(partitionOffsets.begin());

    auto partitionedTables =
        cudf::split(partitionedTable->view(), partitionOffsets);

    for (int i = 0; i < numPartitions_; ++i) {
      auto partitionData = partitionedTables[i];
      if (partitionData.num_rows() == 0) {
        // Skip empty partitions.
        continue;
      }

      ContinueFuture future;
      // DM: We should investigate if keeping partitionedTables alive and using
      // the table view in partitonedData is more efficient than creating a new
      // table each time. Currently out of scope because it would need a new
      // type of RowVector that can hold a table view and shared_ptr to the
      // table.
      auto blockingReason = queues_[i]->enqueue(
          std::make_shared<CudfVector>(
              pool(),
              outputType_,
              partitionData.num_rows(),
              std::make_unique<cudf::table>(partitionData),
              stream),
          partitionData.num_rows(),
          &future);
      if (blockingReason != exec::BlockingReason::kNotBlocked) {
        blockingReasons_.push_back(blockingReason);
        futures_.push_back(std::move(future));
      }
    }
  } else {
    // Single partition case.
    ContinueFuture future;
    auto blockingReason =
        queues_[0]->enqueue(input, input->retainedSize(), &future);
    if (blockingReason != exec::BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(blockingReason);
      futures_.push_back(std::move(future));
    }
  }
}

exec::BlockingReason CudfLocalPartition::isBlocked(ContinueFuture* future) {
  if (!futures_.empty()) {
    auto blockingReason = blockingReasons_.front();
    *future = folly::collectAll(futures_.begin(), futures_.end()).unit();
    futures_.clear();
    blockingReasons_.clear();
    return blockingReason;
  }

  return exec::BlockingReason::kNotBlocked;
}

void CudfLocalPartition::noMoreInput() {
  Operator::noMoreInput();
  for (const auto& queue : queues_) {
    queue->noMoreData();
  }
}

bool CudfLocalPartition::isFinished() {
  if (!futures_.empty() || !noMoreInput_) {
    return false;
  }

  return true;
}

} // namespace facebook::velox::cudf_velox

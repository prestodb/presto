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

#include "velox/exec/ScaleWriterLocalPartition.h"

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {
ScaleWriterPartitioningLocalPartition::ScaleWriterPartitioningLocalPartition(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::LocalPartitionNode>& planNode)
    : LocalPartition(operatorId, ctx, planNode),
      maxQueryMemoryUsageRatio_(
          ctx->queryConfig().scaleWriterRebalanceMaxMemoryUsageRatio()),
      maxTablePartitionsPerWriter_(
          ctx->queryConfig().scaleWriterMaxPartitionsPerWriter()),
      numTablePartitions_(maxTablePartitionsPerWriter_ * numPartitions_),
      queryPool_(pool()->root()),
      tablePartitionRebalancer_(ctx->task->getScaleWriterPartitionBalancer(
          ctx->splitGroupId,
          planNodeId())) {
  VELOX_CHECK_GT(maxTablePartitionsPerWriter_, 0);
  VELOX_CHECK_NOT_NULL(tablePartitionRebalancer_);

  VELOX_CHECK_EQ(
      numTablePartitions_, tablePartitionRebalancer_->numPartitions());
  VELOX_CHECK_EQ(numPartitions_, tablePartitionRebalancer_->numTasks());

  writerAssignmentCounts_.resize(numPartitions_, 0);
  tablePartitionRowCounts_.resize(numTablePartitions_, 0);
  tablePartitionWriterIds_.resize(numTablePartitions_, -1);
  tablePartitionWriterIndexes_.resize(numTablePartitions_, 0);
  writerAssignmmentIndicesBuffers_.resize(numPartitions_);
  rawWriterAssignmmentIndicesBuffers_.resize(numPartitions_);

  // Reset the hash partition function with the number of logical table
  // partitions instead of the number of table writers.
  // 'tablePartitionRebalancer_' is responsible for maintaining the mapping
  // from logical table partition id to the assigned table writer ids.
  partitionFunction_ = numPartitions_ == 1
      ? nullptr
      : planNode->partitionFunctionSpec().create(
            numTablePartitions_,
            /*localExchange=*/true);
}

void ScaleWriterPartitioningLocalPartition::initialize() {
  LocalPartition::initialize();
  VELOX_CHECK_NULL(memoryManager_);
  memoryManager_ =
      operatorCtx_->driver()->task()->getLocalExchangeMemoryManager(
          operatorCtx_->driverCtx()->splitGroupId, planNodeId());
}

void ScaleWriterPartitioningLocalPartition::prepareForWriterAssignments(
    vector_size_t numInput) {
  const auto maxIndicesBufferBytes = numInput * sizeof(vector_size_t);
  for (auto writerId = 0; writerId < numPartitions_; ++writerId) {
    if (writerAssignmmentIndicesBuffers_[writerId] == nullptr ||
        !writerAssignmmentIndicesBuffers_[writerId]->unique() ||
        writerAssignmmentIndicesBuffers_[writerId]->size() <
            maxIndicesBufferBytes) {
      writerAssignmmentIndicesBuffers_[writerId] =
          allocateIndices(numInput, pool());
      rawWriterAssignmmentIndicesBuffers_[writerId] =
          writerAssignmmentIndicesBuffers_[writerId]
              ->asMutable<vector_size_t>();
    }
  }
  std::fill(writerAssignmentCounts_.begin(), writerAssignmentCounts_.end(), 0);
  // Reset the value of partition writer id assignments for the new input.
  std::fill(
      tablePartitionWriterIds_.begin(), tablePartitionWriterIds_.end(), -1);
}

void ScaleWriterPartitioningLocalPartition::addInput(RowVectorPtr input) {
  prepareForInput(input);

  if (numPartitions_ == 1) {
    ContinueFuture future;
    auto blockingReason =
        queues_[0]->enqueue(input, input->retainedSize(), &future);
    if (blockingReason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(blockingReason);
      futures_.push_back(std::move(future));
    }
    return;
  }

  // Scale up writers when current buffer memory utilization is more than 50%
  // of the maximum. This also mean that we won't scale local  writers if the
  // writing speed can keep up with incoming data. In another word, buffer
  // utilization is below 50%.
  //
  // TODO: investigate using the consumer/producer queue time ratio as
  // additional signal to trigger rebalance to avoid unnecessary rebalancing
  // when the worker is overloaded which might cause a lot of queuing on both
  // producer and consumer sides. The buffered memory ratio is not a reliable
  // signal in that case.
  if ((memoryManager_->bufferedBytes() >
       memoryManager_->maxBufferBytes() * 0.5) &&
      // Do not scale up if total memory used is greater than
      // 'maxQueryMemoryUsageRatio_' of max query memory capacity. We have to be
      // conservative here otherwise scaling of writers will happen first
      // before we hit the query memory capacity limit, and then we won't be
      // able to do anything to prevent query OOM.
      queryPool_->reservedBytes() <
          queryPool_->maxCapacity() * maxQueryMemoryUsageRatio_) {
    tablePartitionRebalancer_->rebalance();
  }

  const auto singlePartition =
      partitionFunction_->partition(*input, partitions_);

  const auto numInput = input->size();
  const int64_t totalInputBytes = input->retainedSize();
  // Reset the value of partition row count for the new input.
  std::fill(
      tablePartitionRowCounts_.begin(), tablePartitionRowCounts_.end(), 0);

  // Assign each row to a writer by looking at logical table partition
  // assignments maintained by 'tablePartitionRebalancer_'.
  //
  // Get partition id which limits to 'tablePartitionCount_'. If there are
  // more physical table partitions than the logical 'tablePartitionCount_',
  // then it is possible that multiple physical table partitions will get
  // assigned the same logical partition id. Thus, multiple table partitions
  // will be scaled together since we track the written bytes of a logical table
  // partition.
  if (singlePartition.has_value()) {
    const auto partitionId = singlePartition.value();
    tablePartitionRowCounts_[partitionId] = numInput;

    VELOX_CHECK_EQ(tablePartitionWriterIds_[partitionId], -1);
    const auto writerId = getNextWriterId(partitionId);
    ContinueFuture future;
    auto blockingReason =
        queues_[writerId]->enqueue(input, totalInputBytes, &future);
    if (blockingReason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(blockingReason);
      futures_.push_back(std::move(future));
    }
  } else {
    prepareForWriterAssignments(numInput);

    for (auto row = 0; row < numInput; ++row) {
      const auto partitionId = partitions_[row];
      ++tablePartitionRowCounts_[partitionId];

      // Get writer id for this partition by looking at the scaling state.
      auto writerId = tablePartitionWriterIds_[partitionId];
      if (writerId == -1) {
        writerId = getNextWriterId(partitionId);
        tablePartitionWriterIds_[partitionId] = writerId;
      }
      rawWriterAssignmmentIndicesBuffers_[writerId]
                                         [writerAssignmentCounts_[writerId]++] =
                                             row;
    }

    for (auto i = 0; i < numPartitions_; ++i) {
      const auto writerRowCount = writerAssignmentCounts_[i];
      if (writerRowCount == 0) {
        continue;
      }

      auto writerInput = wrapChildren(
          input,
          writerRowCount,
          std::move(writerAssignmmentIndicesBuffers_[i]),
          queues_[i]->getVector());
      ContinueFuture future;
      auto reason = queues_[i]->enqueue(
          writerInput, totalInputBytes * writerRowCount / numInput, &future);
      if (reason != BlockingReason::kNotBlocked) {
        blockingReasons_.push_back(reason);
        futures_.push_back(std::move(future));
      }
    }
  }

  // Only update the scaling state if the memory used is below the
  // 'maxQueryMemoryUsageRatio_' limit. Otherwise, if we keep updating the
  // scaling state and the memory used is fluctuating around the limit, then we
  // could do massive scaling in a single rebalancing cycle which could cause
  // query OOM.
  if (queryPool_->reservedBytes() <
      queryPool_->maxCapacity() * maxQueryMemoryUsageRatio_) {
    for (auto tablePartition = 0; tablePartition < numTablePartitions_;
         ++tablePartition) {
      tablePartitionRebalancer_->addPartitionRowCount(
          tablePartition, tablePartitionRowCounts_[tablePartition]);
    }
    tablePartitionRebalancer_->addProcessedBytes(totalInputBytes);
  }
}

uint32_t ScaleWriterPartitioningLocalPartition::getNextWriterId(
    uint32_t partitionId) {
  return tablePartitionRebalancer_->getTaskId(
      partitionId, tablePartitionWriterIndexes_[partitionId]++);
}

void ScaleWriterPartitioningLocalPartition::close() {
  LocalPartition::close();

  // The last driver operator reports the shared table partition rebalancer
  // stats. We expect one reference hold by this operator and one referenced by
  // the task.
  if (tablePartitionRebalancer_.use_count() != 2) {
    return;
  }

  const auto scaleStats = tablePartitionRebalancer_->stats();
  auto lockedStats = stats_.wlock();
  if (scaleStats.numScaledPartitions != 0) {
    lockedStats->addRuntimeStat(
        kScaledPartitions, RuntimeCounter(scaleStats.numScaledPartitions));
  }
  if (scaleStats.numBalanceTriggers != 0) {
    lockedStats->addRuntimeStat(
        kRebalanceTriggers, RuntimeCounter(scaleStats.numBalanceTriggers));
  }
}

ScaleWriterLocalPartition::ScaleWriterLocalPartition(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::LocalPartitionNode>& planNode)
    : LocalPartition(operatorId, ctx, planNode),
      maxQueryMemoryUsageRatio_(
          ctx->queryConfig().scaleWriterRebalanceMaxMemoryUsageRatio()),
      queryPool_(pool()->root()),
      minDataProcessedBytes_(
          ctx->queryConfig()
              .scaleWriterMinPartitionProcessedBytesRebalanceThreshold()) {
  if (partitionFunction_ != nullptr) {
    VELOX_CHECK_NOT_NULL(
        dynamic_cast<RoundRobinPartitionFunction*>(partitionFunction_.get()));
  }
}

void ScaleWriterLocalPartition::initialize() {
  LocalPartition::initialize();
  VELOX_CHECK_NULL(memoryManager_);
  memoryManager_ =
      operatorCtx_->driver()->task()->getLocalExchangeMemoryManager(
          operatorCtx_->driverCtx()->splitGroupId, planNodeId());
}

void ScaleWriterLocalPartition::addInput(RowVectorPtr input) {
  prepareForInput(input);

  const int64_t totalInputBytes = input->retainedSize();
  processedDataBytes_ += totalInputBytes;

  uint32_t writerId = 0;
  if (numPartitions_ > 1) {
    writerId = getNextWriterId();
  }
  VELOX_CHECK_LT(writerId, numPartitions_);

  ContinueFuture future;
  auto blockingReason =
      queues_[writerId]->enqueue(input, input->retainedSize(), &future);
  if (blockingReason != BlockingReason::kNotBlocked) {
    blockingReasons_.push_back(blockingReason);
    futures_.push_back(std::move(future));
  }
}

uint32_t ScaleWriterLocalPartition::getNextWriterId() {
  VELOX_CHECK_LE(numWriters_, numPartitions_);
  VELOX_CHECK_GE(processedDataBytes_, processedBytesAtLastScale_);

  // Scale up writers when current buffer memory utilization is more than 50%
  // of the maximum. This also mean that we won't scale local  writers if the
  // writing speed can keep up with incoming data. In another word, buffer
  // utilization is below 50%.
  //
  // TODO: investigate using the consumer/producer queue time ratio as
  // additional signal to trigger rebalance to avoid unnecessary rebalancing
  // when the worker is overloaded which might cause a lot of queuing on both
  // producer and consumer sides. The buffered memory ratio is not a reliable
  // signal in that case.
  if ((numWriters_ < numPartitions_) &&
      (memoryManager_->bufferedBytes() >=
       memoryManager_->maxBufferBytes() / 2) &&
      // Do not scale up if total memory used is greater than
      // 'maxQueryMemoryUsageRatio_' of max query memory capacity. We have to be
      // conservative here otherwise scaling of writers will happen first
      // before we hit the query memory capacity limit, and then we won't be
      // able to do anything to prevent query OOM.
      (processedDataBytes_ - processedBytesAtLastScale_ >=
       numWriters_ * minDataProcessedBytes_) &&
      (queryPool_->reservedBytes() <
       queryPool_->maxCapacity() * maxQueryMemoryUsageRatio_)) {
    ++numWriters_;
    processedBytesAtLastScale_ = processedDataBytes_;
    LOG(INFO) << "Scaled task writer count to: " << numWriters_
              << " with max of " << numPartitions_;
  }
  return (nextWriterIndex_++) % numWriters_;
}

void ScaleWriterLocalPartition::close() {
  LocalPartition::close();

  if (numWriters_ == 1) {
    return;
  }
  stats_.wlock()->addRuntimeStat(
      kScaledWriters, RuntimeCounter(numWriters_ - 1));
}
} // namespace facebook::velox::exec

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

#include "velox/core/PlanNode.h"
#include "velox/experimental/wave/exec/AggregateFunctionRegistry.h"
#include "velox/experimental/wave/exec/AggregationInstructions.h"
#include "velox/experimental/wave/exec/WaveOperator.h"

namespace facebook::velox::wave {

class Aggregation : public WaveOperator {
 public:
  Aggregation(
      CompileState&,
      const core::AggregationNode&,
      const std::shared_ptr<aggregation::AggregateFunctionRegistry>&);

  ~Aggregation() override;

  bool isStreaming() const override {
    return false;
  }

  void enqueue(WaveVectorPtr input) override {
    VELOX_CHECK(!noMoreInput_);
    buffered_.push_back(std::move(input));
  }

  void flush(bool noMoreInput) override;

  AdvanceResult canAdvance(WaveStream& stream) override;

  void schedule(WaveStream& stream, int32_t maxRows) override;

  bool isFinished() const override {
    return finished_;
  }

  std::string toString() const override {
    return "Aggregation";
  }

  struct IdMapHolder {
    WaveBufferPtr idMap;
    WaveBufferPtr keys;
    WaveBufferPtr ids;
  };

 private:
  struct AggregateInfo {
    aggregation::AggregateFunction* function;
    std::vector<int32_t> inputChannels;
  };

  void toAggregateInfo(
      const TypePtr& inputType,
      const core::AggregationNode::Aggregate& aggregate,
      AggregateInfo* out);

  struct ProgramsHolder {
    WaveBufferPtr programs;
    WaveBufferPtr status;
    WaveBufferPtr instructions;
    std::vector<WaveBufferPtr> operands;
    std::vector<WaveVectorPtr> results;
  };

  BlockStatus* getStatus(int size, WaveBufferPtr& holder);
  void normalizeKeys();
  void doAggregates();
  void waitFlushDone();

  GpuArena* arena_;
  std::shared_ptr<aggregation::AggregateFunctionRegistry> functionRegistry_;

  aggregation::GroupsContainer* container_;
  struct {
    WaveBufferPtr container;
    WaveBufferPtr keyTypes;
    WaveBufferPtr idMaps;
    WaveBufferPtr groups;
    std::vector<IdMapHolder> idMapHolders;
    std::vector<WaveBufferPtr> keys;
    std::vector<WaveBufferPtr> accumulators;
    WaveBufferPtr groupKeys;
    WaveBufferPtr groupAccumulators;
  } containerHolder_;
  std::vector<int32_t> keyChannels_;
  std::vector<AggregateInfo> aggregates_;

  ProgramsHolder normalizeKeysHolder_;
  ProgramsHolder aggregateHolder_;

  std::vector<WaveVectorPtr> buffered_;
  std::vector<WaveVectorPtr> inputs_;
  std::unique_ptr<Stream> flushStream_;
  Event flushStart_{true}, flushDone_{true};

  struct {
    int64_t ingestedRowCount;
    float gpuTimeMs;
  } stats_{};

  bool noMoreInput_ = false;
  bool finished_ = false;
};

} // namespace facebook::velox::wave

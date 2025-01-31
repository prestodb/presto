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
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class IndexLookupJoin : public Operator {
 public:
  IndexLookupJoin(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::IndexLookupJoinNode>& joinNode);

  void initialize() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool needsInput() const override {
    if (noMoreInput_ || input_ != nullptr) {
      return false;
    }
    return true;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

  void close() override;

 private:
  // Initialize the lookup input and output type, and the output projections.
  void initLookupInput();
  void initLookupOutput();
  void initOutputProjections();
  // Prepare lookup input for index source lookup for a given 'input_'.
  void prepareLookupInput();

  void lookup();

  RowVectorPtr getOutputFromLookupResult();
  RowVectorPtr produceOutputForInnerJoin();
  RowVectorPtr produceOutputForLeftJoin();
  // Produces output for the remaining input rows that has no matches from the
  // lookup at the end of current input batch processing.
  RowVectorPtr produceRemainingOutputForLeftJoin();

  // Returns true if we have remaining output rows from the current
  // 'lookupResult_' after finishing processing all the output results from the
  // current 'lookupResultIter_'. For left join, we need to produce for the all
  // the input rows that have no matches in 'lookupResult_'.
  bool hasRemainingOutputForLeftJoin() const;

  // Checks if we have finished processing the current 'lookupResult_'. If so,
  // we reset 'lookupResult_' and corresponding processing state.
  void maybeFinishLookupResult();

  // Invoked after finished processing the current 'input_' batch. The function
  // resets the input batch and the lookup result states.
  void finishInput();

  // Prepare output row mappings for the next output batch with max size of
  // 'outputBatchSize'. This is only used by left join which needs to fill nulls
  // for output rows without lookup matches.
  void prepareOutputRowMappings(size_t outputBatchSize);
  // Prepare 'output_' for the next output batch with size of 'numOutputRows'.
  void prepareOutput(vector_size_t numOutputRows);

  // Maximum number of rows in the output batch.
  const vector_size_t outputBatchSize_;
  // Type of join.
  const core::JoinType joinType_;
  const size_t numKeys_;
  const RowTypePtr probeType_;
  const RowTypePtr lookupType_;
  const std::shared_ptr<connector::ConnectorTableHandle> lookupTableHandle_;
  const std::vector<core::TypedExprPtr> lookupConditions_;
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      lookupColumnHandles_;
  const std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  core::ExpressionEvaluator* const expressionEvaluator_;
  const std::shared_ptr<connector::Connector> connector_;

  // The lookup join plan node used to initialize this operator and reset after
  // that.
  std::shared_ptr<const core::IndexLookupJoinNode> joinNode_;

  // The data type of the lookup input including probe side columns either used
  // in equi-clauses or join conditions.
  RowTypePtr lookupInputType_;
  // The column channels in probe 'input_' referenced by 'lookupInputType_'.
  std::vector<column_index_t> lookupInputChannels_;
  // The reused row vector for lookup input.
  RowVectorPtr lookupInput_;

  // The data type of the lookup output from the lookup source.
  RowTypePtr lookupOutputType_;

  // Used to project output columns from the probe input and lookup output.
  std::vector<IdentityProjection> probeOutputProjections_;
  std::vector<IdentityProjection> lookupOutputProjections_;

  std::shared_ptr<connector::IndexSource> indexSource_;

  // Used for synchronization with the async fetch result from index source
  // through 'lookupResultIter_'.
  ContinueFuture lookupFuture_{ContinueFuture::makeEmpty()};
  // Used to fetch lookup results for each input batch, and reset after
  // processing all the outputs from the result.
  std::unique_ptr<connector::IndexSource::LookupResultIterator>
      lookupResultIter_;
  // Used to store the lookup result fetched from 'lookupResultIter_' for output
  // processing. We might split the output result into multiple output batches
  // based on the operator's output batch size limit.
  std::unique_ptr<connector::IndexSource::LookupResult> lookupResult_;

  // Points to the next output row in 'lookupResult_' for processing until
  // reaches to the end of 'lookupResult_'.
  vector_size_t nextOutputResultRow_{0};

  // Points to the input row in 'input_' that has matched in 'lookupResult_'.
  // The gap between consecutive input row indices indicates the number of input
  // rows that has no matches in 'lookupResult_'. The left join needs to fill
  // the lookup output with nulls for these input rows.
  const vector_size_t* rawLookupInputHitIndices_{nullptr};
  // This is set for left join to detect missed input rows.
  // If not null, it points to the last processed input row in 'input_'. It is
  // used with 'rawLookupInputHitIndices_' to detect input rows that has no
  // match in 'lookupResult_'.
  std::optional<vector_size_t> lastProcessedInputRow_;

  // Reusable buffers used by left join for output row mappings.
  BufferPtr probeOutputRowMapping_;
  vector_size_t* rawProbeOutputRowIndices_{nullptr};
  BufferPtr lookupOutputRowMapping_;
  vector_size_t* rawLookupOutputRowIndices_{nullptr};
  BufferPtr lookupOutputNulls_;
  uint64_t* rawLookupOutputNulls_{nullptr};

  // The reusable output vector for the join output.
  RowVectorPtr output_;
};
} // namespace facebook::velox::exec

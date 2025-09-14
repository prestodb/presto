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
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

class IndexLookupJoin : public Operator {
 public:
  IndexLookupJoin(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::IndexLookupJoinNode>& joinNode);

  void initialize() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool startDrain() override;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override {
    return noMoreInput_ && (numInputBatches() == 0);
  }

  void close() override;

  /// Defines lookup runtime stats.
  /// The end-to-end walltime in nanoseconds that the index connector do the
  /// lookup.
  static inline const std::string kConnectorLookupWallTime{
      "connectorLookupWallNanos"};
  /// The cpu time in nanoseconds that the index connector process response from
  /// storage client for followup processing by index join operator.
  static inline const std::string kConnectorResultPrepareTime{
      "connectorResultPrepareCpuNanos"};
  /// The cpu time in nanoseconds that the storage client process request for
  /// remote storage lookup such as encoding the lookup input data into remotr
  /// storage request.
  static inline const std::string kClientRequestProcessTime{
      "clientRequestProcessCpuNanos"};
  /// The walltime in nanoseconds that the storage client wait for the lookup
  /// from remote storage.
  static inline const std::string kClientLookupWaitWallTime{
      "clientlookupWaitWallNanos"};
  /// The number of split requests sent to remote storage for a client lookup
  /// request.
  static inline const std::string kClientNumStorageRequests{
      "clientNumStorageRequests"};
  /// The cpu time in nanoseconds that the storage client process response from
  /// remote storage lookup such as decoding the response data into velox
  /// vectors.
  static inline const std::string kClientResultProcessTime{
      "clientResultProcessCpuNanos"};
  /// The byte size of the raw result received from the remote storage lookup.
  static inline const std::string kClientLookupResultRawSize{
      "clientLookupResultRawSize"};
  /// The byte size of the result data in velox vectors that are decoded from
  /// the raw data received from the remote storage lookup.
  static inline const std::string kClientLookupResultSize{
      "clientLookupResultSize"};

 private:
  using LookupResultIter = connector::IndexSource::LookupResultIterator;
  using LookupResult = connector::IndexSource::LookupResult;

  // Contains the state of an input batch processing.
  struct InputBatchState {
    // The input batch to process.
    RowVectorPtr input;

    // If true, it indicates that the probe input has null join keys.
    bool lookupInputHasNullKeys{false};
    // Select input rows with non-null join keys.
    SelectivityVector nonNullInputRows;
    // The map from lookup input row to the corresponding probe input row. It is
    // used to handle the case that probe input has null keys.
    BufferPtr nonNullInputMappings;
    vector_size_t* rawNonNullInputMappings{nullptr};

    // The reusable vector projected from 'input' as index lookup input.
    RowVectorPtr lookupInput;
    // Used to fetch lookup results for an input batch.
    std::shared_ptr<LookupResultIter> lookupResultIter;
    // Used for synchronization with the async fetch result from index source
    // through 'lookupResultIter'.
    ContinueFuture lookupFuture;
    // Used to store the lookup result fetched from 'lookupResultIter' for
    // output processing. We might split the output result into multiple output
    // batches based on the operator's output batch size limit.
    std::unique_ptr<LookupResult> lookupResult;
    // Specifies the indices of input row in 'input' that have matches in
    // 'output' from 'lookupResult'. This is only used in case
    // 'lookupInputHasNullKeys' is true in which 'inputHits' in 'lookupResult'
    // points to rows in 'lookupInput' which might be different from 'input'.
    // To ease the rest of index lookup join result processing, we need to
    // redirect the lookup input hit row from 'lookupInput' to the corresponding
    // row in 'input' through the mapping specified by 'nonNullInputMappings'.
    // The redirect input hit indices are stored in 'resultInputHitIndices'.
    BufferPtr resultInputHitIndices;

    InputBatchState() : lookupFuture(ContinueFuture::makeEmpty()) {}

    void reset() {
      input = nullptr;
      lookupResultIter = nullptr;
      lookupFuture = ContinueFuture::makeEmpty();
      lookupResult = nullptr;
    }

    // Indicates if this input batch is empty.
    bool empty() const {
      return input == nullptr;
    }

    // Ensures that the lookup result's inputHits buffer is writable and returns
    // a mutable pointer. If the buffer is already mutable, returns it directly.
    // Otherwise, creates a new writable buffer by copying the existing data and
    // returns a pointer to the new buffer. This is needed when filters or null
    // key handling requires modifying the input hit indices.
    vector_size_t* ensureInputHitsWritable(memory::MemoryPool* pool);
  };

  void initInputBatches();
  // Initialize the lookup input and output type, and the output projections.
  void initLookupInput();
  void initLookupOutput();
  void initOutputProjections();
  void initFilter();

  // Applies the join filter directly on the lookup result, updating the
  // lookup result to only include rows that pass the filter. Returns true if
  // some rows passed the filter, otherwise false.
  bool applyFilterOnLookupResult(InputBatchState& batch);

  void ensureInputLoaded(const InputBatchState& batch);
  // Prepare index source lookup for a given 'input_'.
  void prepareLookup(InputBatchState& batch);
  void startLookup(InputBatchState& batch);

  void startLookupBlockWait();
  void endLookupBlockWait();

  RowVectorPtr getOutputFromLookupResult(InputBatchState& batch);
  RowVectorPtr produceOutputForInnerJoin(const InputBatchState& batch);
  RowVectorPtr produceOutputForLeftJoin(const InputBatchState& batch);
  // Handles production of remaining output after lookup result processing is
  // complete. For left joins, this ensures unmatched rows from the probe side
  // are included in the output with null values for lookup columns. For inner
  // joins, this simply finishes the input batch.
  RowVectorPtr produceRemainingOutput(InputBatchState& batch);
  // Produces output for the remaining input rows that has no matches from the
  // lookup at the end of current input batch processing.
  RowVectorPtr produceRemainingOutputForLeftJoin(const InputBatchState& batch);

  // Returns true if we have remaining output rows from the current
  // 'lookupResult_' after finishing processing all the output results from the
  // current 'lookupResultIter_'. For left join, we need to produce for the all
  // the input rows that have no matches in 'lookupResult_'.
  bool hasRemainingOutputForLeftJoin(const InputBatchState& batch) const;

  // Checks if we have finished processing the current 'lookupResult_'. If so,
  // call 'finishLookupResult' to reset 'lookupResult_' and corresponding
  // processing state.
  void maybeFinishLookupResult(InputBatchState& batch);
  void finishLookupResult(InputBatchState& batch);

  // Invoked after finished processing the current 'input_' batch. The function
  // resets the input batch and the lookup result states.
  void finishInput(InputBatchState& batch);

  // Prepare output row mappings for the next output batch with max size of
  // 'outputBatchSize'. This is only used by left join which needs to fill nulls
  // for output rows without lookup matches.
  void prepareOutputRowMappings(size_t outputBatchSize);

  // Prepare 'output_' for the next output batch with size of 'numOutputRows'.
  void prepareOutput(vector_size_t numOutputRows);

  // Invoked to ensure the match column is created to store the output match
  // result for the left join.
  void ensureMatchColumn(vector_size_t maxOutputRows);

  // Invoked to fill the match column and output nulls with the match result for
  // the left join.
  void
  fillOutputMatchRows(vector_size_t offset, vector_size_t size, bool match);

  // Invoked to set the match column with the actual output size.
  void setMatchColumnSize(vector_size_t numOutputRows);

  // Invoked to decode the probe input keys to detect if there are any null
  // keys.
  void decodeAndDetectNonNullKeys(InputBatchState& batch);

  // Invoked to prepare the lookup result for processing. If the probe input has
  // null keys, it maps the hit rows in lookup result to the corresponding probe
  // input rows.
  void prepareLookupResult(InputBatchState& batch);

  // Invoked at operator close to record the lookup stats.
  void recordConnectorStats();

  // Returns true if we support to fetch more than one input batch for index
  // lookup prefetch.
  bool lookupPrefetchEnabled() const {
    return maxNumInputBatches_ > 1;
  }

  // Returns the number of input batches to process.
  size_t numInputBatches() const {
    VELOX_CHECK_LE(startBatchIndex_, endBatchIndex_);
    return endBatchIndex_ - startBatchIndex_;
  }

  // Returns an empty batch for next input batch. The function also advances
  // 'endBatchIndex_'.
  InputBatchState& nextInputBatch() {
    VELOX_CHECK_LT(numInputBatches(), maxNumInputBatches_);
    VELOX_CHECK(inputBatches_[endBatchIndex_ % maxNumInputBatches_].empty());
    return inputBatches_[endBatchIndex_++ % maxNumInputBatches_];
  }

  // Returns the current processing batch.
  InputBatchState& currentInputBatch() {
    return inputBatches_[startBatchIndex_ % maxNumInputBatches_];
  }

  const InputBatchState& currentInputBatch() const {
    return inputBatches_[startBatchIndex_ % maxNumInputBatches_];
  }

  // Maximum number of rows in the output batch.
  const vector_size_t outputBatchSize_;
  // Type of join.
  const core::JoinType joinType_;
  const bool hasMarker_;
  const size_t numKeys_;
  const RowTypePtr probeType_;
  const RowTypePtr lookupType_;
  const connector::ConnectorTableHandlePtr lookupTableHandle_;
  const std::vector<core::IndexLookupConditionPtr> joinConditions_;
  const connector::ColumnHandleMap lookupColumnHandles_;
  const std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  const std::shared_ptr<connector::Connector> connector_;
  const size_t maxNumInputBatches_;

  // The lookup join plan node used to initialize this operator and reset after
  // that.
  std::shared_ptr<const core::IndexLookupJoinNode> joinNode_;

  // The data type of the lookup input including probe side columns either used
  // in equi-clauses or join conditions.
  RowTypePtr lookupInputType_;
  // The column channels in probe 'input_' referenced by 'lookupInputType_'.
  std::vector<column_index_t> lookupInputChannels_;

  // Used to decode and check if any probe-side input key or condition columns
  // have nulls.
  std::vector<std::unique_ptr<VectorHasher>> lookupKeyOrConditionHashers_;

  // The input batches to process with ranges pointed by 'startBatchIndex_' and
  // 'endBatchIndex_'.
  std::vector<InputBatchState> inputBatches_;

  // Points to the input batches in 'inputBatches_' for processing with range of
  // '[startBatchIndex_ % maxNumInputBatches_, endBatchIndex_ %
  // maxNumInputBatches_ - 1)'.
  uint64_t startBatchIndex_{0};
  uint64_t endBatchIndex_{0};

  // The data type of the lookup output from the lookup source.
  RowTypePtr lookupOutputType_;

  // Used to project output columns from the probe input and lookup output.
  std::vector<IdentityProjection> probeOutputProjections_;
  std::vector<IdentityProjection> lookupOutputProjections_;
  std::optional<column_index_t> matchOutputChannel_;

  std::shared_ptr<connector::IndexSource> indexSource_;

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

  // Join filter.
  std::unique_ptr<ExprSet> filter_;

  // Join filter input type.
  RowTypePtr filterInputType_;

  // Maps probe-side input channels to channels in 'filterInputType_'.
  std::vector<IdentityProjection> filterProbeInputProjections_;
  // Maps lookup-side input channels to channels in 'filterInputType_',
  std::vector<IdentityProjection> filterLookupInputProjections_;

  // Reusable memory for filter evaluations.
  RowVectorPtr filterInput_;
  SelectivityVector filterRows_;
  std::vector<VectorPtr> filterResult_;
  DecodedVector decodedFilterResult_;
  BufferPtr filteredIndices_;

  // The reusable output vector for the join output.
  RowVectorPtr output_;
  FlatVectorPtr<bool> matchColumn_{nullptr};
  uint64_t* rawMatchValues_{nullptr};

  // The start time of the current lookup driver block wait, and reset after the
  // driver wait completes.
  std::optional<size_t> blockWaitStartNs_;
};
} // namespace facebook::velox::exec

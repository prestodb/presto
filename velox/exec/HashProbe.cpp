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

#include "velox/exec/HashProbe.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/ControlExpr.h"

namespace facebook::velox::exec {

namespace {

// Returns the type for the hash table row. Build side keys first,
// then dependent build side columns.
std::shared_ptr<const RowType> makeTableType(
    const RowType* type,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        keys) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::unordered_set<ChannelIndex> keyChannels(keys.size());
  names.reserve(type->size());
  types.reserve(type->size());
  for (const auto& key : keys) {
    auto channel = type->getChildIdx(key->name());
    names.emplace_back(type->nameOf(channel));
    types.emplace_back(type->childAt(channel));
    keyChannels.insert(channel);
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (keyChannels.find(i) == keyChannels.end()) {
      names.emplace_back(type->nameOf(i));
      types.emplace_back(type->childAt(i));
    }
  }
  return std::make_shared<RowType>(std::move(names), std::move(types));
}
} // namespace

HashProbe::HashProbe(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::HashJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "HashProbe"),
      outputBatchSize_{driverCtx->queryConfig().preferredOutputBatchSize()},
      joinType_{joinNode->joinType()},
      filterResult_(1),
      outputRows_(outputBatchSize_) {
  auto probeType = joinNode->sources()[0]->outputType();
  auto numKeys = joinNode->leftKeys().size();
  keyChannels_.reserve(numKeys);
  hashers_.reserve(numKeys);
  for (auto& key : joinNode->leftKeys()) {
    auto channel = exprToChannel(key.get(), probeType);
    keyChannels_.emplace_back(channel);
    hashers_.push_back(
        std::make_unique<VectorHasher>(probeType->childAt(channel), channel));
  }
  lookup_ = std::make_unique<HashLookup>(hashers_);
  auto buildType = joinNode->sources()[1]->outputType();
  auto tableType = makeTableType(buildType.get(), joinNode->rightKeys());
  if (joinNode->filter()) {
    initializeFilter(joinNode->filter(), probeType, tableType);
  }

  size_t countIdentityProjection = 0;
  for (auto i = 0; i < probeType->size(); ++i) {
    auto name = probeType->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (outIndex.has_value()) {
      identityProjections_.emplace_back(i, outIndex.value());
      if (outIndex.value() == i) {
        countIdentityProjection++;
      }
    }
  }

  for (ChannelIndex i = 0; i < outputType_->size(); ++i) {
    auto tableChannel = tableType->getChildIdxIfExists(outputType_->nameOf(i));
    if (tableChannel.has_value()) {
      tableResultProjections_.emplace_back(tableChannel.value(), i);
    }
  }

  if (countIdentityProjection == probeType->size() &&
      tableResultProjections_.empty()) {
    isIdentityProjection_ = true;
  }
}

void HashProbe::initializeFilter(
    const std::shared_ptr<const core::ITypedExpr>& filter,
    const RowTypePtr& probeType,
    const RowTypePtr& tableType) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> filters = {filter};
  filter_ =
      std::make_unique<ExprSet>(std::move(filters), operatorCtx_->execCtx());

  ChannelIndex filterChannel = 0;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  auto numFields = filter_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);
  for (auto& field : filter_->expr(0)->distinctFields()) {
    const auto& name = field->field();
    auto channel = probeType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterProbeInputs_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(probeType->nameOf(channelValue));
      types.emplace_back(probeType->childAt(channelValue));
      continue;
    }
    channel = tableType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterBuildInputs_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(tableType->nameOf(channelValue));
      types.emplace_back(tableType->childAt(channelValue));
      continue;
    }
    VELOX_FAIL(
        "Join filter field {} not in probe or build input", field->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

BlockingReason HashProbe::isBlocked(ContinueFuture* future) {
  if (table_) {
    return BlockingReason::kNotBlocked;
  }

  auto hashBuildResult =
      operatorCtx_->task()
          ->getHashJoinBridge(
              operatorCtx_->driverCtx()->splitGroupId, planNodeId())
          ->tableOrFuture(future);
  if (!hashBuildResult.has_value()) {
    return BlockingReason::kWaitForJoinBuild;
  }

  if (hashBuildResult->antiJoinHasNullKeys) {
    // Anti join with null keys on the build side always returns nothing.
    VELOX_CHECK(isAntiJoin(joinType_));
    finished_ = true;
  } else {
    table_ = hashBuildResult->table;
    if (table_->numDistinct() == 0) {
      // Build side is empty. Inner, right and semi joins return nothing in this
      // case, hence, we can terminate the pipeline early.
      if (isInnerJoin(joinType_) || isSemiJoin(joinType_) ||
          isRightJoin(joinType_)) {
        finished_ = true;
      }
    } else if (
        (isInnerJoin(joinType_) || isSemiJoin(joinType_)) &&
        table_->hashMode() != BaseHashTable::HashMode::kHash) {
      // Find out whether there are any upstream operators that can accept
      // dynamic filters on all or a subset of the join keys. Create dynamic
      // filters to push down.
      const auto& buildHashers = table_->hashers();
      auto channels = operatorCtx_->driverCtx()->driver->canPushdownFilters(
          this, keyChannels_);
      for (auto i = 0; i < keyChannels_.size(); i++) {
        if (channels.find(keyChannels_[i]) != channels.end()) {
          if (auto filter = buildHashers[i]->getFilter(false)) {
            dynamicFilters_.emplace(keyChannels_[i], std::move(filter));
          }
        }
      }
    }
  }

  return BlockingReason::kNotBlocked;
}

void HashProbe::clearDynamicFilters() {
  // The join can be completely replaced with a pushed down
  // filter when the following conditions are met:
  //  * hash table has a single key with unique values,
  //  * build side has no dependent columns.
  if (keyChannels_.size() == 1 && !table_->hasDuplicateKeys() &&
      tableResultProjections_.empty() && !filter_ && !dynamicFilters_.empty()) {
    canReplaceWithDynamicFilter_ = true;
  }

  Operator::clearDynamicFilters();
}

void HashProbe::addInput(RowVectorPtr input) {
  input_ = std::move(input);

  if (canReplaceWithDynamicFilter_) {
    replacedWithDynamicFilter_ = true;
    return;
  }

  if (table_->numDistinct() == 0) {
    // Build side is empty. This state is valid only for anti, left and full
    // joins.
    VELOX_CHECK(
        isAntiJoin(joinType_) || isLeftJoin(joinType_) ||
        isFullJoin(joinType_));
    return;
  }

  nonNullRows_.resize(input_->size());
  nonNullRows_.setAll();
  deselectRowsWithNulls(
      *input_, keyChannels_, nonNullRows_, *operatorCtx_->execCtx());

  activeRows_ = nonNullRows_;
  lookup_->hashes.resize(input_->size());
  auto mode = table_->hashMode();
  auto& buildHashers = table_->hashers();
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    auto key = input_->loadedChildAt(keyChannels_[i]);
    if (mode != BaseHashTable::HashMode::kHash) {
      buildHashers[i]->lookupValueIds(
          *key, activeRows_, scratchMemory_, lookup_->hashes);
    } else {
      hashers_[i]->hash(*key, activeRows_, i > 0, lookup_->hashes);
    }
  }
  lookup_->rows.clear();
  if (activeRows_.isAllSelected()) {
    lookup_->rows.resize(activeRows_.size());
    std::iota(lookup_->rows.begin(), lookup_->rows.end(), 0);
  } else {
    bits::forEachSetBit(
        activeRows_.asRange().bits(),
        0,
        activeRows_.size(),
        [&](vector_size_t row) { lookup_->rows.push_back(row); });
  }
  if (lookup_->rows.empty()) {
    if (joinType_ != core::JoinType::kAnti) {
      input_ = nullptr;
    }
    return;
  }
  passingInputRowsInitialized_ = false;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
    // Make sure to allocate an entry in 'hits' for every input row to allow for
    // including rows without a match in the output. Also, make sure to
    // initialize all 'hits' to nullptr as HashTable::joinProbe will only
    // process activeRows_.
    auto numInput = input_->size();
    auto& hits = lookup_->hits;
    hits.resize(numInput);
    std::fill(hits.data(), hits.data() + numInput, nullptr);
    table_->joinProbe(*lookup_);

    // Update lookup_->rows to include all input rows, not just activeRows_ as
    // we need to include all rows in the output.
    auto& rows = lookup_->rows;
    rows.resize(numInput);
    std::iota(rows.begin(), rows.end(), 0);
    results_.reset(*lookup_);
  } else {
    lookup_->hits.resize(lookup_->rows.back() + 1);
    table_->joinProbe(*lookup_);
    results_.reset(*lookup_);
  }
}

namespace {
// Copy values from 'rows' of 'table' according to 'projections' in
// 'result'. Reuses 'result' children where possible.
void extractColumns(
    BaseHashTable* table,
    folly::Range<char**> rows,
    folly::Range<const IdentityProjection*> projections,
    memory::MemoryPool* pool,
    const RowVectorPtr& result) {
  for (auto projection : projections) {
    auto& child = result->childAt(projection.outputChannel);
    // TODO: Consider reuse of complex types.
    if (!child || !BaseVector::isReusableFlatVector(child)) {
      child = BaseVector::create(
          result->type()->childAt(projection.outputChannel), rows.size(), pool);
    }
    child->resize(rows.size());
    table->rows()->extractColumn(
        rows.data(), rows.size(), projection.inputChannel, child);
  }
}

folly::Range<vector_size_t*> initializeRowNumberMapping(
    BufferPtr& mapping,
    vector_size_t size,
    memory::MemoryPool* pool) {
  if (!mapping || !mapping->unique() ||
      mapping->size() < sizeof(vector_size_t) * size) {
    mapping = allocateIndices(size, pool);
  }
  return folly::Range(mapping->asMutable<vector_size_t>(), size);
}
} // namespace

void HashProbe::prepareOutput(vector_size_t size) {
  // Try to re-use memory for the output vectors that contain build-side data.
  // We expect output vectors containing probe-side data to be null (reset in
  // clearIdentityProjectedOutput). BaseVector::prepareForReuse keeps null
  // children unmodified and makes non-null (build side) children reusable.
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, size);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, size, pool()));
  }
}

void HashProbe::fillOutput(vector_size_t size) {
  prepareOutput(size);

  for (auto projection : identityProjections_) {
    // Load input vector if it is being split into multiple batches. It is not
    // safe to wrap unloaded LazyVector into two different dictionaries.
    ensureLoadedIfNotAtEnd(projection.inputChannel);
    auto inputChild = input_->childAt(projection.inputChannel);

    output_->childAt(projection.outputChannel) =
        wrapChild(size, rowNumberMapping_, inputChild);
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputRows_.data(), size),
      tableResultProjections_,
      pool(),
      output_);
}

RowVectorPtr HashProbe::getNonMatchingOutputForRightJoin() {
  if (!lastRightJoinProbe_) {
    return nullptr;
  }

  outputRows_.resize(outputBatchSize_);
  auto numOut = table_->listNotProbedRows(
      &rightJoinIterator_,
      outputBatchSize_,
      RowContainer::kUnlimited,
      outputRows_.data());
  if (!numOut) {
    return nullptr;
  }

  prepareOutput(numOut);

  // Populate probe-side columns of the output with nulls.
  for (auto projection : identityProjections_) {
    output_->childAt(projection.outputChannel) = BaseVector::createNullConstant(
        outputType_->childAt(projection.outputChannel), numOut, pool());
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputRows_.data(), numOut),
      tableResultProjections_,
      pool(),
      output_);
  return output_;
}

void HashProbe::clearIdentityProjectedOutput() {
  if (!output_ || !output_.unique()) {
    return;
  }
  for (auto& projection : identityProjections_) {
    output_->childAt(projection.outputChannel) = nullptr;
  }
}

RowVectorPtr HashProbe::getOutput() {
  clearIdentityProjectedOutput();
  if (!input_) {
    if (noMoreInput_ && (isRightJoin(joinType_) || isFullJoin(joinType_))) {
      auto output = getNonMatchingOutputForRightJoin();
      if (output == nullptr) {
        finished_ = true;
      }
      return output;
    }
    if (noMoreInput_) {
      finished_ = true;
    }
    return nullptr;
  }

  const auto inputSize = input_->size();

  if (replacedWithDynamicFilter_) {
    stats_.addRuntimeStat(
        "replacedWithDynamicFilterRows", RuntimeCounter(inputSize));
    auto output = Operator::fillOutput(inputSize, nullptr);
    input_ = nullptr;
    return output;
  }

  const bool isSemiOrAntiJoin =
      core::isSemiJoin(joinType_) || core::isAntiJoin(joinType_);

  const bool emptyBuildSide = (table_->numDistinct() == 0);

  // Semi and anti joins are always cardinality reducing, e.g. for a given row
  // of input they produce zero or 1 row of output. Therefore, we can process
  // each batch of input in one go.
  auto outputBatchSize =
      (isSemiOrAntiJoin || emptyBuildSide) ? inputSize : outputBatchSize_;
  auto mapping =
      initializeRowNumberMapping(rowNumberMapping_, outputBatchSize, pool());
  outputRows_.resize(outputBatchSize);

  for (;;) {
    int numOut = 0;

    if (emptyBuildSide) {
      // When build side is empty, anti and left joins return all probe side
      // rows, including ones with null join keys.
      std::iota(mapping.begin(), mapping.end(), 0);
      numOut = inputSize;
    } else if (isAntiJoin(joinType_)) {
      // When build side is not empty, anti join returns probe rows with no
      // nulls in the join key and no match in the build side.
      for (auto i = 0; i < inputSize; i++) {
        if (nonNullRows_.isValid(i) &&
            (!activeRows_.isValid(i) || !lookup_->hits[i])) {
          mapping[numOut] = i;
          ++numOut;
        }
      }
    } else {
      numOut = table_->listJoinResults(
          results_,
          isLeftJoin(joinType_) || isFullJoin(joinType_),
          mapping,
          folly::Range(outputRows_.data(), outputRows_.size()));
    }

    if (!numOut) {
      input_ = nullptr;
      return nullptr;
    }
    VELOX_CHECK_LE(numOut, outputRows_.size());

    numOut = evalFilter(numOut);
    if (!numOut) {
      // The filter was false on all rows.
      if (isSemiOrAntiJoin) {
        input_ = nullptr;
        return nullptr;
      }
      continue;
    }

    if (isRightJoin(joinType_) || isFullJoin(joinType_)) {
      // Mark build-side rows that have a match on the join condition.
      table_->rows()->setProbedFlag(outputRows_.data(), numOut);
    }

    fillOutput(numOut);

    if (isSemiOrAntiJoin || emptyBuildSide) {
      input_ = nullptr;
    }
    return output_;
  }
}

void HashProbe::fillFilterInput(vector_size_t size) {
  if (!filterInput_) {
    filterInput_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(filterInputType_, 1, pool()));
  }
  filterInput_->resize(size);
  for (auto projection : filterProbeInputs_) {
    ensureLoadedIfNotAtEnd(projection.inputChannel);
    filterInput_->childAt(projection.outputChannel) = wrapChild(
        size, rowNumberMapping_, input_->childAt(projection.inputChannel));
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputRows_.data(), size),
      filterBuildInputs_,
      pool(),
      filterInput_);
}

int32_t HashProbe::evalFilter(int32_t numRows) {
  if (!filter_) {
    return numRows;
  }
  fillFilterInput(numRows);
  filterRows_.resize(numRows);
  filterRows_.setAll();

  EvalCtx evalCtx(operatorCtx_->execCtx(), filter_.get(), filterInput_.get());
  filter_->eval(0, 1, true, filterRows_, &evalCtx, &filterResult_);

  decodedFilterResult_.decode(*filterResult_[0], filterRows_);

  int32_t numPassed = 0;
  auto rawMapping = rowNumberMapping_->asMutable<vector_size_t>();
  if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
    // Identify probe rows which got filtered out and add them back with nulls
    // for build side.
    auto addMiss = [&](auto row) {
      outputRows_[numPassed] = nullptr;
      rawMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      const bool passed = !decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i);
      leftJoinTracker_.advance(rawMapping[i], passed, addMiss);
      if (passed) {
        outputRows_[numPassed] = outputRows_[i];
        rawMapping[numPassed++] = rawMapping[i];
      }
    }
    if (results_.atEnd()) {
      leftJoinTracker_.finish(addMiss);
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      if (!decodedFilterResult_.isNullAt(i) &&
          decodedFilterResult_.valueAt<bool>(i)) {
        outputRows_[numPassed] = outputRows_[i];
        rawMapping[numPassed++] = rawMapping[i];
      }
    }
  }
  return numPassed;
}

void HashProbe::ensureLoadedIfNotAtEnd(ChannelIndex channel) {
  if (core::isSemiJoin(joinType_) || core::isAntiJoin(joinType_) ||
      results_.atEnd()) {
    return;
  }

  if (!passingInputRowsInitialized_) {
    passingInputRowsInitialized_ = true;
    passingInputRows_.resize(input_->size());
    if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
      passingInputRows_.setAll();
    } else {
      passingInputRows_.clearAll();
      auto hitsSize = lookup_->hits.size();
      auto hits = lookup_->hits.data();
      for (auto i = 0; i < hitsSize; ++i) {
        if (hits[i]) {
          passingInputRows_.setValid(i, true);
        }
      }
    }
    passingInputRows_.updateBounds();
  }

  LazyVector::ensureLoadedRows(input_->childAt(channel), passingInputRows_);
}

void HashProbe::noMoreInput() {
  Operator::noMoreInput();
  if (isRightJoin(joinType_) || isFullJoin(joinType_)) {
    std::vector<ContinuePromise> promises;
    std::vector<std::shared_ptr<Driver>> peers;
    // The last Driver to hit HashProbe::finish is responsible for producing
    // non-matching build-side rows for the right join.
    ContinueFuture future{false};
    if (!operatorCtx_->task()->allPeersFinished(
            planNodeId(), operatorCtx_->driver(), &future, promises, peers)) {
      return;
    }

    lastRightJoinProbe_ = true;
  }
}

bool HashProbe::isFinished() {
  return finished_;
}
} // namespace facebook::velox::exec

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

#include "velox/dwio/dwrf/reader/FlatMapColumnReader.h"
#include <folly/Conv.h>
#include <folly/container/F14Set.h>
#include <folly/json.h>

#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/FlatMapHelper.h"

namespace facebook::velox::dwrf {

using dwio::common::TypeWithId;
using memory::MemoryPool;
using namespace dwio::common::flatmap;

StringKeyBuffer::StringKeyBuffer(
    MemoryPool& pool,
    const std::vector<std::unique_ptr<KeyNode<StringView>>>& nodes)
    : data_{pool, nodes.size() + 1} {
  uint64_t size = 0;
  for (auto& node : nodes) {
    auto& str = node->getKey().get();
    size += str.size();
  }
  buffer_ = AlignedBuffer::allocate<char>(size, &pool);

  auto data = buffer_->asMutable<char>();
  uint32_t ordinal = 0;
  for (auto& node : nodes) {
    node->setOrdinal(ordinal);
    auto& str = node->getKey().get();
    std::memcpy(data, str.data(), str.size());
    data_[ordinal++] = data;
    data += str.size();
  }
  data_[ordinal] = data;
}

namespace {

template <typename T>
KeyValue<T> extractKey(const proto::KeyInfo& info) {
  return KeyValue<T>(info.intkey());
}

template <>
inline KeyValue<StringView> extractKey<StringView>(const proto::KeyInfo& info) {
  return KeyValue<StringView>(StringView(info.byteskey()));
}

template <typename T>
KeyPredicate<T> prepareKeyPredicate(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    StripeStreams& stripe) {
  auto& cs = stripe.getColumnSelector();
  const auto expr = cs.getNode(requestedType->id())->getNode().expression;
  return dwio::common::flatmap::prepareKeyPredicate<T>(expr);
}

// Helper function to perform uniqueness checks before visiting streams
uint32_t visitUniqueStreamsOfNode(
    const std::shared_ptr<const TypeWithId>& fileType,
    StripeStreams& stripe,
    std::function<void(const StreamInformation&)> visitor) {
  const auto dataValueType = fileType->childAt(1);
  folly::F14FastSet<size_t> processed;

  auto streams = stripe.visitStreamsOfNode(
      dataValueType->id(), [&](const StreamInformation& stream) {
        auto sequence = stream.getSequence();
        // No need to load shared dictionary stream here.
        if (sequence == 0 || processed.count(sequence)) {
          return;
        }
        processed.insert(sequence);

        visitor(stream);
      });

  return streams;
}

void triggerKeySelectionNotification(
    const FlatMapContext& context,
    const facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats&
        keySelectionStats) {
  if (!context.keySelectionCallback) {
    return;
  }
  context.keySelectionCallback(keySelectionStats);
}

template <typename T>
std::vector<std::unique_ptr<KeyNode<T>>> getKeyNodesFiltered(
    const std::function<bool(const KeyValue<T>&)>& keyPredicate,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& fileType,
    StripeStreams& stripe,
    const StreamLabels& streamLabels,
    memory::MemoryPool& memoryPool,
    FlatMapContext& flatMapContext) {
  std::vector<std::unique_ptr<KeyNode<T>>> keyNodes;

  auto keySelectionStats =
      facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats{};

  const auto requestedValueType = requestedType->childAt(1);
  const auto dataValueType = fileType->childAt(1);
  folly::F14FastSet<size_t> processed;

  // load all sub streams
  // fetch reader, in map bitmap and key object.
  auto streams = visitUniqueStreamsOfNode(
      fileType, stripe, [&](const StreamInformation& stream) {
        auto sequence = stream.getSequence();
        // No need to load shared dictionary stream here.
        if (sequence == 0 || processed.count(sequence)) {
          return;
        }
        processed.insert(sequence);

        EncodingKey seqEk(dataValueType->id(), sequence);
        const auto& keyInfo = stripe.getEncoding(seqEk).key();
        auto key = extractKey<T>(keyInfo);
        // if key filter not passed through read schema
        if (!keyPredicate(key)) {
          return;
        }

        // fetch reader, in map bitmap and key object.
        auto labels = streamLabels.append(folly::to<std::string>(key.get()));
        auto inMap = stripe.getStream(
            seqEk.forKind(proto::Stream_Kind_IN_MAP), labels.label(), true);
        DWIO_ENSURE_NOT_NULL(inMap, "In map stream is required");
        // build seekable
        auto inMapDecoder = createBooleanRleDecoder(std::move(inMap), seqEk);

        auto valueReader = ColumnReader::build(
            requestedValueType,
            dataValueType,
            stripe,
            labels,
            nullptr,
            0,
            FlatMapContext{
                .sequence = sequence,
                .inMapDecoder = inMapDecoder.get(),
                .keySelectionCallback = nullptr});

        keyNodes.push_back(std::make_unique<KeyNode<T>>(
            std::move(valueReader),
            std::move(inMapDecoder),
            key,
            sequence,
            memoryPool));
      });

  keySelectionStats.selectedKeys = keyNodes.size();
  keySelectionStats.totalKeys = processed.size();

  triggerKeySelectionNotification(flatMapContext, keySelectionStats);

  VLOG(1) << "[Flat-Map] Initialized a flat-map column reader for node "
          << fileType->id() << ", keys=" << keyNodes.size()
          << ", streams=" << streams;
  return keyNodes;
}

template <typename T>
std::vector<std::unique_ptr<KeyNode<T>>> rearrangeKeyNodesAsProjectedOrder(
    std::vector<std::unique_ptr<KeyNode<T>>>& availableKeyNodes,
    const std::vector<std::string>& keys) {
  std::vector<std::unique_ptr<KeyNode<T>>> keyNodes(keys.size());

  std::unordered_map<KeyValue<T>, size_t, KeyValueHash<T>> keyLookup;
  for (size_t i = 0; i < keys.size(); ++i) {
    keyLookup[parseKeyValue<T>(keys[i])] = i;
  }

  for (auto& keyNode : availableKeyNodes) {
    const auto it = keyLookup.find(keyNode->getKey());
    if (it == keyLookup.end()) {
      continue;
    }
    keyNodes[it->second] = std::move(keyNode);
  }

  return keyNodes;
}

} // namespace

template <typename T>
FlatMapColumnReader<T>::FlatMapColumnReader(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& fileType,
    StripeStreams& stripe,
    const StreamLabels& streamLabels,
    folly::Executor* executor,
    size_t decodingParallelismFactor,
    FlatMapContext flatMapContext)
    : ColumnReader(fileType, stripe, streamLabels, std::move(flatMapContext)),
      requestedType_{requestedType},
      returnFlatVector_{stripe.getRowReaderOptions().getReturnFlatVector()},
      executor_{executor} {
  DWIO_ENSURE_EQ(fileType_->id(), fileType->id());

  const auto keyPredicate = prepareKeyPredicate<T>(requestedType, stripe);

  keyNodes_ = getKeyNodesFiltered<T>(
      [&keyPredicate](const auto& keyValue) { return keyPredicate(keyValue); },
      requestedType,
      fileType,
      stripe,
      streamLabels,
      memoryPool_,
      flatMapContext_);

  parallelForOnKeyNodes_ = std::make_unique<dwio::common::ParallelFor>(
      executor_, 0, keyNodes_.size(), decodingParallelismFactor);

  // sort nodes by sequence id so order of keys is fixed
  std::sort(keyNodes_.begin(), keyNodes_.end(), [](auto& a, auto& b) {
    return a->getSequence() < b->getSequence();
  });

  initStringKeyBuffer();
}

template <typename T>
uint64_t FlatMapColumnReader<T>::skip(uint64_t numValues) {
  // skip basic rows
  numValues = ColumnReader::skip(numValues);

  // skip every single node
  for (auto& node : keyNodes_) {
    node->skip(numValues);
  }

  return numValues;
}

template <typename T>
void FlatMapColumnReader<T>::initKeysVector(
    VectorPtr& vector,
    vector_size_t size) {
  initializeFlatVector<T>(
      vector,
      memoryPool_,
      requestedType_->type()->asMap().keyType(),
      size,
      false);
  vector->resize(size, false);
}

template <>
void FlatMapColumnReader<StringView>::initKeysVector(
    VectorPtr& vector,
    vector_size_t size) {
  initializeFlatVector<StringView>(
      vector,
      memoryPool_,
      VARCHAR(),
      size,
      false,
      std::vector<BufferPtr>{stringKeyBuffer_->getBuffer()});
  vector->resize(size, false);
}

// When read-string-as-row flag is on, string readers produce ROW(BIGINT,
// BIGINT) type instead of VARCHAR or VARBINARY. In these cases,
// requestedType_->type is not the right type of the final vector.
const std::shared_ptr<const Type> getMapType(
    const VectorPtr& keysVector,
    const VectorPtr& valuesVector,
    const std::shared_ptr<const Type> backupType) {
  return (keysVector == nullptr || valuesVector == nullptr)
      ? backupType
      : MAP(keysVector->type(), valuesVector->type());
}

template <typename T>
void FlatMapColumnReader<T>::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto mapVector = detail::resetIfWrongVectorType<MapVector>(result);
  VectorPtr keysVector;
  VectorPtr valuesVector;
  BufferPtr offsets;
  BufferPtr lengths;
  if (mapVector) {
    keysVector = mapVector->mapKeys();
    if (returnFlatVector_) {
      valuesVector = mapVector->mapValues();
    }
    offsets = mapVector->mutableOffsets(numValues);
    lengths = mapVector->mutableSizes(numValues);
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  uint64_t nullCount = nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;

  // Release extra references of keys and values vectors as well as nulls,
  // offsets and lengths buffers.
  result.reset();

  if (!offsets) {
    offsets = AlignedBuffer::allocate<vector_size_t>(numValues, &memoryPool_);
  }
  if (!lengths) {
    lengths = AlignedBuffer::allocate<vector_size_t>(numValues, &memoryPool_);
  }

  auto nonNullMaps = numValues - nullCount;

  // opt - only loop over nodes that have value
  std::vector<KeyNode<T>*> nodes;
  utils::BulkBitIterator<char> bulkInMapIter{};
  std::vector<const BaseVector*> nodeBatches;
  std::vector<const BaseVector*> batches(keyNodes_.size(), nullptr);
  size_t totalChildren = 0;
  if (nonNullMaps > 0) {
    auto keyNodes_sz = keyNodes_.size();
    nodeBatches.reserve(keyNodes_sz);
    nodes.reserve(keyNodes_sz);
    parallelForOnKeyNodes_->execute(
        [&](size_t i) { batches[i] = keyNodes_[i]->load(nonNullMaps); });
    for (size_t i = 0; i < keyNodes_sz; ++i) {
      auto& batch = batches[i];
      auto& node = keyNodes_[i];
      if (batch) {
        nodes.emplace_back(node.get());
        node->addToBulkInMapBitIterator(bulkInMapIter);
        nodeBatches.push_back(batch);
        totalChildren += batch->size();
      }
    }
  }

  if (nodeBatches.empty()) {
    auto* offsetsPtr = offsets->asMutable<vector_size_t>();
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();
    std::fill(offsetsPtr, offsetsPtr + numValues, 0);
    std::fill(lengthsPtr, lengthsPtr + numValues, 0);

    // todo: refactor this method to reduce branching
    result = std::make_shared<MapVector>(
        &memoryPool_,
        getMapType(keysVector, valuesVector, requestedType_->type()),
        std::move(nulls),
        numValues,
        std::move(offsets),
        std::move(lengths),
        nullptr,
        nullptr,
        nullCount);
    return;
  }

  size_t startIndices[nodeBatches.size()];
  size_t nodeIndices[nodeBatches.size()];

  auto& mapValueType = requestedType_->type()->asMap().valueType();
  if (totalChildren > 0) {
    size_t childOffset = 0;
    for (size_t i = 0; i < nodeBatches.size(); i++) {
      nodeIndices[i] = 0;
      startIndices[i] = childOffset;
      childOffset += nodeBatches[i]->size();
    }

    initKeysVector(keysVector, totalChildren);
    initializeVector(valuesVector, mapValueType, memoryPool_, nodeBatches);

    if (!returnFlatVector_) {
      for (auto batch : nodeBatches) {
        valuesVector->append(batch);
      }
    }
  }

  BufferPtr indices;
  vector_size_t* indicesPtr = nullptr;
  if (!returnFlatVector_) {
    indices = AlignedBuffer::allocate<int32_t>(totalChildren, &memoryPool_);
    indices->setSize(totalChildren * sizeof(vector_size_t));
    indicesPtr = indices->asMutable<vector_size_t>();
  }

  // now we're doing the rotation concat for sure to fill data
  vector_size_t offset = 0;
  auto* offsetsPtr = offsets->asMutable<vector_size_t>();
  auto* lengthsPtr = lengths->asMutable<vector_size_t>();
  for (uint64_t i = 0; i < numValues; ++i) {
    // case for having map on this row
    offsetsPtr[i] = offset;
    if (!nullsPtr || !bits::isBitNull(nullsPtr, i)) {
      bulkInMapIter.loadNext();
      for (size_t j = 0; j < nodes.size(); j++) {
        if (bulkInMapIter.hasValueAt(j)) {
          nodes[j]->fillKeysVector(keysVector, offset, stringKeyBuffer_.get());
          if (returnFlatVector_) {
            copyOne(
                mapValueType,
                *valuesVector,
                offset,
                *nodeBatches[j],
                nodeIndices[j]);
          } else {
            indicesPtr[offset] = startIndices[j] + nodeIndices[j];
          }
          offset++;
          nodeIndices[j]++;
        }
      }
    }

    lengthsPtr[i] = offset - offsetsPtr[i];
  }

  DWIO_ENSURE_EQ(totalChildren, offset, "fill the same amount of items");

  VLOG(1) << "[Flat-Map] num elements: " << numValues
          << ", total children: " << totalChildren;

  if (totalChildren > 0 && !returnFlatVector_) {
    valuesVector = BaseVector::wrapInDictionary(
        nullptr, indices, totalChildren, std::move(valuesVector));
  }

  auto mapType = getMapType(keysVector, valuesVector, requestedType_->type());

  // TODO Reuse
  result = std::make_shared<MapVector>(
      &memoryPool_,
      mapType,
      std::move(nulls),
      numValues,
      std::move(offsets),
      std::move(lengths),
      std::move(keysVector),
      std::move(valuesVector),
      nullCount);
}

template <>
void FlatMapColumnReader<StringView>::initStringKeyBuffer() {
  stringKeyBuffer_ = std::make_unique<StringKeyBuffer>(memoryPool_, keyNodes_);
}

template <typename T>
uint64_t KeyNode<T>::readInMapData(uint64_t numValues) {
  // load a batch for current request
  inMapData_.reserve(bits::nwords(numValues) * 8);
  auto data = inMapData_.data();
  inMap_->next(data, numValues, nullptr);
  return bits::countBits(reinterpret_cast<uint64_t*>(data), 0, numValues);
}

// skip number of values in this node
template <typename T>
void KeyNode<T>::skip(uint64_t numValues) {
  auto toSkip = readInMapData(numValues);
  reader_->skip(toSkip);
}

template <typename T>
BaseVector* KeyNode<T>::load(uint64_t numValues) {
  DWIO_ENSURE_GT(numValues, 0, "numValues should be positive");
  auto numItems = readInMapData(numValues);

  // we're going to load next count of data
  if (numItems > 0) {
    reader_->next(numItems, vector_);
    DWIO_ENSURE_EQ(numItems, vector_->size(), "items loaded assurance");
    return vector_.get();
  }
  return nullptr;
}

template <typename T>
void KeyNode<T>::loadAsChild(
    VectorPtr& vec,
    uint64_t numValues,
    BufferPtr& mergedNulls,
    uint64_t nonNullMaps,
    const uint64_t* nulls) {
  DWIO_ENSURE_GT(numValues, 0, "numValues should be positive");
  reader_->next(
      numValues, vec, mergeNulls(numValues, mergedNulls, nonNullMaps, nulls));
  DWIO_ENSURE_EQ(numValues, vec->size(), "items loaded assurance");
}

template <typename T>
const uint64_t* KeyNode<T>::mergeNulls(
    uint64_t numValues,
    BufferPtr& mergedNulls,
    uint64_t nonNullMaps,
    const uint64_t* nulls) {
  const auto numItems = readInMapData(nonNullMaps);
  if (numItems == 0) {
    return getAllNulls(numValues, mergedNulls);
  }

  const auto inmapNulls = reinterpret_cast<uint64_t*>(inMapData_.data());
  if (nulls == nullptr) {
    return inmapNulls;
  }

  auto* result = ensureNullBuffer(numValues, mergedNulls);

  uint64_t offsetInMap = 0;

  for (uint64_t i = 0; i < numValues; ++i) {
    bits::setNull(
        result,
        i,
        bits::isBitNull(nulls, i) ? bits::kNotNull
                                  : bits::isBitNull(inmapNulls, offsetInMap++));
  }

  return result;
}

template <typename T>
uint64_t* KeyNode<T>::ensureNullBuffer(
    uint64_t numValues,
    BufferPtr& mergedNulls) {
  const auto numBytes = bits::nbytes(numValues);
  if (!mergedNulls || mergedNulls->capacity() < numBytes) {
    mergedNulls = AlignedBuffer::allocate<bool>(numValues, &memoryPool_);
  }
  return mergedNulls->asMutable<uint64_t>();
}

template <typename T>
const uint64_t* KeyNode<T>::getAllNulls(
    uint64_t numValues,
    BufferPtr& mergedNulls) {
  auto* result = ensureNullBuffer(numValues, mergedNulls);
  for (uint64_t i = 0; i < numValues; ++i) {
    bits::setNull(result, i, true);
  }
  return result;
}

template <>
void KeyNode<StringView>::fillKeysVector(
    VectorPtr& vector,
    vector_size_t offset,
    const StringKeyBuffer* buffer) {
  // Ideally, this should be dynamic_cast, but we would like to avoid the
  // cost. We cannot make vector type template variable because for string
  // type, we will have two different vector representations
  auto& flatVec = static_cast<FlatVector<StringView>&>(*vector);
  buffer->fillKey(ordinal_, [&](auto data, auto size) {
    const_cast<StringView*>(flatVec.rawValues())[offset] =
        StringView{data, static_cast<int32_t>(size)};
  });
}

template <typename T>
std::vector<std::unique_ptr<KeyNode<T>>> getKeyNodesForStructEncoding(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& fileType,
    StripeStreams& stripe,
    const StreamLabels& streamLabels,
    memory::MemoryPool& memoryPool,
    FlatMapContext& flatMapContext) {
  // `KeyNode` is ordered based on the projection. So if [3, 2, 1] is
  // projected, the vector of key node will be created [3, 2, 1].
  // If the key is not found in the stripe, the key node will be nullptr.

  auto keyPredicate = prepareKeyPredicate<T>(requestedType, stripe);

  auto availableKeyNodes = getKeyNodesFiltered<T>(
      [&keyPredicate](const auto& keyValue) { return keyPredicate(keyValue); },
      requestedType,
      fileType,
      stripe,
      streamLabels,
      memoryPool,
      flatMapContext);

  const auto& mapColumnIdAsStruct =
      stripe.getRowReaderOptions().getMapColumnIdAsStruct();
  auto it = mapColumnIdAsStruct.find(requestedType->id());
  DWIO_ENSURE(it != mapColumnIdAsStruct.end());

  return rearrangeKeyNodesAsProjectedOrder<T>(availableKeyNodes, it->second);
}

template <typename T>
FlatMapStructEncodingColumnReader<T>::FlatMapStructEncodingColumnReader(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& fileType,
    StripeStreams& stripe,
    const StreamLabels& streamLabels,
    folly::Executor* executor,
    size_t decodingParallelismFactor,
    FlatMapContext flatMapContext)
    : ColumnReader(
          requestedType,
          stripe,
          streamLabels,
          std::move(flatMapContext)),
      requestedType_{requestedType},
      keyNodes_{getKeyNodesForStructEncoding<T>(
          requestedType,
          fileType,
          stripe,
          streamLabels,
          memoryPool_,
          flatMapContext_)},
      nullColumnReader_{std::make_unique<NullColumnReader>(
          stripe,
          requestedType_->type()->asMap().valueType())},
      executor_{executor},
      parallelForOnKeyNodes_{
          executor_,
          0,
          keyNodes_.size(),
          decodingParallelismFactor} {
  DWIO_ENSURE_EQ(fileType_->id(), fileType->id());
  DWIO_ENSURE(!keyNodes_.empty()); // "For struct encoding, keys to project
                                   // must be configured.";
}

template <typename T>
uint64_t FlatMapStructEncodingColumnReader<T>::skip(uint64_t numValues) {
  // skip basic rows
  numValues = ColumnReader::skip(numValues);

  // skip every single node
  for (auto& node : keyNodes_) {
    if (node) {
      node->skip(numValues);
    }
  }

  return numValues;
}

namespace {

BufferPtr getBufferForCurrentThread(
    folly::Synchronized<std::unordered_map<std::thread::id, BufferPtr>>&
        bufferMap) {
  const auto id = std::this_thread::get_id();
  {
    auto bufferMapRead = bufferMap.rlock();
    auto mergedNullsIt = bufferMapRead->find(id);
    if (mergedNullsIt != bufferMapRead->end()) {
      return mergedNullsIt->second;
    }
  }
  return bufferMap.wlock()->emplace(id, nullptr).first->second;
}

} // namespace

template <typename T>
void FlatMapStructEncodingColumnReader<T>::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  auto rowVector = detail::resetIfWrongVectorType<RowVector>(result);
  std::vector<VectorPtr> children;
  if (rowVector) {
    // Track children vectors in a local variable because readNulls may reset
    // the parent vector.
    rowVector->unsafeResize(numValues, false);
    children = rowVector->children();
    DWIO_ENSURE_EQ(children.size(), keyNodes_.size());
  }

  BufferPtr nulls = readNulls(numValues, result, incomingNulls);
  const auto* nullsPtr = nulls ? nulls->as<uint64_t>() : nullptr;
  const uint64_t nullCount =
      nullsPtr ? bits::countNulls(nullsPtr, 0, numValues) : 0;
  const auto nonNullMaps = numValues - nullCount;

  std::vector<VectorPtr>* childrenPtr = nullptr;
  if (result) {
    // Parent vector still exist, so there is no need to double reference
    // children vectors.
    childrenPtr = &rowVector->children();
    children.clear();
  } else {
    children.resize(keyNodes_.size());
    childrenPtr = &children;
  }

  if (executor_) {
    auto mergedNullsBuffers =
        folly::Synchronized<std::unordered_map<std::thread::id, BufferPtr>>();
    parallelForOnKeyNodes_.execute([numValues,
                                    nonNullMaps,
                                    nullsPtr,
                                    &mergedNullsBuffers,
                                    this,
                                    childrenPtr](size_t i) {
      auto mergedNullsBuffer = getBufferForCurrentThread(mergedNullsBuffers);
      auto& node = keyNodes_[i];
      auto& child = (*childrenPtr)[i];
      if (node) {
        node->loadAsChild(
            child, numValues, mergedNullsBuffer, nonNullMaps, nullsPtr);
      } else {
        nullColumnReader_->next(numValues, child, nullsPtr);
      }
    });
  } else {
    for (size_t i = 0; i < keyNodes_.size(); ++i) {
      auto& node = keyNodes_[i];
      auto& child = (*childrenPtr)[i];

      if (node) {
        node->loadAsChild(
            child, numValues, mergedNulls_, nonNullMaps, nullsPtr);
      } else {
        nullColumnReader_->next(numValues, child, nullsPtr);
      }
    }
  }

  if (result) {
    result->setNullCount(nullCount);
  } else {
    result = std::make_shared<RowVector>(
        &memoryPool_,
        ROW(std::vector<std::string>(keyNodes_.size()),
            std::vector<std::shared_ptr<const Type>>(
                keyNodes_.size(), requestedType_->type()->asMap().valueType())),
        nulls,
        numValues,
        std::move(children),
        nullCount);
  }
}

inline bool isRequiringStructEncoding(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const dwio::common::RowReaderOptions& rowOptions) {
  return rowOptions.getMapColumnIdAsStruct().count(requestedType->id()) > 0;
}

template <typename T>
std::unique_ptr<ColumnReader> createFlatMapColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    StripeStreams& stripe,
    const StreamLabels& streamLabels,
    folly::Executor* FOLLY_NULLABLE executor,
    size_t decodingParallelismFactor,
    FlatMapContext flatMapContext) {
  if (isRequiringStructEncoding(requestedType, stripe.getRowReaderOptions())) {
    return std::make_unique<FlatMapStructEncodingColumnReader<T>>(
        requestedType,
        fileType,
        stripe,
        streamLabels,
        executor,
        decodingParallelismFactor,
        std::move(flatMapContext));
  } else {
    return std::make_unique<FlatMapColumnReader<T>>(
        requestedType,
        fileType,
        stripe,
        streamLabels,
        executor,
        decodingParallelismFactor,
        std::move(flatMapContext));
  }
}

/* static */ std::unique_ptr<ColumnReader> FlatMapColumnReaderFactory::create(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    StripeStreams& stripe,
    const StreamLabels& streamLabels,
    folly::Executor* executor,
    size_t decodingParallelismFactor,
    FlatMapContext flatMapContext) {
  // create flat map column reader based on key type
  const auto kind = fileType->childAt(0)->type()->kind();

  switch (kind) {
    case TypeKind::TINYINT:
      return createFlatMapColumnReader<int8_t>(
          requestedType,
          fileType,
          stripe,
          streamLabels,
          executor,
          decodingParallelismFactor,
          std::move(flatMapContext));
    case TypeKind::SMALLINT:
      return createFlatMapColumnReader<int16_t>(
          requestedType,
          fileType,
          stripe,
          streamLabels,
          executor,
          decodingParallelismFactor,
          std::move(flatMapContext));
    case TypeKind::INTEGER:
      return createFlatMapColumnReader<int32_t>(
          requestedType,
          fileType,
          stripe,
          streamLabels,
          executor,
          decodingParallelismFactor,
          std::move(flatMapContext));
    case TypeKind::BIGINT:
      return createFlatMapColumnReader<int64_t>(
          requestedType,
          fileType,
          stripe,
          streamLabels,
          executor,
          decodingParallelismFactor,
          std::move(flatMapContext));
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      return createFlatMapColumnReader<StringView>(
          requestedType,
          fileType,
          stripe,
          streamLabels,
          executor,
          decodingParallelismFactor,
          std::move(flatMapContext));
    default:
      DWIO_RAISE("Not supported key type: ", kind);
  }
}

// declare all possible flat map column reader
template class FlatMapColumnReader<int8_t>;
template class FlatMapColumnReader<int16_t>;
template class FlatMapColumnReader<int32_t>;
template class FlatMapColumnReader<int64_t>;
template class FlatMapColumnReader<StringView>;

template class FlatMapStructEncodingColumnReader<int8_t>;
template class FlatMapStructEncodingColumnReader<int16_t>;
template class FlatMapStructEncodingColumnReader<int32_t>;
template class FlatMapStructEncodingColumnReader<int64_t>;
template class FlatMapStructEncodingColumnReader<StringView>;

} // namespace facebook::velox::dwrf

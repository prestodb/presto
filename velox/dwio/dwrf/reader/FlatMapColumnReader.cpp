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
#include <folly/json.h>
#include "velox/common/base/BitUtil.h"
#include "velox/dwio/dwrf/reader/FlatMapHelper.h"

namespace facebook::velox::dwrf {

using dwio::common::TypeWithId;
using memory::MemoryPool;

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
KeyValue<StringView> extractKey<StringView>(const proto::KeyInfo& info) {
  return KeyValue<StringView>(StringView(info.byteskey()));
}

template <typename T>
KeyValue<T> parseKeyValue(std::string_view str) {
  return KeyValue<T>(folly::to<T>(str));
}

template <>
KeyValue<StringView> parseKeyValue<StringView>(std::string_view str) {
  return KeyValue<StringView>(StringView(str));
}

template <typename T>
struct KeyProjection {
  KeyProjectionMode mode = KeyProjectionMode::ALLOW;
  KeyValue<T> value;
};

template <typename T>
KeyProjection<T> convertDynamic(const folly::dynamic& v) {
  constexpr char reject_prefix = '!';
  const auto str = v.asString();
  const std::string_view view(str);
  if (!view.empty() && view.front() == reject_prefix) {
    return {
        .mode = KeyProjectionMode::REJECT,
        .value = parseKeyValue<T>(view.substr(1)),
    };
  } else {
    return {
        .mode = KeyProjectionMode::ALLOW,
        .value = parseKeyValue<T>(view),
    };
  }
}

template <>
KeyProjection<StringView> convertDynamic<StringView>(const folly::dynamic& v) {
  return {
      .mode = KeyProjectionMode::ALLOW,
      .value = KeyValue<StringView>(StringView(v.asString()))};
}

template <typename T>
void forEachConfiguredKey(
    const std::function<void(KeyValue<T>&&)>& cb,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const StripeStreams& stripe) {
  // if keys filter is passed, translate them as the internal filter
  const auto& cs = stripe.getColumnSelector();
  const auto expr = cs.getNode(requestedType->id)->getNode().expression;
  if (!expr.empty()) {
    // JSON parse option?
    const auto array = folly::parseJson(expr);
    for (const auto& v : array) {
      DWIO_ENSURE(!v.isNull(), "map key filter should not be null");
      cb(convertDynamic<T>(v).value);
    }
    VLOG(1) << "[Flat-Map] key filters count: " << array.size();
  }
}

template <typename T>
std::vector<std::unique_ptr<KeyNode<T>>> getKeyNodesFiltered(
    const std::function<bool(const KeyValue<T>&)>& keyPredicate,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    memory::MemoryPool& memoryPool) {
  std::vector<std::unique_ptr<KeyNode<T>>> keyNodes;

  const auto requestedValueType = requestedType->childAt(1);
  const auto dataValueType = dataType->childAt(1);
  std::unordered_set<size_t> processed;

  // load all sub streams
  // fetch reader, in map bitmap and key object.
  auto streams = stripe.visitStreamsOfNode(
      dataValueType->id, [&](const StreamInformation& stream) {
        auto sequence = stream.getSequence();
        // No need to load shared dictionary stream here.
        if (sequence == 0) {
          return;
        }
        // if this branch (sequence) is not in the node list yet
        if (processed.count(sequence) == 0) {
          EncodingKey seqEk(dataValueType->id, sequence);
          const auto& keyInfo = stripe.getEncoding(seqEk).key();
          auto key = extractKey<T>(keyInfo);
          // check if we have key filter passed through read schema
          if (keyPredicate(key)) {
            // fetch reader, in map bitmap and key object.
            auto inMap = stripe.getStream(
                seqEk.forKind(proto::Stream_Kind_IN_MAP), true);
            DWIO_ENSURE_NOT_NULL(inMap, "In map stream is required");
            // build seekable
            auto inMapDecoder =
                createBooleanRleDecoder(std::move(inMap), seqEk);

            // std::unique_ptr<ColumnReader>
            auto valueReader = ColumnReader::build(
                requestedValueType,
                dataValueType,
                stripe,
                FlatMapContext{sequence, inMapDecoder.get()});

            keyNodes.push_back(std::make_unique<KeyNode<T>>(
                std::move(valueReader),
                std::move(inMapDecoder),
                key,
                sequence,
                memoryPool));
            processed.insert(sequence);
          }
        }
      });

  VLOG(1) << "[Flat-Map] Initialized a flat-map column reader for node "
          << dataType->id << ", keys=" << keyNodes.size()
          << ", streams=" << streams;
  return keyNodes;
}

template <typename T>
struct ParsedKeyFilter {
  KeyProjectionMode mode = KeyProjectionMode::ALLOW;
  std::vector<KeyValue<T>> keys;
};

template <typename T>
ParsedKeyFilter<T> parseKeyFilter(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const StripeStreams& stripe) {
  std::vector<KeyProjectionMode> modes;
  std::vector<KeyValue<T>> keys;

  auto& cs = stripe.getColumnSelector();
  const auto expr = cs.getNode(requestedType->id)->getNode().expression;
  if (!expr.empty()) {
    // JSON parse option?
    auto array = folly::parseJson(expr);
    for (auto v : array) {
      DWIO_ENSURE(!v.isNull(), "map key filter should not be null");
      auto converted = convertDynamic<T>(v);
      modes.push_back(converted.mode);
      keys.push_back(std::move(converted.value));
    }
    VLOG(1) << "[Flat-Map] key filters count: " << array.size();
  }

  DWIO_ENSURE_EQ(modes.size(), keys.size());
  // You cannot mix allow key and reject key.
  DWIO_ENSURE(
      modes.empty() ||
      std::all_of(modes.begin(), modes.end(), [&modes](const auto& v) {
        return v == modes.front();
      }));

  return {
      .mode = modes.empty() ? KeyProjectionMode::ALLOW : modes.front(),
      .keys = std::move(keys)};
}

template <typename T>
KeyPredicate<T> prepareKeyPredicate(
    const std::shared_ptr<const TypeWithId>& requestedType,
    StripeStreams& stripe) {
  auto parsedKeyFilter = parseKeyFilter<T>(requestedType, stripe);
  return KeyPredicate<T>(
      parsedKeyFilter.mode,
      typename KeyPredicate<T>::Lookup(
          parsedKeyFilter.keys.begin(), parsedKeyFilter.keys.end()));
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
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(dataType, stripe, std::move(flatMapContext)),
      requestedType_{requestedType},
      returnFlatVector_{stripe.getRowReaderOptions().getReturnFlatVector()} {
  DWIO_ENSURE_EQ(nodeType_->id, dataType->id);

  const auto keyPredicate = prepareKeyPredicate<T>(requestedType, stripe);

  keyNodes_ = getKeyNodesFiltered<T>(
      [&keyPredicate](const auto& keyValue) { return keyPredicate(keyValue); },
      requestedType,
      dataType,
      stripe,
      memoryPool_);

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
  flatmap_helper::initializeFlatVector<T>(vector, memoryPool_, size, false);
  vector->setSize(size);
}

template <>
void FlatMapColumnReader<StringView>::initKeysVector(
    VectorPtr& vector,
    vector_size_t size) {
  flatmap_helper::initializeFlatVector<StringView>(
      vector,
      memoryPool_,
      size,
      false,
      std::vector<BufferPtr>{stringKeyBuffer_->getBuffer()});
  vector->setSize(size);
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

  if (mapVector) {
    detail::resetIfNotWritable(result, offsets, lengths);
  }

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
  size_t totalChildren = 0;
  if (nonNullMaps > 0) {
    for (auto& node : keyNodes_) {
      // if the node has value filled into key-value batch
      // future optimization - enable batch to be sortable on row index
      // and below next can be updated to next(keys, values, numValues)
      // which writes row index into batch and offsets can be generated
      auto batch = node->load(nonNullMaps);
      if (batch) {
        nodes.emplace_back(node.get());
        node->addToBulkInMapBitIterator(bulkInMapIter);
        nodeBatches.push_back(batch);
        totalChildren += batch->size();
      }
    }
  }

  size_t startIndices[nodeBatches.size()];
  size_t nodeIndices[nodeBatches.size()];

  auto& mapValueType = requestedType_->type->asMap().valueType();
  if (totalChildren > 0) {
    size_t childOffset = 0;
    for (size_t i = 0; i < nodeBatches.size(); i++) {
      nodeIndices[i] = 0;
      startIndices[i] = childOffset;
      childOffset += nodeBatches[i]->size();
    }

    initKeysVector(keysVector, totalChildren);
    flatmap_helper::initializeVector(
        valuesVector, mapValueType, memoryPool_, nodeBatches);

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
            flatmap_helper::copyOne(
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

  // When read-string-as-row flag is on, string readers produce ROW(BIGINT,
  // BIGINT) type instead of VARCHAR or VARBINARY. In these cases,
  // requestedType_->type is not the right type of the final vector.
  auto mapType = (keysVector == nullptr || valuesVector == nullptr)
      ? requestedType_->type
      : MAP(keysVector->type(), valuesVector->type());

  // TODO Reuse
  result = std::make_shared<MapVector>(
      &memoryPool_,
      mapType,
      nulls,
      numValues,
      offsets,
      lengths,
      keysVector,
      valuesVector,
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
const uint64_t* FOLLY_NULLABLE KeyNode<T>::mergeNulls(
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
uint64_t* FOLLY_NULLABLE
KeyNode<T>::ensureNullBuffer(uint64_t numValues, BufferPtr& mergedNulls) {
  const auto numBytes = bits::nbytes(numValues);
  if (!mergedNulls || mergedNulls->capacity() < numBytes) {
    mergedNulls = AlignedBuffer::allocate<bool>(numValues, &memoryPool_);
  }
  return mergedNulls->asMutable<uint64_t>();
}

template <typename T>
const uint64_t* FOLLY_NULLABLE
KeyNode<T>::getAllNulls(uint64_t numValues, BufferPtr& mergedNulls) {
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
        StringView{data, size};
  });
}

template <typename T>
std::vector<std::unique_ptr<KeyNode<T>>> getKeyNodesForStructEncoding(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    memory::MemoryPool& memoryPool) {
  // `KeyNode` is ordered based on the projection. So if [3, 2, 1] is
  // projected, the vector of key node will be created [3, 2, 1].
  // If the key is not found in the stripe, the key node will be nullptr.

  auto parsedKeyFilter = parseKeyFilter<T>(requestedType, stripe);

  const KeyPredicate<T> keyPredicate(
      parsedKeyFilter.mode,
      typename KeyPredicate<T>::Lookup(
          parsedKeyFilter.keys.begin(), parsedKeyFilter.keys.end()));

  auto availableKeyNodes = getKeyNodesFiltered<T>(
      [&keyPredicate](const auto& keyValue) { return keyPredicate(keyValue); },
      requestedType,
      dataType,
      stripe,
      memoryPool);

  const auto& mapColumnIdAsStruct =
      stripe.getRowReaderOptions().getMapColumnIdAsStruct();
  auto it = mapColumnIdAsStruct.find(requestedType->id);
  DWIO_ENSURE(it != mapColumnIdAsStruct.end());

  return rearrangeKeyNodesAsProjectedOrder<T>(availableKeyNodes, it->second);
}

template <typename T>
FlatMapStructEncodingColumnReader<T>::FlatMapStructEncodingColumnReader(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : ColumnReader(requestedType, stripe, std::move(flatMapContext)),
      requestedType_{requestedType},
      keyNodes_{getKeyNodesForStructEncoding<T>(
          requestedType,
          dataType,
          stripe,
          memoryPool_)},
      nullColumnReader_{std::make_unique<NullColumnReader>(
          stripe,
          requestedType_->type->asMap().valueType())} {
  DWIO_ENSURE_EQ(nodeType_->id, dataType->id);
  DWIO_ENSURE(!keyNodes_.empty()); // "For struct encoding, keys to project must
                                   // be configured.";
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

template <typename T>
void FlatMapStructEncodingColumnReader<T>::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* FOLLY_NULLABLE incomingNulls) {
  auto rowVector = detail::resetIfWrongVectorType<RowVector>(result);
  std::vector<VectorPtr> children;
  if (rowVector) {
    // Track children vectors in a local variable because readNulls may reset
    // the parent vector.
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

  for (size_t i = 0; i < keyNodes_.size(); ++i) {
    auto& node = keyNodes_[i];
    auto& child = (*childrenPtr)[i];

    if (node) {
      node->loadAsChild(child, numValues, mergedNulls_, nonNullMaps, nullsPtr);
    } else {
      nullColumnReader_->next(numValues, child, nullsPtr);
    }
  }

  if (result) {
    result->setSize(numValues);
    result->setNullCount(nullCount);
  } else {
    result = std::make_shared<RowVector>(
        &memoryPool_,
        ROW(std::vector<std::string>(keyNodes_.size()),
            std::vector<std::shared_ptr<const Type>>(
                keyNodes_.size(), requestedType_->type->asMap().valueType())),
        nulls,
        numValues,
        std::move(children),
        nullCount);
  }
}

inline bool isRequiringStructEncoding(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const dwio::common::RowReaderOptions& rowOptions) {
  return rowOptions.getMapColumnIdAsStruct().count(requestedType->id) > 0;
}

template <typename T>
std::unique_ptr<ColumnReader> createFlatMapColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext) {
  if (isRequiringStructEncoding(requestedType, stripe.getRowReaderOptions())) {
    return std::make_unique<FlatMapStructEncodingColumnReader<T>>(
        requestedType, dataType, stripe, std::move(flatMapContext));
  } else {
    return std::make_unique<FlatMapColumnReader<T>>(
        requestedType, dataType, stripe, std::move(flatMapContext));
  }
}

/* static */ std::unique_ptr<ColumnReader> FlatMapColumnReaderFactory::create(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext) {
  // create flat map column reader based on key type
  const auto kind = dataType->childAt(0)->type->kind();

  switch (kind) {
    case TypeKind::TINYINT:
      return createFlatMapColumnReader<int8_t>(
          requestedType, dataType, stripe, std::move(flatMapContext));
    case TypeKind::SMALLINT:
      return createFlatMapColumnReader<int16_t>(
          requestedType, dataType, stripe, std::move(flatMapContext));
    case TypeKind::INTEGER:
      return createFlatMapColumnReader<int32_t>(
          requestedType, dataType, stripe, std::move(flatMapContext));
    case TypeKind::BIGINT:
      return createFlatMapColumnReader<int64_t>(
          requestedType, dataType, stripe, std::move(flatMapContext));
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      return createFlatMapColumnReader<StringView>(
          requestedType, dataType, stripe, std::move(flatMapContext));
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

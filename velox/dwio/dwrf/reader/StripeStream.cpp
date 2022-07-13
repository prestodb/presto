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

#include <folly/ScopeGuard.h>

#include "velox/common/base/BitSet.h"
#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/common/wrap/coded-stream-wrapper.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"

namespace facebook::velox::dwrf {

using dwio::common::LogType;
using dwio::common::TypeWithId;

namespace {

template <typename IsProjected>
void findProjectedNodes(
    BitSet& projectedNodes,
    const TypeWithId& expected,
    const TypeWithId& actual,
    IsProjected isProjected) {
  // we don't need to perform schema compatibility check since reader should
  // have already done that before reaching here.
  // if a leaf node is projected, all the intermediate node from root to the
  // node should also be projected. So we can return as soon as seeing node that
  // is not projected
  if (!isProjected(expected.id)) {
    return;
  }
  projectedNodes.insert(actual.id);
  switch (actual.type->kind()) {
    case TypeKind::ROW: {
      uint64_t childCount = std::min(expected.size(), actual.size());
      for (uint64_t i = 0; i < childCount; ++i) {
        findProjectedNodes(
            projectedNodes,
            *expected.childAt(i),
            *actual.childAt(i),
            isProjected);
      }
      break;
    }
    case TypeKind::ARRAY:
      findProjectedNodes(
          projectedNodes,
          *expected.childAt(0),
          *actual.childAt(0),
          isProjected);
      break;
    case TypeKind::MAP: {
      findProjectedNodes(
          projectedNodes,
          *expected.childAt(0),
          *actual.childAt(0),
          isProjected);
      findProjectedNodes(
          projectedNodes,
          *expected.childAt(1),
          *actual.childAt(1),
          isProjected);
      break;
    }
    default:
      break;
  }
}

template <typename T>
static inline void ensureCapacity(
    BufferPtr& data,
    size_t capacity,
    velox::memory::MemoryPool* pool) {
  if (!data || data->capacity() < BaseVector::byteSize<T>(capacity)) {
    data = AlignedBuffer::allocate<T>(capacity, pool);
  }
}

template <typename T>
BufferPtr readDict(
    dwio::common::IntDecoder<true>* dictReader,
    int64_t dictionarySize,
    velox::memory::MemoryPool* pool) {
  BufferPtr dictionaryBuffer = AlignedBuffer::allocate<T>(dictionarySize, pool);
  dictReader->bulkRead(dictionarySize, dictionaryBuffer->asMutable<T>());
  return dictionaryBuffer;
}
} // namespace

std::function<BufferPtr()>
StripeStreamsBase::getIntDictionaryInitializerForNode(
    const EncodingKey& ek,
    uint64_t elementWidth,
    uint64_t dictionaryWidth) {
  // Create local copy for manipulation
  EncodingKey localEk{ek};
  auto dictData = localEk.forKind(proto::Stream_Kind_DICTIONARY_DATA);
  auto dataStream = getStream(dictData, false);
  auto dictionarySize = getEncoding(localEk).dictionarysize();
  // Try fetching shared dictionary streams instead.
  if (!dataStream) {
    localEk = EncodingKey(ek.node, 0);
    dictData = localEk.forKind(proto::Stream_Kind_DICTIONARY_DATA);
    dataStream = getStream(dictData, false);
  }
  bool dictVInts = getUseVInts(dictData);
  DWIO_ENSURE(dataStream.get());
  stripeDictionaryCache_->registerIntDictionary(
      localEk,
      [dictReader = createDirectDecoder</* isSigned = */ true>(
           std::move(dataStream), dictVInts, elementWidth),
       dictionaryWidth,
       dictionarySize](velox::memory::MemoryPool* pool) mutable {
        return VELOX_WIDTH_DISPATCH(
            dictionaryWidth, readDict, dictReader.get(), dictionarySize, pool);
      });
  return [&dictCache = *stripeDictionaryCache_, localEk]() {
    // If this is not flat map or if dictionary is not shared, return as is
    return dictCache.getIntDictionary(localEk);
  };
}

void StripeStreamsImpl::loadStreams() {
  auto& footer = reader_.getStripeFooter();

  // HACK!!!
  // Column selector filters based on requested schema (ie, table schema), while
  // we need filter based on file schema. As a result we cannot call
  // shouldReadNode directly. Instead, build projected nodes set based on node
  // id from file schema. Column selector should really be fixed to handle file
  // schema properly
  BitSet projectedNodes(0);
  auto expected = selector_.getSchemaWithId();
  auto actual = reader_.getReader().getSchemaWithId();
  findProjectedNodes(projectedNodes, *expected, *actual, [&](uint32_t node) {
    return selector_.shouldReadNode(node);
  });

  auto addStream = [&](auto& stream, auto& offset) {
    if (stream.has_offset()) {
      offset = stream.offset();
    }
    if (projectedNodes.contains(stream.node())) {
      streams_[stream] = {offset, stream};
    }
    offset += stream.length();
  };

  uint64_t streamOffset = 0;
  for (auto& stream : footer.streams()) {
    addStream(stream, streamOffset);
  }

  // update column encoding for each stream
  for (uint32_t i = 0; i < footer.encoding_size(); ++i) {
    auto& e = footer.encoding(i);
    auto node = e.has_node() ? e.node() : i;
    if (projectedNodes.contains(node)) {
      encodings_[{node, e.has_sequence() ? e.sequence() : 0}] = i;
    }
  }

  // handle encrypted columns
  auto& handler = reader_.getDecryptionHandler();
  if (handler.isEncrypted()) {
    DWIO_ENSURE_EQ(
        handler.getEncryptionGroupCount(), footer.encryptiongroups_size());
    std::unordered_set<uint32_t> groupIndices;
    bits::forEachSetBit(
        projectedNodes.bits(), 0, projectedNodes.max() + 1, [&](uint32_t node) {
          if (handler.isEncrypted(node)) {
            groupIndices.insert(handler.getEncryptionGroupIndex(node));
          }
        });

    // decrypt encryption groups
    for (auto index : groupIndices) {
      auto& group = footer.encryptiongroups(index);
      auto groupProto =
          reader_.getReader().readProtoFromString<proto::StripeEncryptionGroup>(
              group,
              std::addressof(handler.getEncryptionProviderByIndex(index)));
      streamOffset = 0;
      for (auto& stream : groupProto->streams()) {
        addStream(stream, streamOffset);
      }
      for (auto& encoding : groupProto->encoding()) {
        DWIO_ENSURE(encoding.has_node(), "node is required");
        auto node = encoding.node();
        if (projectedNodes.contains(node)) {
          decryptedEncodings_[{
              node, encoding.has_sequence() ? encoding.sequence() : 0}] =
              encoding;
        }
      }
    }
  }
}

std::unique_ptr<dwio::common::SeekableInputStream>
StripeStreamsImpl::getCompressedStream(const DwrfStreamIdentifier& si) const {
  const auto& info = getStreamInfo(si);

  std::unique_ptr<dwio::common::SeekableInputStream> streamRead;
  if (si.kind() == StreamKind::StreamKind_ROW_INDEX) {
    streamRead = getIndexStreamFromCache(info);
  }

  if (!streamRead) {
    streamRead = reader_.getStripeInput().enqueue(
        {info.getOffset() + stripeStart_, info.getLength()}, &si);
  }

  DWIO_ENSURE(streamRead != nullptr, " Stream can't be read", si.toString());
  return streamRead;
}

std::unordered_map<uint32_t, std::vector<uint32_t>>
StripeStreamsImpl::getEncodingKeys() const {
  DWIO_ENSURE_EQ(
      decryptedEncodings_.size(),
      0,
      "Not supported for reader with encryption");

  std::unordered_map<uint32_t, std::vector<uint32_t>> encodingKeys;
  for (const auto& kv : encodings_) {
    const auto ek = kv.first;
    encodingKeys[ek.node].push_back(ek.sequence);
  }

  return encodingKeys;
}

std::unordered_map<uint32_t, std::vector<DwrfStreamIdentifier>>
StripeStreamsImpl::getStreamIdentifiers() const {
  std::unordered_map<uint32_t, std::vector<DwrfStreamIdentifier>>
      nodeToStreamIdMap;

  for (const auto& kv : streams_) {
    nodeToStreamIdMap[kv.first.encodingKey().node].push_back(kv.first);
  }

  return nodeToStreamIdMap;
}

std::unique_ptr<dwio::common::SeekableInputStream> StripeStreamsImpl::getStream(
    const DwrfStreamIdentifier& si,
    bool /* throwIfNotFound*/) const {
  // if not found, return an empty {}
  const auto& info = getStreamInfo(si, false /* throwIfNotFound */);
  if (!info.valid()) { // Stream not found.
    return {};
  }

  std::unique_ptr<dwio::common::SeekableInputStream> streamRead;
  if (si.kind() == StreamKind::StreamKind_ROW_INDEX) {
    streamRead = getIndexStreamFromCache(info);
  }

  if (!streamRead) {
    streamRead = reader_.getStripeInput().enqueue(
        {info.getOffset() + stripeStart_, info.getLength()}, &si);
  }

  if (!streamRead) {
    return streamRead;
  }

  auto streamDebugInfo =
      fmt::format("Stripe {} Stream {}", stripeIndex_, si.toString());
  return reader_.getReader().createDecompressedStream(
      std::move(streamRead),
      streamDebugInfo,
      getDecrypter(si.encodingKey().node));
}

uint32_t StripeStreamsImpl::visitStreamsOfNode(
    uint32_t node,
    std::function<void(const StreamInformation&)> visitor) const {
  uint32_t count = 0;
  for (auto& item : streams_) {
    if (item.first.encodingKey().node == node) {
      visitor(item.second);
      ++count;
    }
  }

  return count;
}

bool StripeStreamsImpl::getUseVInts(const DwrfStreamIdentifier& si) const {
  const auto& info = getStreamInfo(si, false);
  if (!info.valid()) {
    return true;
  }

  return info.getUseVInts();
}

std::unique_ptr<dwio::common::SeekableInputStream>
StripeStreamsImpl::getIndexStreamFromCache(
    const StreamInformation& info) const {
  std::unique_ptr<dwio::common::SeekableInputStream> indexStream;
  auto& reader = reader_.getReader();
  auto& metadataCache = reader.getMetadataCache();
  if (metadataCache) {
    auto indexBase = metadataCache->get(StripeCacheMode::INDEX, stripeIndex_);
    if (indexBase) {
      auto offset = info.getOffset();
      auto length = info.getLength();

      const void* start;
      int32_t ignored;
      DWIO_ENSURE(indexBase->Next(&start, &ignored), "failed to read index");
      indexStream = std::make_unique<dwio::common::SeekableArrayInputStream>(
          static_cast<const char*>(start) + offset, length);
    }
  }
  return indexStream;
}

void StripeStreamsImpl::loadReadPlan() {
  DWIO_ENSURE_EQ(readPlanLoaded_, false, "only load read plan once!");
  SCOPE_EXIT {
    readPlanLoaded_ = true;
  };

  auto& input = reader_.getStripeInput();
  input.load(LogType::STREAM_BUNDLE);
}

} // namespace facebook::velox::dwrf

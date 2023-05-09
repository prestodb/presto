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

#include "velox/dwio/dwrf/writer/LayoutPlanner.h"

namespace facebook::velox::dwrf {

StreamList getStreamList(WriterContext& context) {
  StreamList streams;
  streams.reserve(context.getStreamCount());
  context.iterateUnSuppressedStreams([&](auto& pair) {
    streams.push_back(std::make_pair(
        std::addressof(pair.first), std::addressof(pair.second)));
  });
  return streams;
}

EncodingIter::EncodingIter(
    const proto::StripeFooter& footer,
    const std::vector<proto::StripeEncryptionGroup>& encryptionGroups,
    int32_t encryptionGroupIndex,
    google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
        current,
    google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
        currentEnd)
    : footer_{footer},
      encryptionGroups_{encryptionGroups},
      encryptionGroupIndex_{encryptionGroupIndex},
      current_{std::move(current)},
      currentEnd_{std::move(currentEnd)} {
  // Move to the end or a valid position.
  if (current_ == currentEnd_) {
    next();
  }
}

/* static */ EncodingIter EncodingIter::begin(
    const proto::StripeFooter& footer,
    const std::vector<proto::StripeEncryptionGroup>& encryptionGroups) {
  return EncodingIter{
      footer,
      encryptionGroups,
      -1,
      footer.encoding().begin(),
      footer.encoding().end()};
}

/* static */ EncodingIter EncodingIter::end(
    const proto::StripeFooter& footer,
    const std::vector<proto::StripeEncryptionGroup>& encryptionGroups) {
  if (encryptionGroups.empty()) {
    const auto& footerEncodings = footer.encoding();
    return EncodingIter{
        footer,
        encryptionGroups,
        -1,
        footerEncodings.end(),
        footerEncodings.end()};
  }
  const auto lastEncryptionGroupIndex =
      folly::to<int32_t>(encryptionGroups.size()) - 1;
  const auto& lastEncryptionGroupEncodings = encryptionGroups.back().encoding();
  return EncodingIter{
      footer,
      encryptionGroups,
      lastEncryptionGroupIndex,
      lastEncryptionGroupEncodings.end(),
      lastEncryptionGroupEncodings.end()};
}

void EncodingIter::next() {
  // The check is needed for the initial position
  // if footer is empty but encryption groups are not.
  if (current_ != currentEnd_) {
    ++current_;
  }
  // Get to the absolute end or a valid position.
  while (current_ == currentEnd_) {
    if (encryptionGroupIndex_ == encryptionGroups_.size() - 1) {
      return;
    }
    const auto& encryptionGroup = encryptionGroups_.at(++encryptionGroupIndex_);
    current_ = encryptionGroup.encoding().begin();
    currentEnd_ = encryptionGroup.encoding().end();
  }
  return;
}

EncodingIter& EncodingIter::operator++() {
  next();
  return *this;
}

EncodingIter EncodingIter::operator++(int) {
  auto current = *this;
  next();
  return current;
}

bool EncodingIter::operator==(const EncodingIter& other) const {
  return current_ == other.current_;
}

bool EncodingIter::operator!=(const EncodingIter& other) const {
  return current_ != other.current_;
}

EncodingIter::reference EncodingIter::operator*() const {
  return *current_;
}

EncodingIter::pointer EncodingIter::operator->() const {
  return &(*current_);
}

EncodingManager::EncodingManager(
    const encryption::EncryptionHandler& encryptionHandler)
    : encryptionHandler_{encryptionHandler} {
  initEncryptionGroups();
}

proto::ColumnEncoding& EncodingManager::addEncodingToFooter(uint32_t nodeId) {
  if (encryptionHandler_.isEncrypted(nodeId)) {
    auto index = encryptionHandler_.getEncryptionGroupIndex(nodeId);
    return *encryptionGroups_.at(index).add_encoding();
  } else {
    return *footer_.add_encoding();
  }
}

proto::Stream* EncodingManager::addStreamToFooter(
    uint32_t nodeId,
    uint32_t& currentIndex) {
  if (encryptionHandler_.isEncrypted(nodeId)) {
    currentIndex = encryptionHandler_.getEncryptionGroupIndex(nodeId);
    return encryptionGroups_.at(currentIndex).add_streams();
  } else {
    currentIndex = std::numeric_limits<uint32_t>::max();
    return footer_.add_streams();
  }
}

std::string* EncodingManager::addEncryptionGroupToFooter() {
  return footer_.add_encryptiongroups();
}

proto::StripeEncryptionGroup EncodingManager::getEncryptionGroup(uint32_t i) {
  return encryptionGroups_.at(i);
}

const proto::StripeFooter& EncodingManager::getFooter() const {
  return footer_;
}

EncodingIter EncodingManager::begin() const {
  return EncodingIter::begin(footer_, encryptionGroups_);
}

EncodingIter EncodingManager::end() const {
  return EncodingIter::end(footer_, encryptionGroups_);
}

void EncodingManager::initEncryptionGroups() {
  // initialize encryption groups
  if (encryptionHandler_.isEncrypted()) {
    auto count = encryptionHandler_.getEncryptionGroupCount();
    // We use uint32_t::max to represent non-encrypted when adding streams, so
    // make sure number of encryption groups is smaller than that
    DWIO_ENSURE_LT(count, std::numeric_limits<uint32_t>::max());
    encryptionGroups_.resize(count);
  }
}

void LayoutResult::iterateIndexStreams(
    const std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>&
        consumer) const {
  for (auto iter = streams_.begin(), end = iter + indexCount_; iter != end;
       ++iter) {
    consumer(*iter->first, *iter->second);
  }
}

void LayoutResult::iterateDataStreams(
    const std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>&
        consumer) const {
  for (auto iter = streams_.begin() + indexCount_; iter != streams_.end();
       ++iter) {
    consumer(*iter->first, *iter->second);
  }
}

namespace {

// Helper method to walk the schema tree and grow the node to column id mapping.
void fillNodeToColumnMap(
    const dwio::common::TypeWithId& schema,
    folly::F14FastMap<uint32_t, uint32_t>& nodeToColumnMap) {
  nodeToColumnMap.emplace(schema.id, schema.column);
  for (size_t i = 0; i < schema.size(); ++i) {
    fillNodeToColumnMap(*schema.childAt(i), nodeToColumnMap);
  }
}

struct MapKey {
  uint32_t column;
  uint32_t sequence;
};

struct MapKeyHash {
  size_t operator()(MapKey key) const noexcept {
    return static_cast<uint64_t>(key.column) << 32 | key.sequence;
  }
};

struct MapKeyEqual {
  bool operator()(MapKey lhs, MapKey rhs) const noexcept {
    return lhs.column == rhs.column && lhs.sequence == rhs.sequence;
  }
};

} // namespace

LayoutPlanner::LayoutPlanner(const dwio::common::TypeWithId& schema) {
  fillNodeToColumnMap(schema, nodeToColumnMap_);
}

LayoutResult LayoutPlanner::plan(
    const EncodingContainer& encoding,
    StreamList streams) const {
  // place index before data
  auto iter = std::partition(streams.begin(), streams.end(), [](auto& stream) {
    return isIndexStream(stream.first->kind());
  });
  size_t indexCount = iter - streams.begin();
  auto flatMapCols = getFlatMapColumns(encoding, nodeToColumnMap_);

  // sort streams
  sortBySize(streams.begin(), iter, flatMapCols);
  sortBySize(iter, streams.end(), flatMapCols);

  return LayoutResult{std::move(streams), indexCount};
}

void LayoutPlanner::sortBySize(
    StreamList::iterator begin,
    StreamList::iterator end,
    const folly::F14FastSet<uint32_t>& flatMapCols) {
  // calculate node size
  folly::F14FastMap<uint32_t, uint64_t> nodeSize;
  folly::F14FastMap<MapKey, uint64_t, MapKeyHash, MapKeyEqual> flatMapNodeSize;

  for (auto iter = begin; iter != end; ++iter) {
    auto& stream = *iter->first;
    auto size = iter->second->size();
    // Flatmap aggregate size at column/sequence level. Non-flatmap does that at
    // node level.
    if (flatMapCols.count(stream.column()) > 0) {
      flatMapNodeSize[{
          .column = stream.column(),
          .sequence = stream.encodingKey().sequence,
      }] += size;
    } else {
      nodeSize[stream.encodingKey().node] += size;
    }
  }

  std::sort(begin, end, [&](auto& a, auto& b) {
    // 1. Sort based on flatmap or not. Place streams of non-flatmaps before
    // those of flatmaps. Within flatmap, sort by column and sequence. Lowest
    // first.
    auto& streamA = *a.first;
    auto& streamB = *b.first;
    auto nodeA = streamA.encodingKey().node;
    auto nodeB = streamB.encodingKey().node;
    auto isFlatMapA = flatMapCols.count(streamA.column()) > 0;
    auto isFlatMapB = flatMapCols.count(streamB.column()) > 0;

    // 1. Sort based on flatmap or not. Place streams of non-flatmaps before
    // those of flatmaps.
    if (isFlatMapA != isFlatMapB) {
      return !isFlatMapA;
    }

    // 2. For flatmaps, sort based on column id, smaller column id first. Then
    // sequence 0 (ie. streams shared by all sequences) always before others.
    uint64_t sizeA = 0;
    uint64_t sizeB = 0;
    if (isFlatMapA) {
      auto colA = streamA.column();
      auto colB = streamB.column();
      // Smaller column id first
      if (colA != colB) {
        return colA < colB;
      }

      auto seqA = streamA.encodingKey().sequence;
      auto seqB = streamB.encodingKey().sequence;
      // Sequence 0 always before others
      if (seqA != seqB) {
        if (seqA == 0) {
          return true;
        }
        if (seqB == 0) {
          return false;
        }
      }

      sizeA = flatMapNodeSize[{
          .column = colA,
          .sequence = seqA,
      }];
      sizeB = flatMapNodeSize[{
          .column = colB,
          .sequence = seqB,
      }];
    } else {
      sizeA = nodeSize[nodeA];
      sizeB = nodeSize[nodeB];
    }

    // 3. Sort based on size. Streams with smaller total size will
    // be in front of those with larger total node size.
    if (sizeA != sizeB) {
      return sizeA < sizeB;
    }

    // 4. Flatmap, when sequences have the same size, small sequence goes first.
    if (isFlatMapA) {
      auto seqA = streamA.encodingKey().sequence;
      auto seqB = streamB.encodingKey().sequence;
      if (seqA != seqB) {
        return seqA < seqB;
      }
    }

    // 5. Sort based on node. Small node first
    if (nodeA != nodeB) {
      return nodeA < nodeB;
    }

    // 6. Sort based on kind. Smaller kind first
    return streamA.kind() < streamB.kind();
  });
}

folly::F14FastSet<uint32_t> LayoutPlanner::getFlatMapColumns(
    const EncodingContainer& encoding,
    const folly::F14FastMap<uint32_t, uint32_t>& nodeToColumnMap) {
  folly::F14FastSet<uint32_t> cols;
  for (const auto& enc : encoding) {
    if (enc.kind() == proto::ColumnEncoding::MAP_FLAT) {
      cols.insert(nodeToColumnMap.at(enc.node()));
    }
  }
  return cols;
}

} // namespace facebook::velox::dwrf

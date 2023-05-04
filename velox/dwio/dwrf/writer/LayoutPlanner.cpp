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

  // sort streams
  NodeSizeSorter::sort(streams.begin(), iter);
  NodeSizeSorter::sort(iter, streams.end());

  return LayoutResult{std::move(streams), indexCount};
}

void LayoutPlanner::NodeSizeSorter::sort(
    StreamList::iterator begin,
    StreamList::iterator end) {
  // rules:
  // 1. streams of node with smaller total size will be in front of those with
  // larger total node size
  // 2. if total node size is the same, small node id first
  // 3. for same node, small sequence id first
  // 4. for same sequence, small kind first

  // calculate node size
  auto getNodeKey = [](const DwrfStreamIdentifier& stream) {
    return static_cast<uint64_t>(stream.encodingKey().node) << 32 |
        stream.encodingKey().sequence;
  };

  std::unordered_map<uint64_t, uint64_t> nodeSize;
  for (auto iter = begin; iter != end; ++iter) {
    auto node = getNodeKey(*iter->first);
    auto size = iter->second->size();
    auto existing = nodeSize.find(node);
    if (existing == nodeSize.end()) {
      nodeSize[node] = size;
    } else {
      existing->second += size;
    }
  }

  // sort based on node size
  std::sort(begin, end, [&](auto& a, auto& b) {
    auto sizeA = nodeSize[getNodeKey(*a.first)];
    auto sizeB = nodeSize[getNodeKey(*b.first)];
    if (sizeA != sizeB) {
      return sizeA < sizeB;
    }

    // if size is the same, sort by node and sequence
    auto nodeA = a.first->encodingKey().node;
    auto nodeB = b.first->encodingKey().node;
    if (nodeA != nodeB) {
      return nodeA < nodeB;
    }

    auto seqA = a.first->encodingKey().sequence;
    auto seqB = b.first->encodingKey().sequence;
    if (seqA != seqB) {
      return seqA < seqB;
    }

    return a.first->kind() < b.first->kind();
  });
}

} // namespace facebook::velox::dwrf

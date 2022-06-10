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

LayoutPlanner::LayoutPlanner(StreamList streams)
    : streams_{std::move(streams)} {}

void LayoutPlanner::iterateIndexStreams(
    std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>
        consumer) {
  for (auto iter = streams_.begin(), end = iter + indexCount_; iter != end;
       ++iter) {
    consumer(*iter->first, *iter->second);
  }
}

void LayoutPlanner::iterateDataStreams(
    std::function<void(const DwrfStreamIdentifier&, DataBufferHolder&)>
        consumer) {
  for (auto iter = streams_.begin() + indexCount_; iter != streams_.end();
       ++iter) {
    consumer(*iter->first, *iter->second);
  }
}

void LayoutPlanner::plan() {
  // place index before data
  auto iter =
      std::partition(streams_.begin(), streams_.end(), [](auto& stream) {
        return stream.first->kind() == StreamKind::StreamKind_ROW_INDEX;
      });
  indexCount_ = iter - streams_.begin();

  // sort streams
  NodeSizeSorter::sort(streams_.begin(), iter);
  NodeSizeSorter::sort(iter, streams_.end());
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

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

#include <gtest/gtest.h>
#include "velox/dwio/dwrf/writer/LayoutPlanner.h"

namespace facebook::velox::dwrf {

TEST(LayoutPlannerTests, Basic) {
  auto config = std::make_shared<Config>();
  config->set(
      Config::COMPRESSION, dwio::common::CompressionKind::CompressionKind_NONE);
  WriterContext context{
      config, facebook::velox::memory::getDefaultScopedMemoryPool()};
  // fake streams
  std::vector<DwrfStreamIdentifier> streams;
  streams.reserve(10);
  std::array<char, 256> data;
  std::memset(data.data(), 'a', data.size());
  auto addStream =
      [&](uint32_t node, uint32_t seq, StreamKind kind, uint32_t size) {
        auto streamId = DwrfStreamIdentifier{node, seq, 0, kind};
        streams.push_back(streamId);
        AppendOnlyBufferedStream out{context.newStream(streamId)};
        out.write(data.data(), size);
        out.flush();
      };

  addStream(1, 2, StreamKind::StreamKind_PRESENT, 15); // 0
  addStream(1, 1, StreamKind::StreamKind_DATA, 10); // 1
  addStream(1, 1, StreamKind::StreamKind_PRESENT, 12); // 2
  addStream(3, 0, StreamKind::StreamKind_DATA, 1); // 3
  addStream(3, 0, StreamKind::StreamKind_PRESENT, 2); // 4
  addStream(3, 0, StreamKind::StreamKind_DICTIONARY_DATA, 3); // 5
  addStream(1, 0, StreamKind::StreamKind_ROW_INDEX, 200); // 6
  addStream(2, 0, StreamKind::StreamKind_ROW_INDEX, 100); // 7
  addStream(2, 0, StreamKind::StreamKind_DATA, 6); // 8

  // we expect index streams to be sorted by ascending size (7, 6)
  LayoutPlanner planner{getStreamList(context)};
  planner.plan();
  std::vector<size_t> indices{7, 6};
  size_t pos = 0;
  planner.iterateIndexStreams([&](auto& stream, auto& /* ignored */) {
    ASSERT_LT(pos, indices.size());
    ASSERT_EQ(stream, streams.at(indices[pos++]));
  });
  ASSERT_EQ(pos, indices.size());

  // we expected data streams to be ordered based on these rules:
  // 1. Streams of node with smaller total size will be in front of those with
  // larger total node size
  // 2. if total node size is the same, small node id first
  // 3. for same node, small sequence id first
  // 4. for same sequence, small kind first
  std::vector<size_t> dataStreams{8, 4, 3, 5, 0, 2, 1};
  pos = 0;
  planner.iterateDataStreams([&](auto& stream, auto& /* ignored */) {
    ASSERT_LT(pos, dataStreams.size());
    ASSERT_EQ(stream, streams.at(dataStreams[pos++]));
  });
  ASSERT_EQ(pos, dataStreams.size());
}
} // namespace facebook::velox::dwrf

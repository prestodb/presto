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
namespace {
class TestLayoutPlanner : public LayoutPlanner {
 public:
  using LayoutPlanner::LayoutPlanner;

  const folly::F14FastMap<uint32_t, uint32_t>& getNodeToColumnMap() const {
    return nodeToColumnMap_;
  }
};

void testCreateNodeToColumnIdMapping(
    const std::shared_ptr<const Type>& type,
    const folly::F14FastMap<uint32_t, uint32_t>& expected) {
  auto typeWithId = dwio::common::TypeWithId::create(type);
  EXPECT_EQ(expected, TestLayoutPlanner{*typeWithId}.getNodeToColumnMap());
}
} // namespace

TEST(LayoutPlannerTests, CreateNodeToColumnIdMapping) {
  testCreateNodeToColumnIdMapping(ROW({BOOLEAN()}), {{0, 0}, {1, 0}});
  testCreateNodeToColumnIdMapping(
      ROW(
          {BOOLEAN(),
           TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           VARBINARY()}),
      {{0, 0},
       {1, 0},
       {2, 1},
       {3, 2},
       {4, 3},
       {5, 4},
       {6, 5},
       {7, 6},
       {8, 7},
       {9, 8}});

  testCreateNodeToColumnIdMapping(
      ROW(
          {TIMESTAMP(),
           ARRAY(REAL()),
           MAP(INTEGER(), DOUBLE()),
           MAP(BIGINT(), DOUBLE()),
           MAP(BIGINT(), MAP(VARCHAR(), INTEGER())),
           ROW({REAL(), DOUBLE()})}),
      {{0, 0},
       {1, 0},
       {2, 1},
       {3, 1},
       {4, 2},
       {5, 2},
       {6, 2},
       {7, 3},
       {8, 3},
       {9, 3},
       {10, 4},
       {11, 4},
       {12, 4},
       {13, 4},
       {14, 4},
       {15, 5},
       {16, 5},
       {17, 5}});
}

TEST(LayoutPlannerTests, Basic) {
  auto config = std::make_shared<Config>();
  config->set(
      Config::COMPRESSION, dwio::common::CompressionKind::CompressionKind_NONE);
  WriterContext context{
      config,
      facebook::velox::memory::defaultMemoryManager().addRootPool(
          "LayoutPlannerTests")};
  // fake streams
  std::vector<DwrfStreamIdentifier> streams;
  streams.reserve(10);
  std::array<char, 256> data;
  std::memset(data.data(), 'a', data.size());
  auto addStream = [&](uint32_t node,
                       uint32_t seq,
                       uint32_t col,
                       StreamKind kind,
                       uint32_t size) {
    auto streamId = DwrfStreamIdentifier{node, seq, col, kind};
    streams.push_back(streamId);
    AppendOnlyBufferedStream out{context.newStream(streamId)};
    out.write(data.data(), size);
    out.flush();
  };

  auto encryptionHandler =
      std::make_unique<velox::dwrf::encryption::EncryptionHandler>();
  EncodingManager encodingManager{*encryptionHandler};
  auto addEncoding = [&](uint32_t node,
                         uint32_t seq,
                         proto::ColumnEncoding_Kind kind,
                         std::optional<int64_t> key = std::nullopt) {
    auto& encoding = encodingManager.addEncodingToFooter(node);
    encoding.set_node(node);
    encoding.set_sequence(seq);
    encoding.set_kind(kind);
    if (key.has_value()) {
      encoding.mutable_key()->set_intkey(*key);
    }
  };

  addStream(5, 2, 2, StreamKind::StreamKind_PRESENT, 15); // 0
  addStream(5, 1, 2, StreamKind::StreamKind_DATA, 10); // 1
  addStream(5, 1, 2, StreamKind::StreamKind_PRESENT, 12); // 2
  addStream(2, 0, 1, StreamKind::StreamKind_DATA, 1); // 3
  addStream(2, 0, 1, StreamKind::StreamKind_PRESENT, 2); // 4
  addStream(2, 0, 1, StreamKind::StreamKind_DICTIONARY_DATA, 3); // 5
  addStream(1, 0, 0, StreamKind::StreamKind_ROW_INDEX, 200); // 6
  addStream(2, 0, 1, StreamKind::StreamKind_ROW_INDEX, 100); // 7
  addStream(1, 0, 0, StreamKind::StreamKind_DATA, 6); // 8
  addStream(5, 0, 2, StreamKind::StreamKind_DICTIONARY_DATA, 15); // 9
  addStream(6, 1, 2, StreamKind::StreamKind_PRESENT, 200); // 10
  addStream(9, 1, 3, StreamKind::StreamKind_PRESENT, 100); // 11
  addStream(3, 0, 2, StreamKind::StreamKind_ROW_INDEX, 50); // 12

  addEncoding(1, 0, proto::ColumnEncoding::DIRECT);
  addEncoding(2, 0, proto::ColumnEncoding::DIRECT);
  addEncoding(3, 0, proto::ColumnEncoding::MAP_FLAT);
  addEncoding(5, 0, proto::ColumnEncoding::DICTIONARY);
  addEncoding(5, 1, proto::ColumnEncoding::DIRECT, 1);
  addEncoding(5, 2, proto::ColumnEncoding::DIRECT, 2);
  addEncoding(6, 1, proto::ColumnEncoding::DIRECT, 1);
  addEncoding(7, 0, proto::ColumnEncoding::MAP_FLAT);
  addEncoding(9, 1, proto::ColumnEncoding::DIRECT, 3);

  auto type = ROW({
      INTEGER(),
      INTEGER(),
      MAP(INTEGER(), ARRAY(INTEGER())),
      MAP(INTEGER(), INTEGER()),
  });

  // we expect index streams to be sorted by ascending size (7, 6)
  auto typeWithId = dwio::common::TypeWithId::create(type);
  LayoutPlanner planner{*typeWithId};
  auto result = planner.plan(encodingManager, getStreamList(context));
  std::vector<size_t> indices{7, 6, 12};
  size_t pos = 0;
  result.iterateIndexStreams([&](auto& stream, auto& /* ignored */) {
    ASSERT_LT(pos, indices.size());
    ASSERT_EQ(stream, streams.at(indices[pos++]));
  });
  ASSERT_EQ(pos, indices.size());

  std::vector<size_t> dataStreams{8, 4, 3, 5, 9, 0, 2, 1, 10, 11};
  pos = 0;
  result.iterateDataStreams([&](auto& stream, auto& /* ignored */) {
    ASSERT_LT(pos, dataStreams.size());
    ASSERT_EQ(stream, streams.at(dataStreams[pos++]));
  });
  ASSERT_EQ(pos, dataStreams.size());
}
} // namespace facebook::velox::dwrf

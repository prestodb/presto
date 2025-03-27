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

#include "velox/dwio/dwrf/common/Common.h"

using namespace ::testing;

namespace facebook::velox::dwrf {

class CommonTest : public ::testing::Test {
 protected:
  proto::Stream createDwrfStream(
      proto::Stream::Kind kind,
      uint32_t node,
      uint32_t sequence,
      uint32_t column) {
    proto::Stream stream;
    stream.set_kind(kind);
    stream.set_node(node);
    stream.set_sequence(sequence);
    stream.set_column(column);
    return stream;
  }

  proto::orc::Stream createOrcStream(
      proto::orc::Stream::Kind kind,
      uint32_t column) {
    proto::orc::Stream stream;
    stream.set_kind(kind);
    stream.set_column(column);
    return stream;
  }
};

TEST_F(
    CommonTest,
    DwrfStreamIdentifierGetFormat_WithStreamTypes_GetCorrectFormat) {
  EXPECT_EQ(
      DwrfStreamIdentifier(createDwrfStream({}, {}, {}, {})).format(),
      DwrfFormat::kDwrf);

  EXPECT_EQ(
      DwrfStreamIdentifier(createOrcStream({}, {})).format(), DwrfFormat::kOrc);
}

TEST_F(CommonTest, DwrfStreamIdentifier_WithDwrfStream_GetCorrectInfo) {
  auto streamId = DwrfStreamIdentifier(
      createDwrfStream(proto::Stream::Kind::Stream_Kind_DATA, 1, 2, 3));

  EXPECT_EQ(streamId.format(), DwrfFormat::kDwrf);
  EXPECT_EQ(streamId.kind(), StreamKind::StreamKind_DATA);
  EXPECT_EQ(streamId.encodingKey().node(), 1);
  EXPECT_EQ(streamId.encodingKey().sequence(), 2);
  EXPECT_EQ(streamId.column(), 3);
}

TEST_F(CommonTest, DwrfStreamIdentifier_WithOrcStream_GetCorrectInfo) {
  auto streamId = DwrfStreamIdentifier(
      createOrcStream(proto::orc::Stream::Kind::Stream_Kind_DATA, 1));

  // ORC doesn't use node directly, column is used as node
  EXPECT_EQ(streamId.format(), DwrfFormat::kOrc);
  EXPECT_EQ(streamId.kind(), StreamKind::StreamKindOrc_DATA);
  EXPECT_EQ(streamId.encodingKey().node(), 1);
  EXPECT_EQ(streamId.encodingKey().sequence(), 0);
  EXPECT_EQ(streamId.column(), dwio::common::MAX_UINT32);
}

TEST_F(
    CommonTest,
    DwrfStreamIdentifierGetKind_WithAllStreamKinds_GetCorrectConversion) {
  // DWRF
  for (auto [dwrfStreamKind, veloxStreamKind] :
       std::vector<std::tuple<proto::Stream_Kind, StreamKind>>{
           // clang-format off
           {proto::Stream_Kind_PRESENT, StreamKind::StreamKind_PRESENT},
           {proto::Stream_Kind_DATA, StreamKind::StreamKind_DATA},
           {proto::Stream_Kind_LENGTH, StreamKind::StreamKind_LENGTH},
           {proto::Stream_Kind_DICTIONARY_DATA, StreamKind::StreamKind_DICTIONARY_DATA},
           {proto::Stream_Kind_DICTIONARY_COUNT, StreamKind::StreamKind_DICTIONARY_COUNT},
           {proto::Stream_Kind_NANO_DATA, StreamKind::StreamKind_NANO_DATA},
           {proto::Stream_Kind_ROW_INDEX, StreamKind::StreamKind_ROW_INDEX},
           {proto::Stream_Kind_IN_DICTIONARY, StreamKind::StreamKind_IN_DICTIONARY}, {proto::Stream_Kind_STRIDE_DICTIONARY, StreamKind::StreamKind_STRIDE_DICTIONARY}, 
           {proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, StreamKind::StreamKind_STRIDE_DICTIONARY_LENGTH},
           {proto::Stream_Kind_BLOOM_FILTER_UTF8, StreamKind::StreamKind_BLOOM_FILTER_UTF8},
           {proto::Stream_Kind_IN_MAP, StreamKind::StreamKind_IN_MAP},
           // clang-format on
       }) {
    EXPECT_EQ(
        DwrfStreamIdentifier(createDwrfStream(dwrfStreamKind, {}, {}, {}))
            .kind(),
        veloxStreamKind);
  }

  for (auto [orcStreamKind, veloxStreamKind] :
       std::vector<std::tuple<proto::orc::Stream_Kind, StreamKind>>{
           // clang-format off
           {proto::orc::Stream_Kind_PRESENT, StreamKind::StreamKindOrc_PRESENT},
           {proto::orc::Stream_Kind_DATA, StreamKind::StreamKindOrc_DATA},
           {proto::orc::Stream_Kind_LENGTH, StreamKind::StreamKindOrc_LENGTH},
           {proto::orc::Stream_Kind_DICTIONARY_DATA, StreamKind::StreamKindOrc_DICTIONARY_DATA},
           {proto::orc::Stream_Kind_DICTIONARY_COUNT, StreamKind::StreamKindOrc_DICTIONARY_COUNT},
           {proto::orc::Stream_Kind_SECONDARY, StreamKind::StreamKindOrc_SECONDARY},
           {proto::orc::Stream_Kind_ROW_INDEX, StreamKind::StreamKindOrc_ROW_INDEX},
           {proto::orc::Stream_Kind_BLOOM_FILTER, StreamKind::StreamKindOrc_BLOOM_FILTER},
           {proto::orc::Stream_Kind_BLOOM_FILTER_UTF8, StreamKind::StreamKindOrc_BLOOM_FILTER_UTF8},
           {proto::orc::Stream_Kind_ENCRYPTED_INDEX, StreamKind::StreamKindOrc_ENCRYPTED_INDEX},
           {proto::orc::Stream_Kind_ENCRYPTED_DATA, StreamKind::StreamKindOrc_ENCRYPTED_DATA},
           {proto::orc::Stream_Kind_STRIPE_STATISTICS, StreamKind::StreamKindOrc_STRIPE_STATISTICS},
           {proto::orc::Stream_Kind_FILE_STATISTICS, StreamKind::StreamKindOrc_FILE_STATISTICS},
           // clang-format on
       }) {
    EXPECT_EQ(
        DwrfStreamIdentifier(createOrcStream(orcStreamKind, {})).kind(),
        veloxStreamKind);
  }
}

TEST_F(
    CommonTest,
    EncodingKeyGetKindFor_WithAllStreamKinds_GetCorrectConversion) {
  // DWRF
  for (auto [dwrfStreamKind, veloxStreamKind] :
       std::vector<std::tuple<proto::Stream_Kind, StreamKind>>{
           // clang-format off
           {proto::Stream_Kind_PRESENT, StreamKind::StreamKind_PRESENT},
           {proto::Stream_Kind_DATA, StreamKind::StreamKind_DATA},
           {proto::Stream_Kind_LENGTH, StreamKind::StreamKind_LENGTH},
           {proto::Stream_Kind_DICTIONARY_DATA, StreamKind::StreamKind_DICTIONARY_DATA},
           {proto::Stream_Kind_DICTIONARY_COUNT, StreamKind::StreamKind_DICTIONARY_COUNT},
           {proto::Stream_Kind_NANO_DATA, StreamKind::StreamKind_NANO_DATA},
           {proto::Stream_Kind_ROW_INDEX, StreamKind::StreamKind_ROW_INDEX},
           {proto::Stream_Kind_IN_DICTIONARY, StreamKind::StreamKind_IN_DICTIONARY}, {proto::Stream_Kind_STRIDE_DICTIONARY, StreamKind::StreamKind_STRIDE_DICTIONARY}, 
           {proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, StreamKind::StreamKind_STRIDE_DICTIONARY_LENGTH},
           {proto::Stream_Kind_BLOOM_FILTER_UTF8, StreamKind::StreamKind_BLOOM_FILTER_UTF8},
           {proto::Stream_Kind_IN_MAP, StreamKind::StreamKind_IN_MAP},
           // clang-format on
       }) {
    EncodingKey encodingKey;
    auto stream = encodingKey.forKind(dwrfStreamKind);

    EXPECT_EQ(stream.kind(), veloxStreamKind);
  }

  for (auto [orcStreamKind, veloxStreamKind] :
       std::vector<std::tuple<proto::orc::Stream_Kind, StreamKind>>{
           // clang-format off
           {proto::orc::Stream_Kind_PRESENT, StreamKind::StreamKindOrc_PRESENT},
           {proto::orc::Stream_Kind_DATA, StreamKind::StreamKindOrc_DATA},
           {proto::orc::Stream_Kind_LENGTH, StreamKind::StreamKindOrc_LENGTH},
           {proto::orc::Stream_Kind_DICTIONARY_DATA, StreamKind::StreamKindOrc_DICTIONARY_DATA},
           {proto::orc::Stream_Kind_DICTIONARY_COUNT, StreamKind::StreamKindOrc_DICTIONARY_COUNT},
           {proto::orc::Stream_Kind_SECONDARY, StreamKind::StreamKindOrc_SECONDARY},
           {proto::orc::Stream_Kind_ROW_INDEX, StreamKind::StreamKindOrc_ROW_INDEX},
           {proto::orc::Stream_Kind_BLOOM_FILTER, StreamKind::StreamKindOrc_BLOOM_FILTER},
           {proto::orc::Stream_Kind_BLOOM_FILTER_UTF8, StreamKind::StreamKindOrc_BLOOM_FILTER_UTF8},
           {proto::orc::Stream_Kind_ENCRYPTED_INDEX, StreamKind::StreamKindOrc_ENCRYPTED_INDEX},
           {proto::orc::Stream_Kind_ENCRYPTED_DATA, StreamKind::StreamKindOrc_ENCRYPTED_DATA},
           {proto::orc::Stream_Kind_STRIPE_STATISTICS, StreamKind::StreamKindOrc_STRIPE_STATISTICS},
           {proto::orc::Stream_Kind_FILE_STATISTICS, StreamKind::StreamKindOrc_FILE_STATISTICS},
           // clang-format on
       }) {
    EncodingKey encodingKey;
    auto stream = encodingKey.forKind(orcStreamKind);

    EXPECT_EQ(stream.kind(), veloxStreamKind);
  }
}
} // namespace facebook::velox::dwrf

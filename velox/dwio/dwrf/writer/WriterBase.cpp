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

#include "velox/dwio/dwrf/writer/WriterBase.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"

namespace facebook::velox::dwrf {

void WriterBase::writeFooter(const Type& type) {
  auto pos = writerSink_->size();
  footer_.set_headerlength(ORC_MAGIC_LEN);
  footer_.set_contentlength(pos - ORC_MAGIC_LEN);
  writerSink_->setMode(WriterSink::Mode::None);

  // write cache when available
  auto cacheSize = writerSink_->getCacheSize();
  if (cacheSize > 0) {
    writerSink_->writeCache();
    for (auto& i : writerSink_->getCacheOffsets()) {
      footer_.add_stripecacheoffsets(i);
    }
    pos = writerSink_->size();
  }

  ProtoUtils::writeType(type, footer_);
  DWIO_ENSURE_EQ(footer_.types_size(), footer_.statistics_size());
  auto writerVersion =
      static_cast<uint32_t>(context_->getConfig(Config::WRITER_VERSION));
  writeUserMetadata(writerVersion);
  footer_.set_numberofrows(context_->fileRowCount);
  footer_.set_rowindexstride(context_->indexStride);

  if (context_->fileRawSize > 0 || context_->fileRowCount == 0) {
    // ColumnTransformWriter, when rewriting presto written
    // file does not have rawSize.
    footer_.set_rawdatasize(context_->fileRawSize);
  }
  auto checksum = writerSink_->getChecksum();
  footer_.set_checksumalgorithm(
      checksum ? checksum->getType() : proto::ChecksumAlgorithm::NULL_);
  writeProto(footer_);
  auto footerLength = writerSink_->size() - pos;

  // write postscript
  pos = writerSink_->size();
  proto::PostScript ps;
  ps.set_writerversion(writerVersion);
  ps.set_footerlength(footerLength);
  ps.set_compression(
      static_cast<proto::CompressionKind>(context_->compression));
  if (context_->compression !=
      dwio::common::CompressionKind::CompressionKind_NONE) {
    ps.set_compressionblocksize(context_->compressionBlockSize);
  }
  ps.set_cachemode(writerSink_->getCacheMode());
  ps.set_cachesize(cacheSize);
  writeProto(ps, dwio::common::CompressionKind::CompressionKind_NONE);
  auto psLength = writerSink_->size() - pos;
  DWIO_ENSURE_LE(psLength, 0xff, "PostScript is too large: ", psLength);
  auto psLen = static_cast<char>(psLength);
  writerSink_->addBuffer(
      context_->getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM), &psLen, 1);
}

void WriterBase::writeUserMetadata(uint32_t writerVersion) {
  // add writer version
  userMetadata_[std::string{WRITER_NAME_KEY}] = kDwioWriter;
  userMetadata_[std::string{WRITER_VERSION_KEY}] =
      folly::to<std::string>(writerVersion);
  std::for_each(userMetadata_.begin(), userMetadata_.end(), [&](auto& pair) {
    auto item = footer_.add_metadata();
    item->set_name(pair.first);
    item->set_value(pair.second);
  });
}

} // namespace facebook::velox::dwrf

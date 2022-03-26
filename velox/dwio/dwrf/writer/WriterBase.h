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

#pragma once

#include "gtest/gtest_prod.h"
#include "velox/dwio/dwrf/writer/WriterContext.h"
#include "velox/dwio/dwrf/writer/WriterSink.h"

namespace facebook::velox::dwrf {

class WriterBase {
 public:
  explicit WriterBase(std::unique_ptr<dwio::common::DataSink> sink)
      : sink_{std::move(sink)} {
    DWIO_ENSURE_NOT_NULL(sink_);
  }

  virtual ~WriterBase() = default;

  virtual void close() {
    if (writerSink_) {
      writerSink_->flush();
    }
    sink_->close();
  }

  virtual void abort() {
    writerSink_ = nullptr;
    context_ = nullptr;
    if (sink_) {
      sink_->close();
      sink_ = nullptr;
    }
  }

  const WriterContext& getContext() const {
    DWIO_ENSURE_NOT_NULL(context_);
    return *context_;
  }

  WriterSink& getSink() {
    DWIO_ENSURE_NOT_NULL(writerSink_);
    return *writerSink_;
  }

  const WriterSink& getSink() const {
    DWIO_ENSURE_NOT_NULL(writerSink_);
    return *writerSink_;
  }

  void addUserMetadata(const std::string& key, const std::string& value) {
    userMetadata_[key] = value;
  }

 protected:
  void writeFooter(const Type& type);

  void initContext(
      const std::shared_ptr<const Config>& config,
      std::unique_ptr<velox::memory::ScopedMemoryPool> pool,
      std::unique_ptr<encryption::EncryptionHandler> handler = nullptr) {
    context_ = std::make_unique<WriterContext>(
        config, std::move(pool), sink_->getMetricsLog(), std::move(handler));
    writerSink_ = std::make_unique<WriterSink>(
        *sink_,
        context_->getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM),
        context_->getConfigs());
  }

  WriterContext& getContext() {
    DWIO_ENSURE_NOT_NULL(context_);
    return *context_;
  }

  template <typename T>
  void writeProto(const T& t) {
    writeProto(t, context_->compression);
  }

  template <typename T>
  void writeProto(const T& t, CompressionKind kind) {
    auto holder = context_->newDataBufferHolder();
    auto stream = context_->newStream(kind, *holder);

    t.SerializeToZeroCopyStream(stream.get());
    stream->flush();

    writerSink_->addBuffers(*holder);
  }

  template <typename T>
  void writeProtoAsString(
      std::string& output,
      const T& t,
      const dwio::common::encryption::Encrypter* encrypter = nullptr) {
    auto holder = context_->newDataBufferHolder();
    auto stream =
        context_->newStream(context_->compression, *holder, encrypter);

    t.SerializeToZeroCopyStream(stream.get());
    stream->flush();

    output.reserve(holder->size());
    for (auto& buffer : holder->getBuffers()) {
      output.append(buffer.data(), buffer.size());
    }
  }

  proto::StripeInformation& addStripeInfo() {
    auto stripe = footer_.add_stripes();
    stripe->set_numberofrows(context_->stripeRowCount);
    if (context_->stripeRawSize > 0 || context_->stripeRowCount == 0) {
      // ColumnTransformWriter, when rewriting presto written
      // file does not have rawSize.
      stripe->set_rawdatasize(context_->stripeRawSize);
    }

    auto checksum = writerSink_->getChecksum();
    if (checksum) {
      stripe->set_checksum(checksum->getDigest());
    }
    return *stripe;
  }

  proto::Footer& getFooter() {
    return footer_;
  }

  void validateStreamSize(
      const StreamIdentifier& streamId,
      uint64_t streamSize) {
    if (context_->isStreamSizeAboveThresholdCheckEnabled) {
      // Jolly doesn't support Streams bigger than 2GB.
      DWIO_ENSURE_LE(
          streamSize,
          std::numeric_limits<int32_t>::max(),
          "Stream is bigger than 2GB ",
          streamId.toString());
    }
  }

 private:
  std::unique_ptr<WriterContext> context_;
  std::unique_ptr<dwio::common::DataSink> sink_;
  std::unique_ptr<WriterSink> writerSink_;
  proto::Footer footer_;
  std::unordered_map<std::string, std::string> userMetadata_;

  void writeUserMetadata(uint32_t writerVersion);

  friend class WriterTest;
  FRIEND_TEST(WriterBaseTest, FlushWriterSinkUponClose);
};

} // namespace facebook::velox::dwrf

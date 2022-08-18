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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/dwrf/reader/ReaderBase.h"
#include "velox/dwio/dwrf/writer/WriterBase.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::type::fbhive;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

class WriterTest : public Test {
 public:
  WriterTest()
      : scopedPool{getDefaultScopedMemoryPool()}, pool{scopedPool->getPool()} {}

  WriterBase& createWriter(
      const std::shared_ptr<Config>& config,
      std::unique_ptr<DataSink> sink = nullptr) {
    if (!sink) {
      auto memSink = std::make_unique<MemorySink>(pool, 1024);
      sinkPtr = memSink.get();
      sink = std::move(memSink);
    }
    writer = std::make_unique<WriterBase>(std::move(sink));
    writer->initContext(config, pool.addScopedChild("test_writer_pool"));
    return *writer;
  }

  auto& getContext() {
    return writer->getContext();
  }

  auto& getFooter() {
    return writer->getFooter();
  }

  auto& addStripeInfo() {
    return writer->addStripeInfo();
  }

  void writeFooter(const Type& type) {
    writer->writeFooter(type);
  }

  void validateStreamSize(uint64_t streamSize) {
    DwrfStreamIdentifier si{1, 0, 0, proto::Stream_Kind_DATA};
    writer->validateStreamSize(si, streamSize);
  }

  std::unique_ptr<ScopedMemoryPool> scopedPool;
  MemoryPool& pool;
  MemorySink* sinkPtr;
  std::unique_ptr<WriterBase> writer;
};

TEST_F(WriterTest, WriteFooter) {
  auto config = std::make_shared<Config>();
  auto& writer = createWriter(config);
  auto& context = getContext();
  std::array<char, 10> data;
  std::memset(data.data(), 'a', 10);
  auto& writerSink = writer.getSink();

  for (size_t i = 0; i < 3; ++i) {
    writerSink.setMode(WriterSink::Mode::Index);
    writerSink.addBuffer(pool, data.data(), data.size());
    writerSink.setMode(WriterSink::Mode::Data);
    writerSink.addBuffer(pool, data.data(), data.size());
    writerSink.setMode(WriterSink::Mode::Footer);
    writerSink.addBuffer(pool, data.data(), data.size());
    writerSink.setMode(WriterSink::Mode::None);
    context.stripeRowCount = 123;
    context.stripeRawSize = 345;
    addStripeInfo();
    context.nextStripe();
  }

  std::string typeStr{"struct<a:int,b:float,c:string>"};
  HiveTypeParser parser;
  auto schema = parser.parse(typeStr);

  for (size_t i = 0; i < 2; ++i) {
    writer.addUserMetadata(
        folly::to<std::string>(i), folly::to<std::string>(i + 1));
  }
  for (size_t i = 0; i < 4; ++i) {
    getFooter().add_statistics();
  }
  writeFooter(*schema);
  writer.close();

  // deserialize and verify
  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());
  auto reader = std::make_unique<ReaderBase>(pool, std::move(input));

  auto& ps = reader->getPostScript();
  ASSERT_EQ(reader->getWriterVersion(), config->get(Config::WRITER_VERSION));
  ASSERT_EQ(reader->getCompressionKind(), config->get(Config::COMPRESSION));
  ASSERT_EQ(
      reader->getCompressionBlockSize(),
      config->get(Config::COMPRESSION_BLOCK_SIZE));
  ASSERT_EQ(ps.cacheSize(), (10 + 10) * 3);
  ASSERT_EQ(ps.cacheMode(), config->get(Config::STRIPE_CACHE_MODE));

  auto& footer = reader->getFooter();
  ASSERT_TRUE(footer.hasHeaderLength());
  ASSERT_EQ(footer.headerLength(), ORC_MAGIC_LEN);
  ASSERT_TRUE(footer.hasContentLength());
  ASSERT_EQ(footer.contentLength(), (10 + 10 + 10) * 3);
  ASSERT_EQ(footer.stripesSize(), 3);
  for (size_t i = 0; i < 3; ++i) {
    auto stripe = footer.stripes(i);
    ASSERT_EQ(stripe.rawDataSize(), 345);
    ASSERT_EQ(stripe.numberOfRows(), 123);
  }
  ASSERT_EQ(footer.typesSize(), 4);
  ASSERT_EQ(footer.metadataSize(), 5);
  for (size_t i = 0; i < 4; ++i) {
    auto item = footer.metadata(i);
    if (item.name() == WRITER_NAME_KEY) {
      ASSERT_EQ(item.value(), kDwioWriter);
    } else if (item.name() == WRITER_VERSION_KEY) {
      ASSERT_EQ(
          item.value(), folly::to<std::string>(reader->getWriterVersion()));
    } else if (item.name() == WRITER_HOSTNAME_KEY) {
      ASSERT_EQ(item.value(), process::getHostName());
    } else {
      ASSERT_EQ(
          folly::to<size_t>(item.name()) + 1, folly::to<size_t>(item.value()));
    }
  }
  ASSERT_TRUE(footer.hasNumberOfRows());
  ASSERT_EQ(footer.numberOfRows(), 123 * 3);
  ASSERT_TRUE(footer.hasRowIndexStride());
  ASSERT_EQ(footer.rowIndexStride(), config->get(Config::ROW_INDEX_STRIDE));
  ASSERT_TRUE(footer.hasRawDataSize());
  ASSERT_EQ(footer.rawDataSize(), 345 * 3);
  ASSERT_TRUE(footer.hasChecksumAlgorithm());
  ASSERT_EQ(
      footer.checksumAlgorithm(), config->get(Config::CHECKSUM_ALGORITHM));
  ASSERT_THAT(
      footer.stripeCacheOffsets(), ElementsAre(0, 10, 20, 30, 40, 50, 60));
  auto& cache = reader->getMetadataCache();
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(cache->has(StripeCacheMode::INDEX, i));
    ASSERT_TRUE(cache->has(StripeCacheMode::FOOTER, i));
  }
}

TEST_F(WriterTest, AddStripeInfo) {
  auto config = std::make_shared<Config>();
  auto& writer = createWriter(config);
  auto& context = getContext();

  context.stripeRowCount = 101;
  context.stripeRawSize = 202;
  std::array<char, 512> data;
  std::memset(data.data(), 'a', data.size());
  auto& writerSink = writer.getSink();
  writerSink.setMode(WriterSink::Mode::Data);
  writerSink.addBuffer(pool, data.data(), data.size());
  writerSink.setMode(WriterSink::Mode::None);

  auto& ret = addStripeInfo();
  ASSERT_EQ(ret.numberofrows(), 101);
  ASSERT_EQ(ret.rawdatasize(), 202);
  ASSERT_EQ(ret.checksum(), 8963334039576633799);
  writer.close();
}

TEST_F(WriterTest, NoChecksum) {
  auto config = std::make_shared<Config>();
  config->set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  auto& writer = createWriter(config);

  std::array<char, 512> data;
  std::memset(data.data(), 'a', data.size());
  auto& writerSink = writer.getSink();
  writerSink.setMode(WriterSink::Mode::Data);
  writerSink.addBuffer(pool, data.data(), data.size());
  writerSink.setMode(WriterSink::Mode::None);

  auto& ret = addStripeInfo();
  ASSERT_FALSE(ret.has_checksum());

  std::string typeStr{"struct<a:int,b:float,c:string>"};
  HiveTypeParser parser;
  auto schema = parser.parse(typeStr);
  for (size_t i = 0; i < 4; ++i) {
    getFooter().add_statistics();
  }
  writeFooter(*schema);
  writer.close();

  // deserialize and verify
  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());
  auto reader = std::make_unique<ReaderBase>(pool, std::move(input));
  auto& footer = reader->getFooter();
  ASSERT_TRUE(footer.hasChecksumAlgorithm());
  ASSERT_EQ(footer.checksumAlgorithm(), proto::ChecksumAlgorithm::NULL_);
}

TEST_F(WriterTest, NoCache) {
  auto config = std::make_shared<Config>();
  config->set(Config::STRIPE_CACHE_MODE, StripeCacheMode::NA);
  auto& writer = createWriter(config);

  std::array<char, 512> data;
  std::memset(data.data(), 'a', data.size());
  auto& writerSink = writer.getSink();
  writerSink.setMode(WriterSink::Mode::Index);
  writerSink.addBuffer(pool, data.data(), 10);
  writerSink.setMode(WriterSink::Mode::Data);
  writerSink.addBuffer(pool, data.data(), 10);
  writerSink.setMode(WriterSink::Mode::Footer);
  writerSink.addBuffer(pool, data.data(), 10);
  writerSink.setMode(WriterSink::Mode::None);

  addStripeInfo();

  std::string typeStr{"struct<a:int,b:float,c:string>"};
  HiveTypeParser parser;
  auto schema = parser.parse(typeStr);
  for (size_t i = 0; i < 4; ++i) {
    getFooter().add_statistics();
  }
  writeFooter(*schema);
  writer.close();

  // deserialize and verify
  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());
  auto reader = std::make_unique<ReaderBase>(pool, std::move(input));
  auto& footer = reader->getFooter();
  ASSERT_EQ(footer.stripeCacheOffsetsSize(), 0);
  auto& ps = reader->getPostScript();
  ASSERT_EQ(ps.cacheMode(), StripeCacheMode::NA);
  ASSERT_EQ(ps.cacheSize(), 0);
  ASSERT_EQ(reader->getMetadataCache(), nullptr);
}

TEST_F(WriterTest, ValidateStreamSizeConfigDisabled) {
  auto config = std::make_shared<Config>();
  config->set(Config::STREAM_SIZE_ABOVE_THRESHOLD_CHECK_ENABLED, false);
  auto& writer = createWriter(config);
  validateStreamSize(std::numeric_limits<uint64_t>::max());
  validateStreamSize(std::numeric_limits<uint32_t>::max());
  writer.close();
}

TEST_F(WriterTest, ValidateStreamSizeConfigEnabled) {
  auto config = std::make_shared<Config>();
  ASSERT_TRUE(config->get(Config::STREAM_SIZE_ABOVE_THRESHOLD_CHECK_ENABLED));
  auto& writer = createWriter(config);
  validateStreamSize(std::numeric_limits<int32_t>::max());

  uint32_t int32Max = std::numeric_limits<int32_t>::max();
  EXPECT_THROW(validateStreamSize(int32Max + 1), exception::LoggedException);

  EXPECT_THROW(
      validateStreamSize(std::numeric_limits<uint64_t>::max()),
      exception::LoggedException);

  EXPECT_THROW(
      validateStreamSize(std::numeric_limits<uint32_t>::max()),
      exception::LoggedException);
  writer.close();
}

class FailingSink : public DataSink {
 public:
  FailingSink() : DataSink{"FailingSink", MetricsLog::voidLog(), nullptr} {}

  virtual ~FailingSink() override {}

  using DataSink::write;

  void write(std::vector<DataBuffer<char>>&) override {
    DWIO_RAISE("Unexpected call");
  }
};

void abandonWriterWithoutClosing() {
  WriterBase writer{std::make_unique<FailingSink>()};

  auto guard = folly::makeGuard([&] { writer.abort(); });
  throw std::runtime_error("Some error");
  // If not for error, guard should be dismissed like this
  // guard.dismiss();
}

TEST_F(WriterTest, DoNotCrashDbgModeOnAbort) {
  EXPECT_THROW(abandonWriterWithoutClosing(), std::runtime_error);
}

class MockDataSink : public dwio::common::DataSink {
 public:
  explicit MockDataSink() : DataSink("mock_data_sink", nullptr) {}
  virtual ~MockDataSink() = default;

  MOCK_METHOD(uint64_t, size, (), (const override));
  MOCK_METHOD(bool, isBuffered, (), (const override));
  MOCK_METHOD(void, write, (std::vector<DataBuffer<char>>&));
};

TEST(WriterBaseTest, FlushWriterSinkUponClose) {
  auto config = std::make_shared<Config>();
  auto pool = getDefaultScopedMemoryPool();
  auto sink = std::make_unique<MockDataSink>();
  MockDataSink* sinkPtr = sink.get();
  EXPECT_CALL(*sinkPtr, write(_)).Times(1);
  EXPECT_CALL(*sinkPtr, isBuffered()).WillOnce(Return(false));
  {
    auto writer = std::make_unique<WriterBase>(std::move(sink));
    writer->initContext(config, pool->addScopedChild("test_writer_pool"));
    writer->close();
  }
}

} // namespace facebook::velox::dwrf

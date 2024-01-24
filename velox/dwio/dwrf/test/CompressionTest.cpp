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

#include "velox/dwio/dwrf/common/Compression.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/encryption/TestProvider.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/test/OrcTest.h"

#include <folly/Random.h>
#include <gtest/gtest.h>
#include "velox/common/compression/Compression.h"

#include <algorithm>

using namespace ::testing;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::memory;
using facebook::velox::VeloxException;

const int32_t DEFAULT_MEM_STREAM_SIZE = 1024 * 1024 * 2; // 2M

class TestBufferPool : public CompressionBufferPool {
 public:
  TestBufferPool(MemoryPool& pool, uint64_t blockSize)
      : buffer_{std::make_unique<DataBuffer<char>>(
            pool,
            blockSize + PAGE_HEADER_SIZE)} {}

  std::unique_ptr<DataBuffer<char>> getBuffer(uint64_t /* unused */) override {
    VELOX_CHECK_NOT_NULL(buffer_);
    return std::move(buffer_);
  }

  void returnBuffer(std::unique_ptr<DataBuffer<char>> buffer) override {
    VELOX_CHECK_NULL(buffer_);
    VELOX_CHECK_NOT_NULL(buffer);
    buffer_ = std::move(buffer);
  }

 private:
  std::unique_ptr<DataBuffer<char>> buffer_;
};

void generateRandomData(char* data, size_t size, bool letter) {
  for (size_t i = 0; i < size; ++i) {
    if (letter) {
      bool capitalized = folly::Random::rand32() % 2 == 0;
      data[i] = capitalized
          ? static_cast<char>('A' + folly::Random::rand32() % 26)
          : static_cast<char>('a' + folly::Random::rand32() % 26);
    } else {
      data[i] = static_cast<char>(folly::Random::rand32() % 256);
    }
  }
}

void decompressAndVerify(
    const MemorySink& memSink,
    CompressionKind kind,
    uint64_t blockSize,
    const char* data,
    size_t size,
    MemoryPool& pool,
    const Decrypter* decrypter) {
  std::unique_ptr<SeekableInputStream> inputStream(
      new SeekableArrayInputStream(memSink.data(), memSink.size()));

  std::unique_ptr<SeekableInputStream> decompressStream = createDecompressor(
      kind,
      std::move(inputStream),
      blockSize,
      pool,
      "Test Comrpession",
      decrypter);

  const char* decompressedBuffer;
  int32_t decompressedSize;
  int32_t pos = 0;
  while (decompressStream->Next(
      reinterpret_cast<const void**>(&decompressedBuffer), &decompressedSize)) {
    for (int32_t i = 0; i < decompressedSize; ++i) {
      EXPECT_LT(static_cast<size_t>(pos), size);
      EXPECT_EQ(data[pos], decompressedBuffer[i]);
      ++pos;
    }
  }
  ASSERT_EQ(pos, size);
}

void compressAndVerify(
    CompressionKind kind,
    FileSink& sink,
    uint64_t block,
    MemoryPool& pool,
    const char* data,
    size_t dataSize,
    const Encrypter* encrypter) {
  TestBufferPool bufferPool(pool, block);
  DataBufferHolder holder{
      pool, block, 0, DEFAULT_PAGE_GROW_RATIO, std::addressof(sink)};
  Config config;
  config.set<uint32_t>(Config::COMPRESSION_THRESHOLD, 128);
  std::unique_ptr<BufferedOutputStream> compressStream =
      createCompressor(kind, bufferPool, holder, config, encrypter);

  size_t pos = 0;
  char* compressBuffer;
  int32_t compressBufferSize = 0;
  while (dataSize > 0 &&
         compressStream->Next(
             reinterpret_cast<void**>(&compressBuffer), &compressBufferSize)) {
    size_t copy_size =
        std::min(static_cast<size_t>(compressBufferSize), dataSize);
    memcpy(compressBuffer, data + pos, copy_size);

    if (copy_size == dataSize) {
      compressStream->BackUp(
          compressBufferSize - static_cast<int32_t>(dataSize));
    }

    pos += copy_size;
    dataSize -= copy_size;
  }

  EXPECT_EQ(0, dataSize);
  compressStream->flush();
}

typedef std::tuple<CompressionKind, const Encrypter*, const Decrypter*>
    TestParams;
TestEncrypter testEncrypter;
TestDecrypter testDecrypter;

class CompressionTest : public TestWithParam<TestParams> {
 public:
  static void SetUpTestCase() {
    MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    auto tuple = GetParam();
    kind_ = std::get<0>(tuple);
    encrypter_ = std::get<1>(tuple);
    decrypter_ = std::get<2>(tuple);
  }

 protected:
  CompressionKind kind_;
  const Encrypter* encrypter_;
  const Decrypter* decrypter_;
  std::shared_ptr<MemoryPool> pool_ = memoryManager()->addLeafPool();
};

TEST_P(CompressionTest, compressOriginalString) {
  MemorySink memSink(DEFAULT_MEM_STREAM_SIZE, {.pool = pool_.get()});

  uint64_t block = 128;

  // simple, short string which will result in the original being saved
  char testData[] = "hello world!";
  compressAndVerify(
      kind_, memSink, block, *pool_, testData, sizeof(testData), encrypter_);
  decompressAndVerify(
      memSink, kind_, block, testData, sizeof(testData), *pool_, decrypter_);
}

TEST_P(CompressionTest, compressSimpleRepeatedString) {
  MemorySink memSink(DEFAULT_MEM_STREAM_SIZE, {.pool = pool_.get()});

  constexpr uint64_t block = 128;

  // simple repeated string (128 'a's) which should be compressed
  char testData[block];
  std::memset(testData, 'a', block);
  compressAndVerify(kind_, memSink, block, *pool_, testData, block, encrypter_);
  decompressAndVerify(
      memSink, kind_, block, testData, block, *pool_, decrypter_);
}

TEST_P(CompressionTest, compressTwoBlocks) {
  MemorySink memSink(DEFAULT_MEM_STREAM_SIZE, {.pool = pool_.get()});

  uint64_t block = 128;
  constexpr size_t size = 170;

  // testData will be compressed in two blocks
  char testData[size];
  std::memset(testData, 'a', size);
  compressAndVerify(kind_, memSink, block, *pool_, testData, size, encrypter_);
  decompressAndVerify(
      memSink, kind_, block, testData, size, *pool_, decrypter_);
}

TEST_P(CompressionTest, compressRandomLetters) {
  MemorySink memSink(DEFAULT_MEM_STREAM_SIZE, {.pool = pool_.get()});

  uint64_t block = 1024;
  constexpr size_t dataSize = 1024 * 1024; // 1M

  // testData will be compressed in two blocks
  char testData[dataSize];
  generateRandomData(testData, dataSize, true);
  compressAndVerify(
      kind_, memSink, block, *pool_, testData, dataSize, encrypter_);
  decompressAndVerify(
      memSink, kind_, block, testData, dataSize, *pool_, decrypter_);
}

TEST_P(CompressionTest, compressRandomBytes) {
  MemorySink memSink(DEFAULT_MEM_STREAM_SIZE, {.pool = pool_.get()});

  uint64_t block = 1024;
  constexpr size_t dataSize = 1024 * 1024; // 1M

  // testData will be compressed in two blocks
  char testData[dataSize];
  generateRandomData(testData, dataSize, false);
  compressAndVerify(
      kind_, memSink, block, *pool_, testData, dataSize, encrypter_);
  decompressAndVerify(
      memSink, kind_, block, testData, dataSize, *pool_, decrypter_);
}

void verifyProto(
    const MemorySink& memSink,
    CompressionKind kind,
    uint64_t block,
    MemoryPool& pool,
    proto::PostScript& expected,
    const Decrypter* decrypter) {
  std::unique_ptr<SeekableInputStream> inputStream(
      new SeekableArrayInputStream(memSink.data(), memSink.size()));

  std::unique_ptr<SeekableInputStream> decompressStream = createDecompressor(
      kind, std::move(inputStream), block, pool, "Test Comrpession", decrypter);

  proto::PostScript ps;
  ps.ParseFromZeroCopyStream(decompressStream.get());

  EXPECT_EQ(expected.footerlength(), ps.footerlength());
  EXPECT_EQ(expected.compression(), ps.compression());
  EXPECT_EQ(expected.writerversion(), ps.writerversion());
}

TEST_P(CompressionTest, compressProtoBuf) {
  MemorySink memSink(DEFAULT_MEM_STREAM_SIZE, {.pool = pool_.get()});

  uint64_t block = 256;

  proto::PostScript ps;
  ps.set_footerlength(197934);
  ps.set_compression(
      static_cast<proto::CompressionKind>(static_cast<int32_t>(kind_)));
  ps.set_writerversion(789);

  TestBufferPool bufferPool(*pool_, block);
  DataBufferHolder holder{*pool_, block, 0, DEFAULT_PAGE_GROW_RATIO, &memSink};
  Config config;
  std::unique_ptr<BufferedOutputStream> compressStream =
      createCompressor(kind_, bufferPool, holder, config, encrypter_);

  EXPECT_TRUE(ps.SerializeToZeroCopyStream(compressStream.get()));
  compressStream->flush();

  verifyProto(memSink, kind_, block, *pool_, ps, decrypter_);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TestCompression,
    CompressionTest,
    Values(
        std::make_tuple(CompressionKind_ZLIB, nullptr, nullptr),
        std::make_tuple(CompressionKind_ZLIB, &testEncrypter, &testDecrypter),
        std::make_tuple(CompressionKind_ZSTD, nullptr, nullptr),
        std::make_tuple(CompressionKind_ZSTD, &testEncrypter, &testDecrypter),
        std::make_tuple(CompressionKind_NONE, nullptr, nullptr),
        std::make_tuple(CompressionKind_NONE, &testEncrypter, &testDecrypter)));

typedef std::tuple<CompressionKind, const Encrypter*> TestParams2;

class RecordPositionTest : public TestWithParam<TestParams2> {
 public:
  static void SetUpTestCase() {
    MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    auto tuple = GetParam();
    kind_ = std::get<0>(tuple);
    encrypter_ = std::get<1>(tuple);
  }

 protected:
  CompressionKind kind_;
  const Encrypter* encrypter_;
  std::shared_ptr<MemoryPool> pool_ = memoryManager()->addLeafPool();
};

TEST_P(RecordPositionTest, testRecordPosition) {
  MemorySink memSink(DEFAULT_MEM_STREAM_SIZE, {.pool = pool_.get()});
  uint64_t block = 256;
  uint64_t initial = 128;

  TestBufferPool bufferPool(*pool_, block);
  DataBufferHolder holder{
      *pool_, block, initial, DEFAULT_PAGE_GROW_RATIO, &memSink};
  Config config;
  std::unique_ptr<BufferedOutputStream> stream =
      createCompressor(kind_, bufferPool, holder, config, encrypter_);

  TestPositionRecorder recorder;
  EXPECT_EQ(stream->size(), 0);
  stream->recordPosition(recorder, 3, 2);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 2);
    EXPECT_EQ(pos.at(0), 0);
    EXPECT_EQ(pos.at(1), 0);
  }

  int32_t size;
  void* data;
  stream->Next(&data, &size);
  EXPECT_EQ(size, initial);
  recorder.addEntry();
  EXPECT_EQ(stream->size(), 0);
  stream->recordPosition(recorder, size, 100);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 2);
    EXPECT_EQ(pos.at(0), 0);
    EXPECT_EQ(pos.at(1), 100);
  }

  stream->Next(&data, &size);
  EXPECT_EQ(size, block - initial);
  recorder.addEntry();
  EXPECT_EQ(stream->size(), 0);
  stream->recordPosition(recorder, size, 100);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 2);
    EXPECT_EQ(pos.at(0), 0);
    EXPECT_EQ(pos.at(1), 100 + initial);
  }

  stream->Next(&data, &size);
  EXPECT_EQ(size, block);
  recorder.addEntry();
  EXPECT_GT(stream->size(), PAGE_HEADER_SIZE);
  stream->recordPosition(recorder, size, 100);
  {
    auto& pos = recorder.getPositions();
    EXPECT_EQ(pos.size(), 2);
    EXPECT_EQ(pos.at(0), stream->size());
    EXPECT_EQ(pos.at(1), 100);
  }

  recorder.addEntry();
  stream->recordPosition(recorder, size, 100, 4);
  auto& pos = recorder.getPositions(4);
  EXPECT_EQ(pos.size(), 2);
  EXPECT_EQ(pos.at(0), stream->size());
  EXPECT_EQ(pos.at(1), 100);
}

TEST_P(CompressionTest, getCompressionBufferOOM) {
  MemorySink memSink(10L << 20, {.pool = pool_.get()});
  const uint64_t compressBlockSize{2L << 20};

  struct {
    bool oomOnNextCall;
    bool hasSink;

    std::string debugString() const {
      return fmt::format("oomOnNextCall:{} hasSink:{}", oomOnNextCall, hasSink);
    }
  } testSettings[] = {
      {true, true}, {true, false}, {false, true}, {false, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format(
        "{} compression {}",
        testData.debugString(),
        compressionKindToString(kind_)));

    auto config = std::make_shared<Config>();
    config->set<CompressionKind>(Config::COMPRESSION, kind_);
    config->set<uint32_t>(Config::COMPRESSION_THRESHOLD, compressBlockSize / 2);
    config->set<uint64_t>(Config::COMPRESSION_BLOCK_SIZE, compressBlockSize);
    WriterContext context{
        config,
        memoryManager()->addRootPool(
            "oomOnCompression",
            kind_ == facebook::velox::common::CompressionKind_NONE ? 3L << 20
                                                                   : 6L << 20)};
    context.initBuffer();

    DataBufferHolder holder{
        context.getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM),
        context.compressionBlockSize(),
        context.getConfigs().get(Config::COMPRESSION_BLOCK_SIZE_MIN),
        context.getConfigs().get(Config::COMPRESSION_BLOCK_SIZE_EXTEND_RATIO),
        testData.hasSink ? &memSink : nullptr};

    std::unique_ptr<BufferedOutputStream> compressStream = createCompressor(
        kind_, context, holder, context.getConfigs(), encrypter_);
    void* buffer;
    int32_t nextBufferSize;
    ASSERT_TRUE(
        compressStream->Next(&buffer, &nextBufferSize, compressBlockSize));
    char* data = static_cast<char*>(buffer);
    for (int i = 0; i < compressBlockSize; ++i) {
      data[i] = folly::Random::rand32() % 256;
    }
    if (testData.oomOnNextCall) {
      VELOX_ASSERT_THROW(
          compressStream->Next(&buffer, &nextBufferSize, compressBlockSize),
          "Exceeded memory pool cap");
    } else {
      VELOX_ASSERT_THROW(compressStream->flush(), "Exceeded memory pool cap");
    }
    if (kind_ != facebook::velox::common::CompressionKind_NONE) {
      auto compressionBuffer = context.getBuffer(0);
      ASSERT_TRUE(compressionBuffer != nullptr);
      context.returnBuffer(std::move(compressionBuffer));
    } else {
      ASSERT_TRUE(context.testingCompressionBuffer() == nullptr);
    }
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TestCompression,
    RecordPositionTest,
    Values(
        std::make_tuple(CompressionKind_ZLIB, nullptr),
        std::make_tuple(CompressionKind_ZLIB, &testEncrypter),
        std::make_tuple(CompressionKind_ZSTD, nullptr),
        std::make_tuple(CompressionKind_ZSTD, &testEncrypter),
        std::make_tuple(CompressionKind_NONE, &testEncrypter)));

TEST(CompressionOptionsTest, testCompressionOptions) {
  auto options = getDwrfOrcCompressionOptions(
      facebook::velox::common::CompressionKind_ZLIB, 256, 4, 7);

  EXPECT_EQ(
      options.format.zlib.windowBits, Compressor::DWRF_ORC_ZLIB_WINDOW_BITS);
  EXPECT_EQ(options.format.zlib.compressionLevel, 4);
  EXPECT_EQ(options.compressionThreshold, 256);

  options = getDwrfOrcCompressionOptions(
      facebook::velox::common::CompressionKind_ZSTD, 256, 4, 7);

  EXPECT_EQ(options.format.zstd.compressionLevel, 7);
  EXPECT_EQ(options.compressionThreshold, 256);
}

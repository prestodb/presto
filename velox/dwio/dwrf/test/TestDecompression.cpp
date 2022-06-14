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

#include <folly/Random.h>
#include <folly/String.h>
#include <folly/compression/Compression.h>
#include <folly/compression/Zlib.h>
#include <gtest/gtest.h>
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/dwrf/common/Compression.h"
#include "velox/dwio/dwrf/test/OrcTest.h"

#include <cstdio>
#include <cstring>

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::memory;
using namespace folly::io;

const std::string simpleFile(getExampleFilePath("simple-file.binary"));

std::unique_ptr<ScopedMemoryPool> scopedPool = getDefaultScopedMemoryPool();

TEST(TestDecompression, testPrintBufferEmpty) {
  std::ostringstream str;
  printBuffer(str, nullptr, 0);
  EXPECT_EQ("", str.str());
}

TEST(TestDecompression, testPrintBufferSmall) {
  std::vector<char> buffer(10);
  std::ostringstream str;
  for (size_t i = 0; i < 10; ++i) {
    buffer[i] = static_cast<char>(i);
  }
  printBuffer(str, buffer.data(), 10);
  EXPECT_EQ("0000000 00 01 02 03 04 05 06 07 08 09\n", str.str());
}

TEST(TestDecompression, testPrintBufferLong) {
  std::vector<char> buffer(300);
  std::ostringstream str;
  for (size_t i = 0; i < 300; ++i) {
    buffer[i] = static_cast<char>(i);
  }
  printBuffer(str, buffer.data(), 300);
  std::ostringstream expected;
  expected << "0000000 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f 10"
           << " 11 12 13 14 15 16 17\n"
           << "0000018 18 19 1a 1b 1c 1d 1e 1f 20 21 22 23 24 25 26 27 28"
           << " 29 2a 2b 2c 2d 2e 2f\n"
           << "0000030 30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f 40"
           << " 41 42 43 44 45 46 47\n"
           << "0000048 48 49 4a 4b 4c 4d 4e 4f 50 51 52 53 54 55 56 57 58"
           << " 59 5a 5b 5c 5d 5e 5f\n"
           << "0000060 60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f 70"
           << " 71 72 73 74 75 76 77\n"
           << "0000078 78 79 7a 7b 7c 7d 7e 7f 80 81 82 83 84 85 86 87 88"
           << " 89 8a 8b 8c 8d 8e 8f\n"
           << "0000090 90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f a0"
           << " a1 a2 a3 a4 a5 a6 a7\n"
           << "00000a8 a8 a9 aa ab ac ad ae af b0 b1 b2 b3 b4 b5 b6 b7 b8"
           << " b9 ba bb bc bd be bf\n"
           << "00000c0 c0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf d0"
           << " d1 d2 d3 d4 d5 d6 d7\n"
           << "00000d8 d8 d9 da db dc dd de df e0 e1 e2 e3 e4 e5 e6 e7 e8"
           << " e9 ea eb ec ed ee ef\n"
           << "00000f0 f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff 00"
           << " 01 02 03 04 05 06 07\n"
           << "0000108 08 09 0a 0b 0c 0d 0e 0f 10 11 12 13 14 15 16 17 18"
           << " 19 1a 1b 1c 1d 1e 1f\n"
           << "0000120 20 21 22 23 24 25 26 27 28 29 2a 2b\n";
  EXPECT_EQ(expected.str(), str.str());
}

TEST(TestDecompression, testArrayBackup) {
  std::vector<char> bytes(200);
  for (size_t i = 0; i < bytes.size(); ++i) {
    bytes[i] = static_cast<char>(i);
  }
  SeekableArrayInputStream stream(bytes.data(), bytes.size(), 20);
  const void* ptr;
  int32_t len;
  ASSERT_THROW(stream.BackUp(10), exception::LoggedException);
  EXPECT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(bytes.data(), static_cast<const char*>(ptr));
  EXPECT_EQ(20, len);
  stream.BackUp(0);
  EXPECT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(bytes.data() + 20, static_cast<const char*>(ptr));
  EXPECT_EQ(20, len);
  stream.BackUp(10);
  for (uint32_t i = 0; i < 8; ++i) {
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    uint32_t consumedBytes = 30 + 20 * i;
    EXPECT_EQ(bytes.data() + consumedBytes, static_cast<const char*>(ptr));
    EXPECT_EQ(consumedBytes + 20, stream.ByteCount());
    EXPECT_EQ(20, len);
  }
  EXPECT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(bytes.data() + 190, static_cast<const char*>(ptr));
  EXPECT_EQ(10, len);
  EXPECT_EQ(true, !stream.Next(&ptr, &len));
  EXPECT_EQ(0, len);
  ASSERT_THROW(stream.BackUp(30), exception::LoggedException);
  EXPECT_EQ(200, stream.ByteCount());
}

TEST(TestDecompression, testArraySkip) {
  std::vector<char> bytes(200);
  for (size_t i = 0; i < bytes.size(); ++i) {
    bytes[i] = static_cast<char>(i);
  }
  SeekableArrayInputStream stream(bytes.data(), bytes.size(), 20);
  const void* ptr;
  int32_t len;
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(bytes.data(), static_cast<const char*>(ptr));
  EXPECT_EQ(20, len);
  ASSERT_EQ(true, !stream.Skip(-10));
  ASSERT_EQ(true, stream.Skip(80));
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(bytes.data() + 100, static_cast<const char*>(ptr));
  EXPECT_EQ(20, len);
  ASSERT_EQ(true, stream.Skip(80));
  ASSERT_EQ(true, !stream.Next(&ptr, &len));
  ASSERT_EQ(true, !stream.Skip(181));
  EXPECT_EQ("SeekableArrayInputStream 200 of 200", stream.getName());
}

TEST(TestDecompression, testArrayCombo) {
  std::vector<char> bytes(200);
  for (size_t i = 0; i < bytes.size(); ++i) {
    bytes[i] = static_cast<char>(i);
  }
  SeekableArrayInputStream stream(bytes.data(), bytes.size(), 20);
  const void* ptr;
  int32_t len;
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(bytes.data(), static_cast<const char*>(ptr));
  EXPECT_EQ(20, len);
  stream.BackUp(10);
  EXPECT_EQ(10, stream.ByteCount());
  stream.Skip(4);
  EXPECT_EQ(14, stream.ByteCount());
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(bytes.data() + 14, static_cast<const char*>(ptr));
  EXPECT_EQ(true, !stream.Skip(320));
  EXPECT_EQ(200, stream.ByteCount());
  EXPECT_EQ(true, !stream.Next(&ptr, &len));
}

// this checks to make sure that a given set of bytes are ascending
void checkBytes(const char* data, int32_t length, uint32_t startValue) {
  for (uint32_t i = 0; static_cast<int32_t>(i) < length; ++i) {
    EXPECT_EQ(startValue + i, static_cast<unsigned char>(data[i]))
        << "Output wrong at " << startValue << " + " << i;
  }
}

TEST(TestDecompression, testFileBackup) {
  SCOPED_TRACE("testFileBackup");
  std::unique_ptr<InputStream> file =
      std::make_unique<FileInputStream>(simpleFile);
  SeekableFileInputStream stream(
      *file, 0, 200, scopedPool->getPool(), LogType::TEST, 20);
  const void* ptr;
  int32_t len;
  ASSERT_THROW(stream.BackUp(10), exception::LoggedException);
  EXPECT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(20, len);
  checkBytes(static_cast<const char*>(ptr), len, 0);
  stream.BackUp(0);
  EXPECT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(20, len);
  checkBytes(static_cast<const char*>(ptr), len, 20);
  stream.BackUp(10);
  EXPECT_EQ(30, stream.ByteCount());
  EXPECT_EQ(true, stream.Next(&ptr, &len));
  EXPECT_EQ(10, len);
  checkBytes(static_cast<const char*>(ptr), len, 30);
  for (uint32_t i = 0; i < 8; ++i) {
    EXPECT_EQ(20 * i + 40, stream.ByteCount());
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(20, len);
    checkBytes(static_cast<const char*>(ptr), len, 20 * i + 40);
  }
  EXPECT_EQ(true, !stream.Next(&ptr, &len));
  EXPECT_EQ(0, len);
  ASSERT_THROW(stream.BackUp(30), exception::LoggedException);
  EXPECT_EQ(200, stream.ByteCount());
}

TEST(TestDecompression, testFileSkip) {
  SCOPED_TRACE("testFileSkip");
  std::unique_ptr<InputStream> file =
      std::make_unique<FileInputStream>(simpleFile);
  SeekableFileInputStream stream(
      *file, 0, 200, scopedPool->getPool(), LogType::TEST, 20);
  const void* ptr;
  int32_t len;
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  checkBytes(static_cast<const char*>(ptr), len, 0);
  EXPECT_EQ(20, len);
  ASSERT_EQ(true, !stream.Skip(-10));
  ASSERT_EQ(true, stream.Skip(80));
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  checkBytes(static_cast<const char*>(ptr), len, 100);
  EXPECT_EQ(20, len);
  ASSERT_EQ(true, !stream.Skip(80));
  ASSERT_EQ(true, !stream.Next(&ptr, &len));
  ASSERT_EQ(true, !stream.Skip(181));
  EXPECT_EQ(std::string(simpleFile) + " from 0 for 200", stream.getName());
}

TEST(TestDecompression, testFileCombo) {
  SCOPED_TRACE("testFileCombo");
  std::unique_ptr<InputStream> file =
      std::make_unique<FileInputStream>(simpleFile);
  SeekableFileInputStream stream(
      *file, 0, 200, scopedPool->getPool(), LogType::TEST, 20);
  const void* ptr;
  int32_t len;
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  checkBytes(static_cast<const char*>(ptr), len, 0);
  EXPECT_EQ(20, len);
  stream.BackUp(10);
  EXPECT_EQ(10, stream.ByteCount());
  stream.Skip(4);
  EXPECT_EQ(14, stream.ByteCount());
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  checkBytes(static_cast<const char*>(ptr), len, 14);
  EXPECT_EQ(true, !stream.Skip(320));
  EXPECT_EQ(200, stream.ByteCount());
  EXPECT_EQ(true, !stream.Next(&ptr, &len));
}

TEST(TestDecompression, testFileSeek) {
  SCOPED_TRACE("testFileSeek");
  std::unique_ptr<InputStream> file =
      std::make_unique<FileInputStream>(simpleFile);
  SeekableFileInputStream stream(
      *file, 0, 200, scopedPool->getPool(), LogType::TEST, 20);
  const void* ptr;
  int32_t len;
  EXPECT_EQ(0, stream.ByteCount());
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  checkBytes(static_cast<const char*>(ptr), len, 0);
  EXPECT_EQ(20, len);
  EXPECT_EQ(20, stream.ByteCount());
  {
    std::vector<uint64_t> offsets(1, 100);
    PositionProvider posn(offsets);
    stream.seekToPosition(posn);
  }
  EXPECT_EQ(100, stream.ByteCount());
  {
    std::vector<uint64_t> offsets(1, 5);
    PositionProvider posn(offsets);
    stream.seekToPosition(posn);
  }
  EXPECT_EQ(5, stream.ByteCount());
  ASSERT_EQ(true, stream.Next(&ptr, &len));
  checkBytes(static_cast<const char*>(ptr), len, 5);
  EXPECT_EQ(20, len);
  {
    std::vector<uint64_t> offsets(1, 201);
    PositionProvider posn(offsets);
    EXPECT_THROW(stream.seekToPosition(posn), exception::LoggedException);
  }
}

namespace {
std::unique_ptr<SeekableInputStream> createTestDecompressor(
    CompressionKind kind,
    std::unique_ptr<SeekableInputStream> input,
    uint64_t bufferSize) {
  return createDecompressor(
      kind,
      std::move(input),
      bufferSize,
      scopedPool->getPool(),
      "Test Decompression");
}

} // namespace

TEST(TestDecompression, testCreateNone) {
  std::vector<char> bytes(10);
  for (uint32_t i = 0; i < bytes.size(); ++i) {
    bytes[i] = static_cast<char>(i);
  }
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_NONE,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(bytes.data(), bytes.size())),
      32768);
  const void* ptr;
  int32_t length;
  result->Next(&ptr, &length);
  for (uint32_t i = 0; i < bytes.size(); ++i) {
    EXPECT_EQ(static_cast<char>(i), static_cast<const char*>(ptr)[i]);
  }
}

TEST(TestDecompression, testLzoEmpty) {
  const unsigned char buffer[] = {0};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_LZO,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, 0)),
      32768);
  EXPECT_EQ(
      "PagedInputStream StreamInfo (Test Decompression) input stream (SeekableArrayInputStream 0 of 0) State (0) remaining length (0)",
      result->getName());
  const void* ptr;
  int32_t length;
  ASSERT_TRUE(!result->Next(&ptr, &length));
}

TEST(TestDecompression, testLzoSmall) {
  const unsigned char buffer[] = {
      70,  0,   0,   48,  88,  88,  88, 88, 97, 98, 99, 100, 97,
      98,  99,  100, 65,  66,  67,  68, 65, 66, 67, 68, 119, 120,
      121, 122, 119, 122, 121, 122, 49, 50, 51, 17, 0,  0};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_LZO,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))),
      128 * 1024);
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  const char* expected = "XXXXabcdabcdABCDABCDwxyzwzyz123";
  ASSERT_EQ(strlen(expected), length);
  for (uint64_t i = 0; i < length; ++i) {
    ASSERT_EQ(
        static_cast<const char>(expected[i]), static_cast<const char*>(ptr)[i]);
  }
  ASSERT_TRUE(!result->Next(&ptr, &length));
}

TEST(TestDecompression, testLzoLong) {
  // set up a framed lzo buffer with 100,000 'a'
  unsigned char buffer[482];
  bzero(buffer, VELOX_ARRAY_SIZE(buffer));
  // header
  buffer[0] = 190;
  buffer[1] = 3;

  // lzo data
  buffer[3] = 2;
  memset(buffer + 4, 97, 5);
  buffer[9] = 32;
  buffer[202] = 134;
  buffer[203] = 16;
  buffer[206] = 3;
  memset(buffer + 207, 97, 21);
  buffer[228] = 32;
  buffer[421] = 138;
  buffer[425] = 3;
  memset(buffer + 426, 97, 21);
  buffer[447] = 32;
  buffer[454] = 112;
  buffer[458] = 2;
  memset(buffer + 459, 97, 20);
  buffer[479] = 17;
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_LZO,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))),
      128 * 1024);
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(100000, length);
  for (uint64_t i = 0; i < length; ++i) {
    ASSERT_EQ('a', static_cast<const char*>(ptr)[i]);
  }
  ASSERT_TRUE(!result->Next(&ptr, &length));
}

TEST(TestDecompression, testLz4Empty) {
  const unsigned char buffer[] = {0};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_LZ4,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, 0)),
      32768);
  EXPECT_EQ(
      "PagedInputStream StreamInfo (Test Decompression) input stream (SeekableArrayInputStream 0 of 0) State (0) remaining length (0)",
      result->getName());
  const void* ptr;
  int32_t length;
  ASSERT_TRUE(!result->Next(&ptr, &length));
}

TEST(TestDecompression, testLz4Small) {
  const unsigned char buffer[] = {60,  0,   0,   128, 88,  88,  88,  88,  97,
                                  98,  99,  100, 4,   0,   64,  65,  66,  67,
                                  68,  4,   0,   176, 119, 120, 121, 122, 119,
                                  122, 121, 122, 49,  50,  51};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_LZ4,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))),
      128 * 1024);
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  const char* expected = "XXXXabcdabcdABCDABCDwxyzwzyz123";
  ASSERT_EQ(strlen(expected), length);
  for (uint64_t i = 0; i < length; ++i) {
    ASSERT_EQ(
        static_cast<const char>(expected[i]), static_cast<const char*>(ptr)[i]);
  }
  ASSERT_TRUE(!result->Next(&ptr, &length));
}

TEST(TestDecompression, testLz4Long) {
  // set up a framed lzo buffer with 100,000 'a'
  unsigned char buffer[406];
  memset(buffer, 255, VELOX_ARRAY_SIZE(buffer));
  // header
  buffer[0] = 38;
  buffer[1] = 3;
  buffer[2] = 0;

  // lz4 data
  buffer[3] = 31;
  buffer[4] = 97;
  buffer[5] = 1;
  buffer[6] = 0;
  buffer[399] = 15;
  buffer[400] = 80;
  memset(buffer + 401, 97, 5);

  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_LZ4,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))),
      128 * 1024);
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(100000, length);
  for (uint64_t i = 0; i < length; ++i) {
    ASSERT_EQ('a', static_cast<const char*>(ptr)[i]);
  }
  ASSERT_TRUE(!result->Next(&ptr, &length));
}

TEST(TestDecompression, testCreateZlib) {
  const unsigned char buffer[] = {0x0b, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_ZLIB,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))),
      32768);
  EXPECT_EQ(
      "PagedInputStream StreamInfo (Test Decompression) input stream (SeekableArrayInputStream 0 of 8) State (0) remaining length (0)",
      result->getName());
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(5, length);
  for (uint32_t i = 0; i < 5; ++i) {
    EXPECT_EQ(static_cast<char>(i), static_cast<const char*>(ptr)[i]);
  }
  EXPECT_EQ(
      "PagedInputStream StreamInfo (Test Decompression) input stream (SeekableArrayInputStream 8 of 8) State (2) remaining length (0)",
      result->getName());
  EXPECT_EQ(5, result->ByteCount());
  result->BackUp(3);
  EXPECT_EQ(2, result->ByteCount());
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(3, length);
  for (uint32_t i = 0; i < 3; ++i) {
    EXPECT_EQ(static_cast<char>(i + 2), static_cast<const char*>(ptr)[i]);
  }
}

TEST(TestDecompression, testLiteralBlocks) {
  const unsigned char buffer[] = {0x19, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4,
                                  0x5,  0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xb,
                                  0x0,  0x0, 0xc, 0xd, 0xe, 0xf, 0x10};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_ZLIB,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer), 5)),
      5);
  EXPECT_EQ(
      "PagedInputStream StreamInfo (Test Decompression) input stream (SeekableArrayInputStream 0 of 23) State (0) remaining length (0)",
      result->getName());
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(2, length);
  EXPECT_EQ(0, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(1, static_cast<const char*>(ptr)[1]);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(5, length);
  EXPECT_EQ(2, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(3, static_cast<const char*>(ptr)[1]);
  EXPECT_EQ(4, static_cast<const char*>(ptr)[2]);
  EXPECT_EQ(5, static_cast<const char*>(ptr)[3]);
  EXPECT_EQ(6, static_cast<const char*>(ptr)[4]);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(5, length);
  EXPECT_EQ(7, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(8, static_cast<const char*>(ptr)[1]);
  EXPECT_EQ(9, static_cast<const char*>(ptr)[2]);
  EXPECT_EQ(10, static_cast<const char*>(ptr)[3]);
  EXPECT_EQ(11, static_cast<const char*>(ptr)[4]);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(2, length);
  EXPECT_EQ(12, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(13, static_cast<const char*>(ptr)[1]);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(3, length);
  EXPECT_EQ(14, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(15, static_cast<const char*>(ptr)[1]);
  EXPECT_EQ(16, static_cast<const char*>(ptr)[2]);
}

TEST(TestDecompression, testInflate) {
  const unsigned char buffer[] = {
      0xe, 0x0, 0x0, 0x63, 0x60, 0x64, 0x62, 0xc0, 0x8d, 0x0};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_ZLIB,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer))),
      1000);
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(30, length);
  for (int32_t i = 0; i < 10; ++i) {
    for (int32_t j = 0; j < 3; ++j) {
      EXPECT_EQ(j, static_cast<const char*>(ptr)[i * 3 + j]);
    }
  }
}

TEST(TestDecompression, testInflateSequence) {
  const unsigned char buffer[] = {0xe,  0x0,  0x0,  0x63, 0x60, 0x64, 0x62,
                                  0xc0, 0x8d, 0x0,  0xe,  0x0,  0x0,  0x63,
                                  0x60, 0x64, 0x62, 0xc0, 0x8d, 0x0};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_ZLIB,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer), 3)),
      1000);
  const void* ptr;
  int32_t length;
  ASSERT_THROW(result->BackUp(20), exception::LoggedException);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(30, length);
  for (int32_t i = 0; i < 10; ++i) {
    for (int32_t j = 0; j < 3; ++j) {
      EXPECT_EQ(j, static_cast<const char*>(ptr)[i * 3 + j]);
    }
  }
  result->BackUp(8);
  result->BackUp(2);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(10, length);
  for (int32_t i = 0; i < 10; ++i) {
    EXPECT_EQ((i + 2) % 3, static_cast<const char*>(ptr)[i]);
  }
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(30, length);
  for (int32_t i = 0; i < 10; ++i) {
    for (int32_t j = 0; j < 3; ++j) {
      EXPECT_EQ(j, static_cast<const char*>(ptr)[i * 3 + j]);
    }
  }
}

TEST(TestDecompression, testSkipZlib) {
  const unsigned char buffer[] = {0x19, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4,
                                  0x5,  0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xb,
                                  0x0,  0x0, 0xc, 0xd, 0xe, 0xf, 0x10};
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_ZLIB,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(buffer, VELOX_ARRAY_SIZE(buffer), 5)),
      5);
  const void* ptr;
  int32_t length;
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(2, length);
  result->Skip(2);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(3, length);
  EXPECT_EQ(4, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(5, static_cast<const char*>(ptr)[1]);
  EXPECT_EQ(6, static_cast<const char*>(ptr)[2]);
  result->BackUp(2);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(2, length);
  EXPECT_EQ(5, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(6, static_cast<const char*>(ptr)[1]);
  result->Skip(8);
  ASSERT_EQ(true, result->Next(&ptr, &length));
  ASSERT_EQ(2, length);
  EXPECT_EQ(15, static_cast<const char*>(ptr)[0]);
  EXPECT_EQ(16, static_cast<const char*>(ptr)[1]);
}

#define HEADER_SIZE 3

class CompressBuffer {
  std::vector<char> buf;

 public:
  explicit CompressBuffer(size_t capacity) : buf(capacity + HEADER_SIZE) {}

  char* getCompressed() {
    return buf.data() + HEADER_SIZE;
  }
  char* getBuffer() {
    return buf.data();
  }

  void writeHeader(size_t compressedSize) {
    buf[0] = static_cast<char>(compressedSize << 1);
    buf[1] = static_cast<char>(compressedSize >> 7);
    buf[2] = static_cast<char>(compressedSize >> 15);
  }

  void writeUncompressedHeader(size_t compressedSize) {
    buf[0] = static_cast<char>(compressedSize << 1) | 1;
    buf[1] = static_cast<char>(compressedSize >> 7);
    buf[2] = static_cast<char>(compressedSize >> 15);
  }

  size_t getCompressedSize() const {
    size_t header = static_cast<unsigned char>(buf[0]);
    header |= static_cast<size_t>(static_cast<unsigned char>(buf[1])) << 8;
    header |= static_cast<size_t>(static_cast<unsigned char>(buf[2])) << 16;
    return header >> 1;
  }

  size_t getBufferSize() const {
    return getCompressedSize() + HEADER_SIZE;
  }
};

TEST(TestDecompression, testBasic) {
  const int32_t N = 1024;
  std::vector<char> buf(N * sizeof(int));
  for (int32_t i = 0; i < N; ++i) {
    (reinterpret_cast<int32_t*>(buf.data()))[i] = i % 8;
  }

  auto ioBuf = folly::IOBuf::wrapBuffer(buf.data(), buf.size());
  auto compressedBuf = getCodec(CodecType::SNAPPY)->compress(ioBuf.get());
  auto compressedSize = compressedBuf->length();
  // compressed size must be < original
  ASSERT_LT(compressedSize, buf.size());
  CompressBuffer compressBuffer(compressedSize);
  memcpy(compressBuffer.getCompressed(), compressedBuf->data(), compressedSize);
  compressBuffer.writeHeader(compressedSize);

  const long blockSize = 3;
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_SNAPPY,
      std::unique_ptr<SeekableInputStream>(new SeekableArrayInputStream(
          compressBuffer.getBuffer(),
          compressBuffer.getBufferSize(),
          blockSize)),
      buf.size());
  const void* data;
  int32_t length;
  ASSERT_TRUE(result->Next(&data, &length));
  ASSERT_EQ(N * sizeof(int), length);
  for (int32_t i = 0; i < N; ++i) {
    EXPECT_EQ(i % 8, (reinterpret_cast<const int32_t*>(data))[i]);
  }
}

TEST(TestDecompression, testMultiBuffer) {
  const int32_t N = 1024;
  std::vector<char> buf(N * sizeof(int));
  for (int32_t i = 0; i < N; ++i) {
    (reinterpret_cast<int32_t*>(buf.data()))[i] = i % 8;
  }

  auto ioBuf = folly::IOBuf::wrapBuffer(buf.data(), buf.size());
  auto compressedBuf = getCodec(CodecType::SNAPPY)->compress(ioBuf.get());
  auto compressedSize = compressedBuf->length();
  // compressed size must be < original
  ASSERT_LT(compressedSize, buf.size());
  CompressBuffer compressBuffer(compressedSize);
  memcpy(compressBuffer.getCompressed(), compressedBuf->data(), compressedSize);
  compressBuffer.writeHeader(compressedSize);

  std::vector<char> input(compressBuffer.getBufferSize() * 4);
  ::memcpy(
      input.data(), compressBuffer.getBuffer(), compressBuffer.getBufferSize());
  ::memcpy(
      input.data() + compressBuffer.getBufferSize(),
      compressBuffer.getBuffer(),
      compressBuffer.getBufferSize());
  ::memcpy(
      input.data() + 2 * compressBuffer.getBufferSize(),
      compressBuffer.getBuffer(),
      compressBuffer.getBufferSize());
  ::memcpy(
      input.data() + 3 * compressBuffer.getBufferSize(),
      compressBuffer.getBuffer(),
      compressBuffer.getBufferSize());

  const long blockSize = 3;
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_SNAPPY,
      std::unique_ptr<SeekableInputStream>(
          new SeekableArrayInputStream(input.data(), input.size(), blockSize)),
      buf.size());
  for (int32_t i = 0; i < 4; ++i) {
    const void* data;
    int32_t length;
    ASSERT_TRUE(result->Next(&data, &length));
    for (int32_t j = 0; j < N; ++j) {
      EXPECT_EQ(j % 8, (reinterpret_cast<const int32_t*>(data))[j]);
    }
  }
}

TEST(TestDecompression, testSkipSnappy) {
  const int32_t N = 1024;
  std::vector<char> buf(N * sizeof(int));
  for (int32_t i = 0; i < N; ++i) {
    (reinterpret_cast<int32_t*>(buf.data()))[i] = i % 8;
  }

  auto ioBuf = folly::IOBuf::wrapBuffer(buf.data(), buf.size());
  auto compressedBuf = getCodec(CodecType::SNAPPY)->compress(ioBuf.get());
  auto compressedSize = compressedBuf->length();
  // compressed size must be < original
  ASSERT_LT(compressedSize, buf.size());
  CompressBuffer compressBuffer(compressedSize);
  memcpy(compressBuffer.getCompressed(), compressedBuf->data(), compressedSize);
  compressBuffer.writeHeader(compressedSize);

  const long blockSize = 3;
  std::unique_ptr<SeekableInputStream> result = createTestDecompressor(
      CompressionKind_SNAPPY,
      std::unique_ptr<SeekableInputStream>(new SeekableArrayInputStream(
          compressBuffer.getBuffer(),
          compressBuffer.getBufferSize(),
          blockSize)),
      buf.size());
  const void* data;
  int32_t length;
  // skip 1/2; in 2 jumps
  ASSERT_TRUE(result->Skip(static_cast<int32_t>(((N / 2) - 2) * sizeof(int))));
  ASSERT_TRUE(result->Skip(static_cast<int32_t>(2 * sizeof(int))));
  ASSERT_TRUE(result->Next(&data, &length));
  ASSERT_EQ((N / 2) * sizeof(int), length);
  for (int32_t i = N / 2; i < N; ++i) {
    EXPECT_EQ(i % 8, (reinterpret_cast<const int32_t*>(data))[i - N / 2]);
  }
}

void fillInput(char* buf, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    buf[i] = folly::Random::rand32() % 26 + 'A';
  }
}

void writeHeader(char* buffer, size_t compressedSize, bool original) {
  buffer[0] = static_cast<char>((compressedSize << 1) + (original ? 1 : 0));
  buffer[1] = static_cast<char>(compressedSize >> 7);
  buffer[2] = static_cast<char>(compressedSize >> 15);
}

size_t
compress(char* buf, size_t size, char* output, size_t offset, Codec& codec) {
  auto ioBuf = folly::IOBuf::wrapBuffer(buf, size);
  auto compressed = codec.compress(ioBuf.get());
  auto str = compressed->moveToFbString();
  auto copied = str.copy(output + offset + 3, str.length());
  writeHeader(output + offset, copied, false);
  return copied + offset + 3;
}

class TestSeek : public ::testing::Test {
 public:
  ~TestSeek() override {}
  static void runTest(Codec& codec, CompressionKind kind) {
    constexpr size_t inputSize = 1024;
    constexpr size_t outputSize = 4096;
    char output[outputSize];
    char input1[inputSize];
    char input2[inputSize];
    size_t offset1;
    size_t offset2;
    prepareTestData(codec, input1, input2, inputSize, output, offset1, offset2);
    auto scopedPool = getDefaultScopedMemoryPool();
    auto& pool = scopedPool->getPool();

    std::unique_ptr<SeekableInputStream> stream = createDecompressor(
        kind,
        std::unique_ptr<SeekableInputStream>(
            new SeekableArrayInputStream(output, offset2, outputSize / 10)),
        outputSize,
        pool,
        "TestSeek Decompressor",
        nullptr);

    const void* data;
    int32_t size;
    stream->Next(&data, &size);
    EXPECT_EQ(size, inputSize);
    EXPECT_EQ(0, memcmp(data, input1, inputSize));

    size_t seekPos = folly::Random::rand32() % 1000 + 1;
    size_t arr[][2]{{0, 0}, {0, seekPos}, {offset1, seekPos}, {offset1, 0}};
    char* input[]{input1, input1, input2, input2};

    for (size_t i = 0; i < VELOX_ARRAY_SIZE(arr); ++i) {
      auto pos = arr[i];
      std::vector<uint64_t> list{pos[0], pos[1]};
      PositionProvider pp(list);
      stream->seekToPosition(pp);
      stream->Next(&data, &size);
      EXPECT_EQ(size, inputSize - pos[1]);
      EXPECT_EQ(0, memcmp(data, input[i] + pos[1], size));
    }
  }

  static void prepareTestData(
      Codec& codec,
      char* input1,
      char* input2,
      size_t inputSize,
      char* output,
      size_t& offset1,
      size_t& offset2) {
    fillInput(input1, inputSize);
    fillInput(input2, inputSize);
    offset1 = compress(input1, inputSize, output, 0, codec);
    offset2 = compress(input2, inputSize, output, offset1, codec);
  }
};

TEST_F(TestSeek, Zlib) {
  auto codec = zlib::getCodec(
      zlib::Options(zlib::Options::Format::RAW), COMPRESSION_LEVEL_DEFAULT);
  runTest(*codec, CompressionKind_ZLIB);
}

TEST_F(TestSeek, Zstd) {
  auto codec = getCodec(CodecType::ZSTD);
  runTest(*codec, CompressionKind_ZSTD);
}

TEST_F(TestSeek, Snappy) {
  auto codec = getCodec(CodecType::SNAPPY);
  runTest(*codec, CompressionKind_SNAPPY);
}

TEST_F(TestSeek, uncompressed) {
  constexpr int32_t kSize = 1000;
  constexpr int32_t kHeaderSize = 3;
  constexpr int32_t kReadSize = 100;
  CompressBuffer data(kSize);
  data.writeUncompressedHeader(kSize);
  for (auto i = 0; i < kSize; ++i) {
    data.getCompressed()[i] = static_cast<char>(i);
  }
  auto stream = createTestDecompressor(
      CompressionKind_SNAPPY,
      std::make_unique<SeekableArrayInputStream>(
          data.getBuffer(), kSize + 3, kReadSize),
      5 * kReadSize);
  const void* result;
  int32_t size;

  // We expect to see the data in chunks made by the inner input stream.
  EXPECT_TRUE(stream->Next(&result, &size));
  EXPECT_EQ(result, data.getCompressed());
  EXPECT_EQ(kReadSize - kHeaderSize, size);

  EXPECT_TRUE(stream->Next(&result, &size));
  EXPECT_EQ(result, data.getCompressed() + kReadSize - kHeaderSize);

  // Backup is limited to the last window returned by Next().
  EXPECT_THROW(stream->BackUp(kReadSize + 1), std::exception);

  EXPECT_TRUE(stream->Next(&result, &size));
  EXPECT_EQ(result, data.getCompressed() + 2 * kReadSize - kHeaderSize);

  // We seek to a position that is not in the last window returned by
  // the input of the PagedInputStream but is in the returned bytes of
  // it. Compressed position is start of stream (the first compression
  // header), the byte offset if 50 bytes from there.
  std::vector<uint64_t> offsets{0, 50};
  PositionProvider position(offsets);
  stream->seekToPosition(position);
  EXPECT_TRUE(stream->Next(&result, &size));
  EXPECT_EQ(result, data.getCompressed() + 50);
}

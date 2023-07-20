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
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MmapAllocator.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::memory;

class ByteStreamTest : public testing::Test {
 protected:
  void SetUp() override {
    constexpr uint64_t kMaxMappedMemory = 64 << 20;
    MmapAllocator::Options options;
    options.capacity = kMaxMappedMemory;
    mmapAllocator_ = std::make_shared<MmapAllocator>(options);
    MemoryAllocator::setDefaultInstance(mmapAllocator_.get());
    memoryManager_ = std::make_unique<MemoryManager>(MemoryManagerOptions{
        .capacity = kMaxMemory, .allocator = MemoryAllocator::getInstance()});
    pool_ = memoryManager_->addLeafPool("ByteStreamTest");
  }

  void TearDown() override {
    MmapAllocator::testingDestroyInstance();
    MemoryAllocator::setDefaultInstance(nullptr);
  }

  std::shared_ptr<MmapAllocator> mmapAllocator_;
  std::unique_ptr<MemoryManager> memoryManager_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(ByteStreamTest, outputStream) {
  auto out = std::make_unique<IOBufOutputStream>(*pool_, nullptr, 10000);
  std::stringstream referenceSStream;
  auto reference = std::make_unique<OStreamOutputStream>(&referenceSStream);
  for (auto i = 0; i < 100; ++i) {
    std::string data;
    data.resize(10000);
    std::fill(data.begin(), data.end(), i);
    out->write(data.data(), data.size());
    reference->write(data.data(), data.size());
  }
  EXPECT_EQ(reference->tellp(), out->tellp());
  for (auto i = 0; i < 100; ++i) {
    std::string data;
    data.resize(6000);
    std::fill(data.begin(), data.end(), i + 10);
    out->seekp(i * 10000 + 5000);
    reference->seekp(i * 10000 + 5000);
    out->write(data.data(), data.size());
    reference->write(data.data(), data.size());
  }
  auto str = referenceSStream.str();
  auto numPages = mmapAllocator_->numAllocated();
  EXPECT_LT(0, numPages);
  auto iobuf = out->getIOBuf();
  // We expect no new memory for the IOBufs, they take ownership of the buffers
  // of 'out'.
  EXPECT_EQ(numPages, mmapAllocator_->numAllocated());

  // 'clone' holds a second reference to the data. 'clone' is
  // destructively coalesced, dropping the second reference but the
  // original reference in 'iobuf' keeps the data alive.
  auto clone = iobuf->clone();
  auto out1Data = clone->coalesce();
  EXPECT_EQ(
      str,
      std::string(
          reinterpret_cast<const char*>(out1Data.data()), out1Data.size()));
  out = nullptr;
  // The memory stays allocated since shared ownership in 'iobuf' chain.
  EXPECT_EQ(numPages, mmapAllocator_->numAllocated());

  iobuf = nullptr;
  // We expect dropping the stream and the iobuf frees the backing memory.
  EXPECT_EQ(0, mmapAllocator_->numAllocated());
}

TEST_F(ByteStreamTest, resetInput) {
  uint8_t* const kFakeBuffer = reinterpret_cast<uint8_t*>(this);
  std::vector<ByteRange> byteRanges;
  size_t totalBytes{0};
  size_t lastRangeEnd;
  for (int32_t i = 0; i < 32; ++i) {
    byteRanges.push_back(ByteRange{kFakeBuffer, 4096 + i, 0});
    totalBytes += 4096 + i;
  }
  lastRangeEnd = byteRanges.back().size;
  ByteStream byteStream;
  ASSERT_EQ(byteStream.size(), 0);
  ASSERT_EQ(byteStream.lastRangeEnd(), 0);
  byteStream.resetInput(std::move(byteRanges));
  ASSERT_EQ(byteStream.size(), totalBytes);
  ASSERT_EQ(byteStream.lastRangeEnd(), lastRangeEnd);
}

TEST_F(ByteStreamTest, remainingSize) {
  const int32_t kSize = 100;
  const int32_t kBufferSize = 4096;
  std::vector<void*> buffers;
  std::vector<ByteRange> byteRanges;
  for (int32_t i = 0; i < kSize; i++) {
    buffers.push_back(pool_->allocate(kBufferSize));
    byteRanges.push_back(
        ByteRange{reinterpret_cast<uint8_t*>(buffers.back()), kBufferSize, 0});
  }
  ByteStream byteStream;
  byteStream.resetInput(std::move(byteRanges));
  const int32_t kReadBytes = 2048;
  int32_t remainingSize = kSize * kBufferSize;
  uint8_t* tempBuffer = reinterpret_cast<uint8_t*>(pool_->allocate(kReadBytes));
  while (byteStream.remainingSize() > 0) {
    byteStream.readBytes(tempBuffer, kReadBytes);
    remainingSize -= kReadBytes;
    ASSERT_EQ(remainingSize, byteStream.remainingSize());
  }
  ASSERT_EQ(0, byteStream.remainingSize());
  for (int32_t i = 0; i < kSize; i++) {
    pool_->free(buffers[i], kBufferSize);
  }
  pool_->free(tempBuffer, kReadBytes);
}

TEST_F(ByteStreamTest, toString) {
  const int32_t kSize = 10;
  const int32_t kBufferSize = 4096;
  std::vector<void*> buffers;
  std::vector<ByteRange> byteRanges;
  for (int32_t i = 0; i < kSize; i++) {
    buffers.push_back(pool_->allocate(kBufferSize));
    byteRanges.push_back(
        ByteRange{reinterpret_cast<uint8_t*>(buffers.back()), kBufferSize, 0});
  }
  ByteStream byteStream;
  byteStream.resetInput(std::move(byteRanges));
  const int32_t kReadBytes = 2048;
  uint8_t* tempBuffer = reinterpret_cast<uint8_t*>(pool_->allocate(kReadBytes));
  for (int32_t i = 0; i < kSize / 2; i++) {
    byteStream.readBytes(tempBuffer, kReadBytes);
  }
  std::string byteStreamStr = byteStream.toString();
  EXPECT_EQ(
      byteStreamStr,
      "ByteStream[lastRangeEnd 4096, 10 ranges "
      "(position/size) [(4096/4096),(4096/4096),(2048/4096 current),"
      "(0/4096),(0/4096),(0/4096),(0/4096),(0/4096),(0/4096),(0/4096)]]");
  for (int32_t i = 0; i < kSize; i++) {
    pool_->free(buffers[i], kBufferSize);
  }
  pool_->free(tempBuffer, kReadBytes);
}

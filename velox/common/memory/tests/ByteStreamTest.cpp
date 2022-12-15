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
#include "velox/common/memory/MappedMemory.h"
#include "velox/common/memory/MmapAllocator.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::memory;

class ByteStreamTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    constexpr uint64_t kMaxMappedMemory = 64 << 20;
    MmapAllocatorOptions options;
    options.capacity = kMaxMappedMemory;
    mmapAllocator_ = std::make_shared<MmapAllocator>(options);
    MappedMemory::setDefaultInstance(mmapAllocator_.get());
  }

  void TearDown() override {
    MappedMemory::destroyTestOnly();
    MappedMemory::setDefaultInstance(nullptr);
  }

  std::shared_ptr<MmapAllocator> mmapAllocator_;
};

TEST_F(ByteStreamTest, outputStream) {
  auto out =
      std::make_unique<IOBufOutputStream>(*mmapAllocator_, nullptr, 10000);
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

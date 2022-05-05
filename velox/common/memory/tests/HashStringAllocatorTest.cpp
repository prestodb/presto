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
#include "velox/common/memory/HashStringAllocator.h"

#include <folly/Random.h>

#include <folly/container/F14Map.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

using namespace facebook::velox;

struct Multipart {
  HashStringAllocator::Position start{nullptr, nullptr};
  HashStringAllocator::Position current{nullptr, nullptr};
  uint64_t size = 0;
  std::string reference;
};

class HashStringAllocatorTest : public testing::Test {
 protected:
  void SetUp() override {
    instance_ = std::make_unique<HashStringAllocator>(
        memory::MappedMemory::getInstance());
    rng_.seed(1);
  }

  HashStringAllocator::Header* allocate(int32_t numBytes) {
    auto result = instance_->allocate(numBytes);
    EXPECT_GE(result->size(), numBytes);
    initializeContents(result);
    return result;
  }

  void initializeContents(HashStringAllocator::Header* header) {
    auto sequence = ++sequence_;
    int32_t numWords = header->size() / sizeof(void*);
    void** ptr = reinterpret_cast<void**>(header->begin());
    ptr[0] = reinterpret_cast<void*>(sequence);
    for (int32_t offset = 1; offset < numWords; offset++) {
      ptr[offset] = ptr + offset + sequence;
    }
  }

  void checkMultipart(Multipart& data) {
    std::string storage;
    auto contiguous = HashStringAllocator::contiguousString(
        StringView(data.start.position, data.reference.size()), storage);
    EXPECT_EQ(StringView(data.reference), contiguous);
  }

  void checkAndFree(Multipart& data) {
    checkMultipart(data);
    data.reference.clear();
    instance_->free(data.start.header);
    data.start = {nullptr, nullptr};
  }

  std::string randomString() {
    std::string result;
    result.resize(
        20 +
        (folly::Random::rand32(rng_) % 10 > 8
             ? folly::Random::rand32(rng_) % 200
             : 1000 + folly::Random::rand32(rng_) % 1000));
    for (auto i = 0; i < result.size(); ++i) {
      result[i] = 32 + (folly::Random::rand32(rng_) % 96);
    }
    return result;
  }

  std::unique_ptr<HashStringAllocator> instance_;
  int32_t sequence_ = 0;
  folly::Random::DefaultGenerator rng_;
};

TEST_F(HashStringAllocatorTest, allocate) {
  for (auto count = 0; count < 3; ++count) {
    std::vector<HashStringAllocator::Header*> headers;
    for (auto i = 0; i < 10'000; ++i) {
      headers.push_back(allocate((i % 10) * 10));
    }
    instance_->checkConsistency();
    for (int32_t step = 7; step >= 1; --step) {
      for (auto i = 0; i < headers.size(); i += step) {
        if (headers[i]) {
          instance_->free(headers[i]);
          headers[i] = nullptr;
        }
      }
      instance_->checkConsistency();
    }
  }
  // We allow for some free overhead for free lists after all is freed.
  EXPECT_LE(instance_->retainedSize() - instance_->freeSpace(), 200);
}

TEST_F(HashStringAllocatorTest, multipart) {
  constexpr int32_t kNumSamples = 10'000;
  std::vector<Multipart> data(kNumSamples);
  for (auto count = 0; count < 3; ++count) {
    for (auto i = 0; i < kNumSamples; ++i) {
      if (data[i].start.header && folly::Random::rand32(rng_) % 10 > 7) {
        checkAndFree(data[i]);
        continue;
      }
      auto chars = randomString();
      ByteStream stream(instance_.get(), false, false);
      if (data[i].start.header) {
        if (folly::Random::rand32(rng_) % 5) {
          // 4/5 of cases append to the end.
          instance_->extendWrite(data[i].current, stream);
        } else {
          // 1/5 of cases rewrite from the start.
          instance_->extendWrite(data[i].start, stream);
          data[i].current = data[i].start;
          data[i].reference.clear();
        }
      } else {
        data[i].start = instance_->newWrite(stream, chars.size());
        data[i].current = data[i].start;
        EXPECT_EQ(
            data[i].start.header,
            HashStringAllocator::headerOf(stream.ranges()[0].buffer));
      }
      stream.appendStringPiece(folly::StringPiece(chars.data(), chars.size()));
      auto reserve = folly::Random::rand32(rng_) % 100;
      data[i].current = instance_->finishWrite(stream, reserve);
      data[i].reference.insert(
          data[i].reference.end(), chars.begin(), chars.end());
    }
    instance_->checkConsistency();
  }
  for (auto i = 0; i < data.size(); ++i) {
    if (data[i].start.header) {
      checkMultipart(data[i]);
    }
  }
  for (auto i = 0; i < data.size(); ++i) {
    if (data[i].start.header) {
      checkAndFree(data[i]);
      instance_->checkConsistency();
    }
  }
  instance_->checkConsistency();
}

TEST_F(HashStringAllocatorTest, rewrite) {
  ByteStream stream(instance_.get());
  auto header = instance_->allocate(5);
  EXPECT_EQ(16, header->size()); // Rounds up to kMinAlloc.
  HashStringAllocator::Position current{header, header->begin()};
  for (auto i = 0; i < 10; ++i) {
    instance_->extendWrite(current, stream);
    stream.appendOne(123456789012345LL);
    current = instance_->finishWrite(stream, 0);
    auto offset = HashStringAllocator::offset(header, current);
    EXPECT_EQ((i + 1) * sizeof(int64_t), offset);
    // The allocated writable space from 'header' is at least the amount
    // written.
    auto avail = HashStringAllocator::available({header, header->begin()});
    EXPECT_LE((i + 1) * sizeof(int64_t), avail);
  }
  EXPECT_EQ(-1, HashStringAllocator::offset(header, {nullptr, nullptr}));
  for (auto repeat = 0; repeat < 2; ++repeat) {
    auto position = HashStringAllocator::seek(header, sizeof(int64_t));
    // We write the words at index 1 and 2.
    instance_->extendWrite(position, stream);
    stream.appendOne(12345LL);
    stream.appendOne(67890LL);
    position = instance_->finishWrite(stream, 0);
    EXPECT_EQ(
        3 * sizeof(int64_t), HashStringAllocator::offset(header, position));
    HashStringAllocator::prepareRead(header, stream);
    EXPECT_EQ(123456789012345LL, stream.read<int64_t>());
    EXPECT_EQ(12345LL, stream.read<int64_t>());
    EXPECT_EQ(67890LL, stream.read<int64_t>());
  }
  // The stream contains 3 int64_t's.
  auto end = HashStringAllocator::seek(header, 3 * sizeof(int64_t));
  EXPECT_EQ(0, HashStringAllocator::available(end));
  instance_->ensureAvailable(32, end);
  EXPECT_EQ(32, HashStringAllocator::available(end));
}

TEST_F(HashStringAllocatorTest, stlAllocator) {
  {
    std::vector<double, StlAllocator<double>> data(
        StlAllocator<double>(instance_.get()));
    uint32_t counter{0};
    {
      RowSizeTracker trackSize(counter, *instance_);

      // The contiguous size goes to 80K, rounded to 128K by
      // std::vector. This covers making an extra-large slab in the
      // allocator.
      for (auto i = 0; i < 10'000; i++) {
        data.push_back(i);
      }
    }
    EXPECT_LE(128 * 1024, counter);
    for (auto i = 0; i < 10'000; i++) {
      ASSERT_EQ(i, data[i]);
    }

    data.clear();
    for (auto i = 0; i < 10'000; i++) {
      data.push_back(i);
    }

    for (auto i = 0; i < 10'000; i++) {
      ASSERT_EQ(i, data[i]);
    }

    data.clear();

    // Repeat allocations, now peaking at a largest contiguous block of 256K
    for (auto i = 0; i < 20'000; i++) {
      data.push_back(i);
    }

    for (auto i = 0; i < 20'000; i++) {
      ASSERT_EQ(i, data[i]);
    }
  }

  instance_->checkConsistency();

  // We allow for some overhead for free lists after all is freed.
  EXPECT_LE(instance_->retainedSize() - instance_->freeSpace(), 100);
}

TEST_F(HashStringAllocatorTest, stlAllocatorWithSet) {
  {
    std::unordered_set<
        double,
        std::hash<double>,
        std::equal_to<double>,
        StlAllocator<double>>
        set(StlAllocator<double>(instance_.get()));

    for (auto i = 0; i < 10'000; i++) {
      set.insert(i);
    }
    for (auto i = 0; i < 10'000; i++) {
      ASSERT_EQ(1, set.count(i));
    }

    set.clear();
    for (auto i = 0; i < 10'000; i++) {
      ASSERT_EQ(0, set.count(i));
    }

    for (auto i = 10'000; i < 20'000; i++) {
      set.insert(i);
    }
    for (auto i = 10'000; i < 20'000; i++) {
      ASSERT_EQ(1, set.count(i));
    }
  }

  instance_->checkConsistency();

  // We allow for some overhead for free lists after all is freed.
  EXPECT_LE(instance_->retainedSize() - instance_->freeSpace(), 100);
}

TEST_F(HashStringAllocatorTest, alignedStlAllocatorWithF14Map) {
  {
    folly::F14FastMap<
        int32_t,
        double,
        std::hash<int32_t>,
        std::equal_to<int32_t>,
        AlignedStlAllocator<std::pair<const int32_t, double>, 16>>
        map(AlignedStlAllocator<std::pair<const int32_t, double>, 16>(
            instance_.get()));

    for (auto i = 0; i < 10'000; i++) {
      map.try_emplace(i, i + 0.05);
    }
    for (auto i = 0; i < 10'000; i++) {
      ASSERT_EQ(1, map.count(i));
    }

    map.clear();
    for (auto i = 0; i < 10'000; i++) {
      ASSERT_EQ(0, map.count(i));
    }

    for (auto i = 10'000; i < 20'000; i++) {
      map.try_emplace(i, i + 0.15);
    }
    for (auto i = 10'000; i < 20'000; i++) {
      ASSERT_EQ(1, map.count(i));
    }
  }

  instance_->checkConsistency();

  // We allow for some overhead for free lists after all is freed. Map tends to
  // generate more free blocks at the end, so we loosen the upper bound a bit.
  EXPECT_LE(instance_->retainedSize() - instance_->freeSpace(), 130);
}

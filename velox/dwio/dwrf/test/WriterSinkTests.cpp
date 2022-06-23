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
#include "folly/Random.h"
#include "velox/dwio/dwrf/writer/WriterSink.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::memory;

class WriterSinkTests : public Test {
 protected:
  void SetUp() override {
    for (size_t i = 0; i < data.size(); ++i) {
      data.data()[i] = static_cast<char>(i % 0xff);
    }
  }

  void TearDown() override {}

  void checkOutput(const MemorySink& out, size_t offset) {
    ASSERT_EQ(out.size() - offset, data.size());
    auto actual = out.getData() + offset;
    for (size_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(data.data()[i], actual[i]);
    }
  }

  std::array<char, 1024> data;
};

TEST_F(WriterSinkTests, Checksum) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024 + 3};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::CRC32);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::NA);
  WriterSink sink{out, pool, config};
  auto offset = out.size();

  auto checksum = sink.getChecksum();
  ASSERT_EQ(checksum->getType(), proto::ChecksumAlgorithm::CRC32);

  sink.setMode(WriterSink::Mode::Index);
  sink.addBuffer(pool, data.data(), data.size());
  checkOutput(out, offset);
  ASSERT_EQ(checksum->getDigest(), 1709612422);

  out.reset();
  offset = out.size();
  sink.setMode(WriterSink::Mode::Data);
  sink.addBuffer(pool, data.data(), data.size());
  checkOutput(out, offset);
  ASSERT_EQ(checksum->getDigest(), 1709612422);

  out.reset();
  offset = out.size();
  sink.setMode(WriterSink::Mode::Footer);
  sink.addBuffer(pool, data.data(), data.size());
  checkOutput(out, offset);
  ASSERT_EQ(checksum->getDigest(), 1709612422);

  out.reset();
  offset = out.size();
  sink.setMode(WriterSink::Mode::None);
  sink.addBuffer(pool, data.data(), data.size());
  checkOutput(out, offset);
  ASSERT_EQ(checksum->getDigest(), 0);

  ASSERT_EQ(sink.size(), out.size() - offset);
}

TEST_F(WriterSinkTests, NoChecksum) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024 + 3};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::NA);
  WriterSink sink{out, pool, config};
  auto offset = out.size();

  ASSERT_EQ(sink.getChecksum(), nullptr);
  sink.setMode(WriterSink::Mode::Index);

  sink.addBuffer(pool, data.data(), data.size());
  checkOutput(out, offset);
}

TEST_F(WriterSinkTests, NoCache) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::NA);
  WriterSink sink{out, pool, config};
  auto offset = out.size();

  ASSERT_EQ(sink.getCacheMode(), config.get(Config::STRIPE_CACHE_MODE));
  ASSERT_EQ(sink.getCacheOffsets().size(), 0);
  ASSERT_EQ(sink.getCacheSize(), 0);

  sink.setMode(WriterSink::Mode::Index);
  sink.addBuffer(pool, data.data(), 512);
  sink.setMode(WriterSink::Mode::None);

  ASSERT_EQ(sink.getCacheOffsets().size(), 0);
  ASSERT_EQ(sink.getCacheSize(), 0);

  sink.writeCache();
  ASSERT_EQ(out.size() - offset, 512);
}

TEST_F(WriterSinkTests, CacheIndex) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::INDEX);
  WriterSink sink{out, pool, config};
  auto offset = out.size();

  ASSERT_EQ(sink.getCacheMode(), config.get(Config::STRIPE_CACHE_MODE));
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);
  ASSERT_EQ(sink.getCacheOffsets().at(0), 0);
  ASSERT_EQ(sink.getCacheSize(), 0);

  sink.setMode(WriterSink::Mode::Index);
  sink.addBuffer(pool, data.data(), 128);
  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);

  sink.setMode(WriterSink::Mode::Data);
  ASSERT_EQ(sink.getCacheSize(), 128);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);
  ASSERT_EQ(sink.getCacheOffsets().at(1), sink.getCacheSize());
  sink.addBuffer(pool, data.data() + 1, 128);

  sink.setMode(WriterSink::Mode::Footer);
  ASSERT_EQ(sink.getCacheSize(), 128);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);
  sink.addBuffer(pool, data.data() + 2, 128);

  sink.setMode(WriterSink::Mode::None);
  ASSERT_EQ(sink.getCacheSize(), 128);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);

  ASSERT_EQ(out.size() - offset, 384);
  sink.writeCache();
  ASSERT_EQ(out.size() - offset, 512);
  ASSERT_EQ(
      std::string(out.getData() + offset + 384, 128),
      std::string(out.getData() + offset, 128));
}

TEST_F(WriterSinkTests, CacheFooter) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::FOOTER);
  WriterSink sink{out, pool, config};
  auto offset = out.size();

  ASSERT_EQ(sink.getCacheMode(), config.get(Config::STRIPE_CACHE_MODE));
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);
  ASSERT_EQ(sink.getCacheOffsets().at(0), 0);
  ASSERT_EQ(sink.getCacheSize(), 0);

  sink.setMode(WriterSink::Mode::Index);
  sink.addBuffer(pool, data.data(), 128);

  sink.setMode(WriterSink::Mode::Data);
  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);
  sink.addBuffer(pool, data.data() + 1, 128);

  sink.setMode(WriterSink::Mode::Footer);
  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);
  sink.addBuffer(pool, data.data() + 2, 128);
  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);

  sink.setMode(WriterSink::Mode::None);
  ASSERT_EQ(sink.getCacheSize(), 128);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);
  ASSERT_EQ(sink.getCacheOffsets().at(1), sink.getCacheSize());

  ASSERT_EQ(out.size() - offset, 384);
  sink.writeCache();
  ASSERT_EQ(out.size() - offset, 512);
  ASSERT_EQ(
      std::string(out.getData() + offset + 256, 128),
      std::string(out.getData() + offset + 384, 128));
}

TEST_F(WriterSinkTests, CacheBothEmptyIndex) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::BOTH);
  WriterSink sink{out, pool, config};

  sink.setMode(WriterSink::Mode::Index);
  // don't add any buffer
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);
  ASSERT_EQ(sink.getCacheOffsets().at(0), 0);

  sink.setMode(WriterSink::Mode::Data);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);
  ASSERT_EQ(sink.getCacheOffsets().at(1), 0);
  sink.addBuffer(pool, data.data() + 1, 128);

  sink.setMode(WriterSink::Mode::Footer);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);
  sink.addBuffer(pool, data.data() + 1, 128);
  sink.setMode(WriterSink::Mode::None);

  ASSERT_EQ(sink.getCacheOffsets().size(), 3);
  ASSERT_EQ(sink.getCacheOffsets().at(2), 128);
  ASSERT_EQ(sink.getCacheSize(), 128);
}

TEST_F(WriterSinkTests, CacheBoth) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::BOTH);
  WriterSink sink{out, pool, config};
  auto offset = out.size();

  ASSERT_EQ(sink.getCacheMode(), config.get(Config::STRIPE_CACHE_MODE));
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);
  ASSERT_EQ(sink.getCacheOffsets().at(0), 0);
  ASSERT_EQ(sink.getCacheSize(), 0);

  sink.setMode(WriterSink::Mode::Index);
  sink.addBuffer(pool, data.data(), 128);
  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getCacheOffsets().size(), 1);

  sink.setMode(WriterSink::Mode::Data);
  ASSERT_EQ(sink.getCacheSize(), 128);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);
  ASSERT_EQ(sink.getCacheOffsets().at(1), sink.getCacheSize());
  sink.addBuffer(pool, data.data() + 1, 128);

  sink.setMode(WriterSink::Mode::Footer);
  ASSERT_EQ(sink.getCacheSize(), 128);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);
  sink.addBuffer(pool, data.data() + 2, 128);
  ASSERT_EQ(sink.getCacheSize(), 128);
  ASSERT_EQ(sink.getCacheOffsets().size(), 2);

  sink.setMode(WriterSink::Mode::None);
  ASSERT_EQ(sink.getCacheSize(), 256);
  ASSERT_EQ(sink.getCacheOffsets().size(), 3);
  ASSERT_EQ(sink.getCacheOffsets().at(2), sink.getCacheSize());

  ASSERT_EQ(out.size() - offset, 384);
  sink.writeCache();
  ASSERT_EQ(out.size() - offset, 640);
  ASSERT_EQ(
      std::string(out.getData() + offset, 128),
      std::string(out.getData() + offset + 384, 128));
  ASSERT_EQ(
      std::string(out.getData() + offset + 256, 128),
      std::string(out.getData() + offset + 512, 128));
}

TEST_F(WriterSinkTests, CacheExceedsLimit) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::BOTH);
  config.set(Config::STRIPE_CACHE_SIZE, static_cast<uint32_t>(100));

  {
    WriterSink sink{out, pool, config};
    auto offset = out.size();

    sink.setMode(WriterSink::Mode::Index);
    sink.addBuffer(pool, data.data(), 10);

    sink.setMode(WriterSink::Mode::Footer);
    ASSERT_EQ(sink.getCacheSize(), 10);
    ASSERT_EQ(sink.getCacheOffsets().size(), 2);
    sink.addBuffer(pool, data.data(), 90);

    sink.setMode(WriterSink::Mode::None);
    ASSERT_EQ(sink.getCacheSize(), 100);
    ASSERT_EQ(sink.getCacheOffsets().size(), 3);

    sink.writeCache();
    ASSERT_EQ(out.size() - offset, 200);
  }

  out.reset();

  {
    WriterSink sink{out, pool, config};
    auto offset = out.size();

    sink.setMode(WriterSink::Mode::Index);
    sink.addBuffer(pool, data.data(), 10);

    sink.setMode(WriterSink::Mode::Footer);
    ASSERT_EQ(sink.getCacheSize(), 10);
    ASSERT_EQ(sink.getCacheOffsets().size(), 2);
    sink.addBuffer(pool, data.data(), 91);

    sink.setMode(WriterSink::Mode::None);
    ASSERT_EQ(sink.getCacheSize(), 10);
    ASSERT_EQ(sink.getCacheOffsets().size(), 2);

    sink.setMode(WriterSink::Mode::Index);
    sink.addBuffer(pool, data.data(), 10);

    sink.setMode(WriterSink::Mode::None);
    ASSERT_EQ(sink.getCacheSize(), 10);
    ASSERT_EQ(sink.getCacheOffsets().size(), 2);

    sink.writeCache();
    ASSERT_EQ(out.size() - offset, 121);
  }

  out.reset();
  {
    config.set(Config::STRIPE_CACHE_SIZE, static_cast<uint32_t>(89));
    WriterSink sink{out, pool, config};
    auto offset = out.size();

    sink.setMode(WriterSink::Mode::Index);
    for (int32_t i = 0; i < 9; i++) {
      sink.addBuffer(pool, data.data(), 10);
      sink.setMode(WriterSink::Mode::Index);
      ASSERT_EQ(sink.getCacheOffsets().size(), 1);
    }

    // Cache max size is 89, Total index size is 90 (9 *10).
    // Since it can't fit in the cache size, it should be discarded.
    sink.setMode(WriterSink::Mode::None);
    ASSERT_EQ(sink.getCacheOffsets().size(), 1);

    sink.writeCache();
    ASSERT_EQ(sink.getCacheMode(), StripeCacheMode::BOTH);
    // Only data gets written.
    ASSERT_EQ(out.size() - offset, 90);
  }

  out.reset();
  {
    config.set(Config::STRIPE_CACHE_SIZE, static_cast<uint32_t>(89));
    WriterSink sink{out, pool, config};
    auto offset = out.size();

    sink.setMode(WriterSink::Mode::Index);
    for (int32_t i = 0; i < 6; i++) {
      sink.addBuffer(pool, data.data(), 10);
      sink.setMode(WriterSink::Mode::Index);
      ASSERT_EQ(sink.getCacheOffsets().size(), 1);
    }

    sink.setMode(WriterSink::Mode::Footer);
    ASSERT_EQ(sink.getCacheOffsets().size(), 2);
    ASSERT_EQ(sink.getCacheSize(), 60);

    sink.addBuffer(pool, data.data(), 10);
    sink.setMode(WriterSink::Mode::None);
    ASSERT_EQ(sink.getCacheOffsets().size(), 3);
    ASSERT_EQ(sink.getCacheSize(), 70);

    sink.setMode(WriterSink::Mode::Index);
    for (int32_t i = 0; i < 6; i++) {
      sink.addBuffer(pool, data.data(), 10);
      sink.setMode(WriterSink::Mode::Index);
    }

    sink.setMode(WriterSink::Mode::Footer);
    sink.addBuffer(pool, data.data(), 10);
    sink.setMode(WriterSink::Mode::None);

    // Last index and footer size, does not fit in cache size, so all of them
    // should be discarded.
    ASSERT_EQ(sink.getCacheOffsets().size(), 3);
    ASSERT_EQ(sink.getCacheSize(), 70);

    sink.writeCache();
    ASSERT_EQ(sink.getCacheMode(), StripeCacheMode::BOTH);
    ASSERT_EQ(out.size() - offset, 210);
  }
}

TEST_F(WriterSinkTests, CacheLarge) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 10 * 1024 * 1024 + 3};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::NULL_);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::BOTH);
  WriterSink sink{out, pool, config};
  auto offset = out.size();

  DataBuffer<char> buffer{pool, 2048};
  std::memset(buffer.data(), 'a', 2048);

  sink.setMode(WriterSink::Mode::Index);
  auto total = 0;
  for (size_t i = 0; i < 1024; ++i) {
    auto size = folly::Random::rand32(buffer.size() - 1) + 1;
    total += size;
    sink.addBuffer(pool, buffer.data(), size);
  }
  sink.setMode(WriterSink::Mode::None);
  ASSERT_THAT(sink.getCacheOffsets(), ElementsAre(0, total));
  ASSERT_EQ(out.size() - offset, total);
  sink.writeCache();
  ASSERT_EQ(out.size() - offset, total * 2);
}

TEST_F(WriterSinkTests, SetModeOutOfOrder) {
  auto scopedPool = getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  MemorySink out{pool, 1024};
  Config config;
  config.set(Config::CHECKSUM_ALGORITHM, proto::ChecksumAlgorithm::CRC32);
  config.set(Config::STRIPE_CACHE_MODE, StripeCacheMode::BOTH);
  WriterSink sink{out, pool, config};

  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 0);

  sink.setMode(WriterSink::Mode::None);
  sink.addBuffer(pool, data.data(), 10);
  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 0);

  sink.setMode(WriterSink::Mode::None);
  ASSERT_EQ(sink.getCacheSize(), 0);

  sink.setMode(WriterSink::Mode::Index);
  ASSERT_EQ(sink.getCacheSize(), 0);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 0);

  sink.addBuffer(pool, data.data(), 10);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 1164760902);

  sink.setMode(WriterSink::Mode::Footer);
  ASSERT_EQ(sink.getCacheSize(), 10);

  sink.addBuffer(pool, data.data(), 10);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 936993659);

  sink.setMode(WriterSink::Mode::Data);
  ASSERT_EQ(sink.getCacheSize(), 20);

  sink.addBuffer(pool, data.data(), 10);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 1522393763);

  sink.setMode(WriterSink::Mode::Footer);
  ASSERT_EQ(sink.getCacheSize(), 20);

  sink.addBuffer(pool, data.data(), 10);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 3650468470);

  sink.setMode(WriterSink::Mode::Index);
  ASSERT_EQ(sink.getCacheSize(), 30);

  sink.addBuffer(pool, data.data(), 10);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 977966233);

  sink.setMode(WriterSink::Mode::None);
  ASSERT_EQ(sink.getCacheSize(), 40);

  sink.addBuffer(pool, data.data(), 10);
  ASSERT_EQ(sink.getChecksum()->getDigest(false), 977966233);
}

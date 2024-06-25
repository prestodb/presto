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

#include "velox/dwio/common/Throttler.h"

#include <gtest/gtest.h>

#include "folly/Random.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::dwio::common {
namespace {

class ThrottlerTest : public testing::Test {
 protected:
  static Throttler::Config throttleConfig(uint32_t cacheTTLMs = 3'600 * 1'000) {
    return Throttler::Config(true, 1, 4, 2.0, 10, 40, 4, cacheTTLMs);
  }

  void SetUp() override {
    Throttler::testingReset();
  }
};

TEST_F(ThrottlerTest, config) {
  const auto config = throttleConfig();
  ASSERT_EQ(
      config.toString(),
      "throttleEnabled:true minThrottleBackoffMs:1ms maxThrottleBackoffMs:4ms backoffScaleFactor:2 minLocalThrottledSignals:10 minGlobalThrottledSignals:40 maxCacheEntries:4 cacheTTLMs:1h 0m 0s");
}

TEST_F(ThrottlerTest, signalType) {
  ASSERT_EQ(Throttler::signalTypeName(Throttler::SignalType::kLocal), "Local");
  ASSERT_EQ(
      Throttler::signalTypeName(Throttler::SignalType::kGlobal), "Global");
  ASSERT_EQ(Throttler::signalTypeName(Throttler::SignalType::kNone), "None");
  ASSERT_EQ(
      Throttler::signalTypeName(static_cast<Throttler::SignalType>(100)),
      "Unknown Signal Type: 100");
}

TEST_F(ThrottlerTest, init) {
  ASSERT_EQ(Throttler::instance(), nullptr);
  Throttler::init(throttleConfig());
  auto* instance = Throttler::instance();
  ASSERT_NE(instance, nullptr);
  ASSERT_EQ(instance, Throttler::instance());
  VELOX_ASSERT_THROW(
      Throttler::init(throttleConfig()), "Throttler has already been set");
  ASSERT_EQ(instance, Throttler::instance());
}

TEST_F(ThrottlerTest, throttleDisabled) {
  Throttler::init(Throttler::Config(false));
  const std::string cluster{"throttleDisabled"};
  const std::string directory{"throttleDisabled"};
  auto* instance = Throttler::instance();
  for (int i = 1; i <= 100; ++i) {
    ASSERT_EQ(
        instance->throttleBackoff(
            i % 2 ? Throttler::SignalType::kLocal
                  : Throttler::SignalType::kGlobal,
            cluster,
            directory),
        0);
  }
  const auto& stats = instance->stats();
  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);
}

TEST_F(ThrottlerTest, noThrottlerSignal) {
  Throttler::init(Throttler::Config(true, 100, 200, 2.0, 10, 1'000));
  const std::string cluster{"noThrottlerSignal"};
  const std::string directory{"noThrottlerSignal"};
  auto* instance = Throttler::instance();
  for (int i = 1; i <= 100; ++i) {
    ASSERT_EQ(
        instance->throttleBackoff(
            Throttler::SignalType::kNone, cluster, directory),
        0);
  }
  const auto& stats = instance->stats();
  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);
}

TEST_F(ThrottlerTest, throttle) {
  const uint64_t minThrottleBackoffMs = 1'000;
  const uint64_t maxThrottleBackoffMs = 2'000;
  for (const bool global : {true, false}) {
    SCOPED_TRACE(fmt::format("global {}", global));

    Throttler::testingReset();
    Throttler::init(Throttler::Config(
        true,
        minThrottleBackoffMs,
        maxThrottleBackoffMs,
        2.0,
        global ? 1'0000 : 2,
        global ? 2 : 1'0000));
    auto* instance = Throttler::instance();
    const auto& stats = instance->stats();
    ASSERT_EQ(stats.localThrottled, 0);
    ASSERT_EQ(stats.globalThrottled, 0);
    ASSERT_EQ(stats.backOffDelay.count(), 0);

    const Throttler::SignalType type =
        global ? Throttler::SignalType::kGlobal : Throttler::SignalType::kLocal;
    const std::string cluster{"throttle"};
    const std::string directory{"throttle"};
    ASSERT_EQ(instance->throttleBackoff(type, cluster, directory), 0);
    ASSERT_EQ(instance->throttleBackoff(type, cluster, directory), 0);

    ASSERT_EQ(stats.localThrottled, 0);
    ASSERT_EQ(stats.globalThrottled, 0);
    ASSERT_EQ(stats.backOffDelay.count(), 0);

    uint64_t measuredBackOffMs{0};
    uint64_t firstBackoffMs{0};
    {
      MicrosecondTimer timer(&measuredBackOffMs);
      firstBackoffMs = instance->throttleBackoff(type, cluster, directory);
    }
    ASSERT_LE(firstBackoffMs, maxThrottleBackoffMs);
    ASSERT_GE(firstBackoffMs, minThrottleBackoffMs);
    ASSERT_GE(measuredBackOffMs, firstBackoffMs);

    ASSERT_EQ(stats.localThrottled, global ? 0 : 1);
    ASSERT_EQ(stats.globalThrottled, global ? 1 : 0);
    ASSERT_EQ(stats.backOffDelay.count(), 1);
    ASSERT_EQ(stats.backOffDelay.sum(), firstBackoffMs);

    measuredBackOffMs = 0;
    uint64_t secondBackoffMs{0};
    {
      MicrosecondTimer timer(&measuredBackOffMs);
      secondBackoffMs = instance->throttleBackoff(type, cluster, directory);
    }
    ASSERT_LE(secondBackoffMs, maxThrottleBackoffMs);
    ASSERT_GE(secondBackoffMs, minThrottleBackoffMs);
    ASSERT_GE(measuredBackOffMs, secondBackoffMs);
    ASSERT_LT(firstBackoffMs, secondBackoffMs);

    ASSERT_EQ(stats.localThrottled, global ? 0 : 2);
    ASSERT_EQ(stats.globalThrottled, global ? 2 : 0);
    ASSERT_EQ(stats.backOffDelay.count(), 2);
    ASSERT_EQ(stats.backOffDelay.sum(), firstBackoffMs + secondBackoffMs);
  }
}

TEST_F(ThrottlerTest, expire) {
  const uint64_t minThrottleBackoffMs = 1'00;
  const uint64_t maxThrottleBackoffMs = 2'00;
  for (const bool global : {true, false}) {
    SCOPED_TRACE(fmt::format("global {}", global));
    Throttler::testingReset();
    Throttler::init(Throttler::Config(
        true,
        minThrottleBackoffMs,
        maxThrottleBackoffMs,
        2.0,
        global ? 1'0000 : 2,
        global ? 2 : 1'0000,
        1'000,
        1'000));
    auto* instance = Throttler::instance();
    const auto& stats = instance->stats();
    ASSERT_EQ(stats.localThrottled, 0);
    ASSERT_EQ(stats.globalThrottled, 0);
    ASSERT_EQ(stats.backOffDelay.count(), 0);

    const Throttler::SignalType type =
        global ? Throttler::SignalType::kGlobal : Throttler::SignalType::kLocal;
    const std::string cluster{"expire"};
    const std::string directory{"expire"};
    ASSERT_EQ(instance->throttleBackoff(type, cluster, directory), 0);
    ASSERT_EQ(instance->throttleBackoff(type, cluster, directory), 0);

    ASSERT_EQ(stats.localThrottled, 0);
    ASSERT_EQ(stats.globalThrottled, 0);
    ASSERT_EQ(stats.backOffDelay.count(), 0);

    std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT

    ASSERT_EQ(instance->throttleBackoff(type, cluster, directory), 0);
    ASSERT_EQ(instance->throttleBackoff(type, cluster, directory), 0);

    ASSERT_EQ(stats.localThrottled, 0);
    ASSERT_EQ(stats.globalThrottled, 0);
    ASSERT_EQ(stats.backOffDelay.count(), 0);
  }
}

TEST_F(ThrottlerTest, differentLocals) {
  const uint64_t minThrottleBackoffMs = 1'000;
  const uint64_t maxThrottleBackoffMs = 2'000;
  Throttler::init(Throttler::Config(
      true, minThrottleBackoffMs, maxThrottleBackoffMs, 2.0, 2, 1'0000));
  auto* instance = Throttler::instance();
  const auto& stats = instance->stats();
  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);

  const std::string cluster1{"differentLocals1"};
  const std::string directory1{"differentLocals1"};
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kLocal, cluster1, directory1),
      0);
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kLocal, cluster1, directory1),
      0);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);

  const std::string directory2{"differentLocals2"};
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kLocal, cluster1, directory2),
      0);
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kLocal, cluster1, directory2),
      0);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);

  const auto path1firstBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kLocal, cluster1, directory1);
  ASSERT_GT(path1firstBackoffMs, 0);
  ASSERT_LT(path1firstBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 1);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 1);
  ASSERT_EQ(stats.backOffDelay.sum(), path1firstBackoffMs);

  const auto path2firstBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kLocal, cluster1, directory2);
  ASSERT_GT(path2firstBackoffMs, 0);
  ASSERT_LT(path2firstBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 2);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 2);
  ASSERT_EQ(
      stats.backOffDelay.sum(), path1firstBackoffMs + path2firstBackoffMs);

  const auto path1SecondBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kLocal, cluster1, directory1);
  ASSERT_EQ(path1SecondBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 3);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 3);
  ASSERT_EQ(
      stats.backOffDelay.sum(),
      path1firstBackoffMs + path2firstBackoffMs + path1SecondBackoffMs);

  const auto path2SecondBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kLocal, cluster1, directory2);
  ASSERT_EQ(path2SecondBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 4);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 4);
  ASSERT_EQ(
      stats.backOffDelay.sum(),
      path1firstBackoffMs + path2firstBackoffMs + path1SecondBackoffMs +
          path2SecondBackoffMs);
}

TEST_F(ThrottlerTest, differentGlobals) {
  const uint64_t minThrottleBackoffMs = 1'000;
  const uint64_t maxThrottleBackoffMs = 2'000;
  Throttler::init(Throttler::Config(
      true, minThrottleBackoffMs, maxThrottleBackoffMs, 2.0, 1'0000, 2));
  auto* instance = Throttler::instance();
  const auto& stats = instance->stats();
  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);

  const std::string cluster1{"differentGlobals1"};
  const std::string directory1{"differentGlobals1"};
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kGlobal, cluster1, directory1),
      0);
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kGlobal, cluster1, directory1),
      0);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);

  const std::string cluster2{"differentGlobals2"};
  const std::string directory2{"differentGlobals1"};
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kGlobal, cluster2, directory2),
      0);
  ASSERT_EQ(
      instance->throttleBackoff(
          Throttler::SignalType::kGlobal, cluster2, directory2),
      0);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 0);
  ASSERT_EQ(stats.backOffDelay.count(), 0);

  const auto path1firstBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kGlobal, cluster1, directory1);
  ASSERT_GT(path1firstBackoffMs, 0);
  ASSERT_LT(path1firstBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 1);
  ASSERT_EQ(stats.backOffDelay.count(), 1);
  ASSERT_EQ(stats.backOffDelay.sum(), path1firstBackoffMs);

  const auto path2firstBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kGlobal, cluster2, directory2);
  ASSERT_GT(path2firstBackoffMs, 0);
  ASSERT_LT(path2firstBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 2);
  ASSERT_EQ(stats.backOffDelay.count(), 2);
  ASSERT_EQ(
      stats.backOffDelay.sum(), path1firstBackoffMs + path2firstBackoffMs);

  const auto path1SecondBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kGlobal, cluster1, directory1);
  ASSERT_EQ(path1SecondBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 3);
  ASSERT_EQ(stats.backOffDelay.count(), 3);
  ASSERT_EQ(
      stats.backOffDelay.sum(),
      path1firstBackoffMs + path2firstBackoffMs + path1SecondBackoffMs);

  const auto path2SecondBackoffMs = instance->throttleBackoff(
      Throttler::SignalType::kGlobal, cluster2, directory2);
  ASSERT_EQ(path2SecondBackoffMs, maxThrottleBackoffMs);

  ASSERT_EQ(stats.localThrottled, 0);
  ASSERT_EQ(stats.globalThrottled, 4);
  ASSERT_EQ(stats.backOffDelay.count(), 4);
  ASSERT_EQ(
      stats.backOffDelay.sum(),
      path1firstBackoffMs + path2firstBackoffMs + path1SecondBackoffMs +
          path2SecondBackoffMs);
}

TEST_F(ThrottlerTest, maxOfGlobalAndLocal) {
  const uint64_t minThrottleBackoffMs = 1'000;
  const uint64_t maxThrottleBackoffMs = 2'000;
  for (const bool localFirst : {false, true}) {
    SCOPED_TRACE(fmt::format("localFirst: {}", localFirst));
    Throttler::testingReset();
    Throttler::init(Throttler::Config(
        true, minThrottleBackoffMs, maxThrottleBackoffMs, 2.0, 2, 2));
    auto* instance = Throttler::instance();
    const auto& stats = instance->stats();
    ASSERT_EQ(stats.localThrottled, 0);
    ASSERT_EQ(stats.globalThrottled, 0);
    ASSERT_EQ(stats.backOffDelay.count(), 0);

    const std::string cluster1{"maxOfGlobalAndLocal1"};
    const std::string directory1{"maxOfGlobalAndLocal1"};
    ASSERT_EQ(
        instance->throttleBackoff(
            localFirst ? Throttler::SignalType::kLocal
                       : Throttler::SignalType::kGlobal,
            cluster1,
            directory1),
        0);
    ASSERT_EQ(
        instance->throttleBackoff(
            localFirst ? Throttler::SignalType::kLocal
                       : Throttler::SignalType::kGlobal,
            cluster1,
            directory1),
        0);

    ASSERT_EQ(stats.localThrottled, 0);
    ASSERT_EQ(stats.globalThrottled, 0);
    ASSERT_EQ(stats.backOffDelay.count(), 0);

    auto backoffMs = instance->throttleBackoff(
        localFirst ? Throttler::SignalType::kLocal
                   : Throttler::SignalType::kGlobal,
        cluster1,
        directory1);
    ASSERT_GT(backoffMs, 0);
    ASSERT_LT(backoffMs, maxThrottleBackoffMs);

    backoffMs = instance->throttleBackoff(
        localFirst ? Throttler::SignalType::kGlobal
                   : Throttler::SignalType::kLocal,
        cluster1,
        directory1);
    ASSERT_GT(backoffMs, 0);
    ASSERT_LT(backoffMs, maxThrottleBackoffMs);

    backoffMs = instance->throttleBackoff(
        localFirst ? Throttler::SignalType::kGlobal
                   : Throttler::SignalType::kLocal,
        cluster1,
        directory1);
    ASSERT_GT(backoffMs, 0);
    ASSERT_LT(backoffMs, maxThrottleBackoffMs);

    backoffMs = instance->throttleBackoff(
        localFirst ? Throttler::SignalType::kLocal
                   : Throttler::SignalType::kGlobal,
        cluster1,
        directory1);
    ASSERT_EQ(backoffMs, maxThrottleBackoffMs);

    const std::string cluster2{"maxOfGlobalAndLocal2"};
    const std::string directory2{"maxOfGlobalAndLocal1"};
    ASSERT_EQ(
        instance->throttleBackoff(
            localFirst ? Throttler::SignalType::kGlobal
                       : Throttler::SignalType::kLocal,
            cluster2,
            directory2),
        0);
    ASSERT_EQ(
        instance->throttleBackoff(
            localFirst ? Throttler::SignalType::kGlobal
                       : Throttler::SignalType::kLocal,
            cluster2,
            directory2),
        0);

    backoffMs = instance->throttleBackoff(
        localFirst ? Throttler::SignalType::kGlobal
                   : Throttler::SignalType::kLocal,
        cluster2,
        directory2);
    ASSERT_GT(backoffMs, 0);
    ASSERT_LT(backoffMs, maxThrottleBackoffMs);

    const std::string directory3{"maxOfGlobalAndLocal3"};
    backoffMs = instance->throttleBackoff(
        Throttler::SignalType::kGlobal, cluster1, directory3);
    if (localFirst) {
      ASSERT_GT(backoffMs, 0);
      ASSERT_LT(backoffMs, maxThrottleBackoffMs);
    } else {
      ASSERT_EQ(backoffMs, maxThrottleBackoffMs);
    }

    if (localFirst) {
      ASSERT_EQ(stats.localThrottled, 2);
      ASSERT_EQ(stats.globalThrottled, 4);
    } else {
      ASSERT_EQ(stats.localThrottled, 3);
      ASSERT_EQ(stats.globalThrottled, 3);
    }
    ASSERT_EQ(stats.backOffDelay.count(), 6);
  }
}

TEST_F(ThrottlerTest, fuzz) {
  const uint64_t minThrottleBackoffMs = 1;
  const uint64_t maxThrottleBackoffMs = 8;
  const double backoffScaleFactor = 2.0;
  const uint32_t minLocalThrottledSignals = 10;
  const uint32_t minGlobalThrottledSignals = 20;
  const uint32_t maxCacheEntries = 64;
  const uint32_t cacheTTLMs = 10;
  Throttler::testingReset();
  Throttler::init(Throttler::Config(
      true,
      minThrottleBackoffMs,
      maxThrottleBackoffMs,
      backoffScaleFactor,
      minLocalThrottledSignals,
      minGlobalThrottledSignals,
      maxCacheEntries,
      cacheTTLMs));
  auto* instance = Throttler::instance();

  const auto seed = getCurrentTimeMs();
  LOG(INFO) << "Random seed: " << getCurrentTimeMs();

  const int numDirectories = 4096;
  std::vector<std::string> directories;
  directories.reserve(numDirectories);
  for (int i = 0; i < numDirectories; ++i) {
    directories.emplace_back(fmt::format("fuzz-{}", i));
  }
  const int numClusters = 128;
  std::vector<std::string> clusters;
  clusters.reserve(numClusters);
  for (int i = 0; i < numClusters; ++i) {
    clusters.emplace_back(fmt::format("fuzz-{}", i));
  }

  std::atomic_bool stopped{false};

  const int numThreads = 64;
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back([&]() {
      folly::Random::DefaultGenerator rng(seed);
      while (!stopped) {
        const Throttler::SignalType type = folly::Random::oneIn(3)
            ? Throttler::SignalType::kGlobal
            : Throttler::SignalType::kLocal;
        const int directoryIndex = folly::Random::rand32(rng) % numDirectories;
        const int clusterIndex = directoryIndex % numClusters;
        instance->throttleBackoff(
            type, clusters[clusterIndex], directories[directoryIndex]);
      }
    });
  }

  // Test for 5 seconds.
  std::this_thread::sleep_for(std::chrono::seconds(5)); // NOLINT
  stopped = true;
  for (auto& thread : threads) {
    thread.join();
  }
}

} // namespace
} // namespace facebook::velox::dwio::common

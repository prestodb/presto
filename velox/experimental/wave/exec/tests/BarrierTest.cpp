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
#include <cuda_runtime.h> // @manual
#include "velox/common/base/tests/GTestUtils.h"

#include "velox/exec/ExchangeSource.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/WaveHiveDataSource.h"
#include "velox/experimental/wave/exec/tests/utils/FileFormat.h"
#include "velox/experimental/wave/exec/tests/utils/WaveTestSplitReader.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_int32(wave_max_reader_batch_rows);
DECLARE_int32(max_streams_per_driver);
DECLARE_int32(wave_reader_rows_per_tb);

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::wave;

class BarrierTest : public testing::Test {
 protected:
  void testFunc(int32_t threadIdx) {
    auto barrierIdx = threadIdx / 10;
    auto name = fmt::format("bar{}", barrierIdx);
    auto barrier = WaveBarrier::get(name, 0, 0);
    message(threadIdx, "enter");
    barrier->enter();
    for (auto action = 0; action < 100; ++action) {
      // std::this_thread::sleep_for(std::chrono::milliseconds(1)); // NOLINT
      if (action % 10 == 0 && threadIdx % 10 < 5) {
        void* reason = reinterpret_cast<void*>(action + 1);
        message(threadIdx, "acq");
        barrier->acquire(nullptr, reason, nullptr);
        message(threadIdx, "exc");
        // std::this_thread::sleep_for(std::chrono::milliseconds(10)); // NOLINT
        ++numAcquired_;
        message(threadIdx, "rel");
        barrier->release();
      } else if (action % 11 == 0) {
        message(threadIdx, "leave-middle");
        barrier->leave();
        message(threadIdx, "enter-middle");
        barrier->enter();
      } else {
        message(threadIdx, "mayYield");
        barrier->mayYield(nullptr, nullptr);
        message(threadIdx, "cont");
      }
    }
    message(threadIdx, "leave");
    barrier->leave();
  }

  void message(int threadIdx, const char* s) {
    if (!verbose_) {
      return;
    }
    std::lock_guard<std::mutex> l(mtx_);
    std::cout << s << " " << threadIdx << std::endl;
  }

  bool verbose_{false};
  std::atomic<int32_t> numAcquired_{0};
  std::mutex mtx_;
};

TEST_F(BarrierTest, basic) {
  constexpr int32_t kNumThreads = 100;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int32_t threadIndex = 0; threadIndex < kNumThreads; ++threadIndex) {
    threads.push_back(
        std::thread([this, threadIndex]() { testFunc(threadIndex); }));
  }
  for (auto& thr : threads) {
    thr.join();
  }
  EXPECT_EQ(5 * kNumThreads, numAcquired_);
}

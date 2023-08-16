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

#include <iostream>

#include <folly/init/Init.h>

#include <gtest/gtest.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Semaphore.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/time/Timer.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/common/tests/CudaTest.h"

DEFINE_int32(num_streams, 0, "Number of paralll streams");
DEFINE_int32(op_size, 0, "Size of invoke kernel (ints read and written)");
DEFINE_int32(
    num_ops,
    0,
    "Number of consecutive kernel executions on each stream");
DEFINE_bool(
    use_callbacks,
    false,
    "Queue a host callback after each kernel execution");
DEFINE_bool(
    sync_streams,
    false,
    "Use events to synchronize all parallel streams before calling the next kernel on each stream.");
DEFINE_bool(
    prefetch,
    true,
    "Use prefetch to move unified memory to device at start and to host at end");

using namespace facebook::velox;
using namespace facebook::velox::wave;

class CudaTest : public testing::Test {
 protected:
  void SetUp() override {
    device_ = getDevice();
    setDevice(device_);
    allocator_ = getAllocator(device_);
  }

  void streamTest(
      int32_t numStreams,
      int32_t numOps,
      int32_t opSize,
      bool prefetch,
      bool useCallbacks,
      bool syncStreams) {
    int32_t firstNotify = useCallbacks ? 1 : numOps - 1;
    constexpr int32_t kBatch = xsimd::batch<int32_t>::size;
    std::vector<std::unique_ptr<TestStream>> streams;
    std::vector<std::unique_ptr<Event>> events;
    std::vector<int32_t*> ints;
    std::mutex mutex;
    int32_t initValues[16] = {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    auto initVector = xsimd::load_unaligned(&initValues[0]);
    auto increment = xsimd::broadcast<int32_t>(1);
    std::vector<int64_t> delay;
    delay.reserve(numStreams * (numOps + 2));

    auto start = getCurrentTimeMicro();
    for (auto i = 0; i < numStreams; ++i) {
      streams.push_back(std::make_unique<TestStream>());
      ints.push_back(reinterpret_cast<int32_t*>(
          allocator_->allocate(opSize * sizeof(int32_t))));
      auto last = ints.back();
      auto data = initVector;
      for (auto i = 0; i < opSize; i += kBatch) {
        data.store_unaligned(last + i);
        data += increment;
      }
    }
    for (auto i = 0; i < numStreams; ++i) {
      streams[i]->addCallback([&]() {
        auto d = getCurrentTimeMicro() - start;
        {
          std::lock_guard<std::mutex> l(mutex);
          delay.push_back(d);
        }
      });
      if (prefetch) {
        streams[i]->prefetch(device_, ints[i], opSize * sizeof(int32_t));
      }
    }

    Semaphore sem(0);
    for (auto counter = 0; counter < numOps; ++counter) {
      if (counter > 0 && syncStreams) {
        waitEach(streams, events);
      }
      for (auto i = 0; i < numStreams; ++i) {
        streams[i]->addOne(ints[i], opSize);
        if (counter == 0 || counter >= firstNotify) {
          streams[i]->addCallback([&]() {
            auto d = getCurrentTimeMicro() - start;
            {
              std::lock_guard<std::mutex> l(mutex);
              delay.push_back(d);
            }
            sem.release();
          });
        }
        if (counter == numOps - 1) {
          if (prefetch) {
            streams[i]->prefetch(nullptr, ints[i], opSize * sizeof(int32_t));
          }
        }
      }
      if (syncStreams && counter < numOps - 1) {
        recordEach(streams, events);
      }
    }
    // Destroy the streams while items pending. Items should finish.
    streams.clear();
    for (auto i = 0; i < numStreams * (numOps + 1 - firstNotify); ++i) {
      sem.acquire();
    }
    for (auto i = 0; i < numStreams; ++i) {
      auto* array = ints[i];
      auto data = initVector + numOps;
      xsimd::batch_bool<int32_t> error;
      error = error ^ error;
      for (auto j = 0; j < opSize; j += kBatch) {
        error = error | (data != xsimd::load_unaligned(array + j));
        data += increment;
      }
      ASSERT_EQ(0, simd::toBitMask(error));
      delay.push_back(getCurrentTimeMicro() - start);
    }
    for (auto i = 0; i < numStreams; ++i) {
      allocator_->free(ints[i], sizeof(int32_t) * opSize);
    }
    std::cout << "Delays: ";
    int32_t counter = 0;
    for (auto d : delay) {
      std::cout << d << " ";
      if (++counter % numStreams == 0) {
        std::cout << std::endl;
      }
    }
    std::cout << std::endl;
    float toDeviceMicros = delay[(2 * numStreams) - 1] - delay[0];
    float inDeviceMicros =
        delay[delay.size() - numStreams - 1] - delay[numStreams * 2 - 1];
    float toHostMicros = delay.back() - delay[delay.size() - numStreams];
    float gbSize =
        (sizeof(int32_t) * numStreams * static_cast<float>(opSize)) / (1 << 30);
    std::cout << "to device= " << toDeviceMicros << "us ("
              << gbSize / (toDeviceMicros / 1000000) << " GB/s)" << std::endl;
    std::cout << "In device (ex. first pass): " << inDeviceMicros << "us ("
              << gbSize * (numOps - 1) / (inDeviceMicros / 1000000) << " GB/s)"
              << std::endl;
    std::cout << "to host= " << toHostMicros << "us ("
              << gbSize / (toHostMicros / 1000000) << " GB/s)" << std::endl;
  }

  void recordEach(
      std::vector<std::unique_ptr<TestStream>>& streams,
      std::vector<std::unique_ptr<Event>>& events) {
    for (auto& stream : streams) {
      events.push_back(std::make_unique<Event>());
      events.back()->record(*stream);
    }
  }

  // Every stream waits for every event recorded on each stream in the previous
  // call to recordEach.
  void waitEach(
      std::vector<std::unique_ptr<TestStream>>& streams,
      std::vector<std::unique_ptr<Event>>& events) {
    auto firstEvent = events.size() - streams.size();
    for (auto& stream : streams) {
      for (auto eventIndex = firstEvent; eventIndex < events.size();
           ++eventIndex) {
        events[eventIndex]->wait(*stream);
      }
    }
  }
  Device* device_;
  GpuAllocator* allocator_;
};

TEST_F(CudaTest, stream) {
  constexpr int32_t opSize = 1000000;
  TestStream stream;
  auto ints = reinterpret_cast<int32_t*>(
      allocator_->allocate(opSize * sizeof(int32_t)));
  for (auto i = 0; i < opSize; ++i) {
    ints[i] = i;
  }
  stream.prefetch(device_, ints, opSize * sizeof(int32_t));
  stream.addOne(ints, opSize);
  stream.prefetch(nullptr, ints, opSize * sizeof(int32_t));
  stream.wait();
  for (auto i = 0; i < opSize; ++i) {
    ASSERT_EQ(ints[i], i + 1);
  }
  allocator_->free(ints, sizeof(int32_t) * opSize);
}

TEST_F(CudaTest, callback) {
  streamTest(10, 10, 1024 * 1024, true, false, false);
}

TEST_F(CudaTest, custom) {
  if (FLAGS_num_streams == 0) {
    return;
  }
  streamTest(
      FLAGS_num_streams,
      FLAGS_num_ops,
      FLAGS_op_size,
      FLAGS_prefetch,
      FLAGS_use_callbacks,
      FLAGS_sync_streams);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest();
  folly::init(&argc, &argv);
  return RUN_ALL_TESTS();
}

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

#include "velox/common/base/Semaphore.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/time/Timer.h"

#include <folly/Benchmark.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <iostream>

DEFINE_int64(repeats, 100, "Number of repetitions");
DEFINE_int64(bytes, 16 * 1024 * 1024, "Bytes to copy in one repetition");
DEFINE_int64(threads, 8, "Number of threads to use within one copy");
DEFINE_bool(system_memcpy, true, "Use libc memcpy");

using namespace facebook::velox;

uint64_t sum(uint64_t* data, int32_t size) {
  uint64_t sum = 0;
  for (auto i = 0; i < size; ++i) {
    sum += data[i];
  }
  return sum;
}

struct CopyCallable {
  void* FOLLY_NULLABLE source;
  void* FOLLY_NULLABLE destination;
  int64_t size;
  Semaphore* FOLLY_NULLABLE sem;

  void operator()() {
    if (FLAGS_system_memcpy) {
      memcpy(destination, source, size);
    } else {
      simd::memcpy(destination, source, size);
    }
    sem->release();
  }
};

int main(int argc, char** argv) {
  constexpr int32_t kAlignment = folly::hardware_destructive_interference_size;
  folly::Init init{&argc, &argv};
  auto chunk = bits::roundUp(
      std::max<int64_t>(FLAGS_bytes / FLAGS_threads, kAlignment), kAlignment);
  int64_t bytes = chunk * FLAGS_threads;
  auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(FLAGS_threads);
  void* other = aligned_alloc(kAlignment, bytes);
  void* source = aligned_alloc(kAlignment, bytes);
  void* destination = aligned_alloc(kAlignment, bytes);
  // Write all memory once outside of timed section to make them resident.
  memset(other, 1, bytes);
  memset(source, 1, bytes);
  memset(destination, 1, bytes);

  Semaphore sem(0);
  std::vector<CopyCallable> ops;
  ops.resize(FLAGS_threads);
  volatile uint64_t totalSum = 0;
  uint64_t totalUsec = 0;
  for (auto repeat = 0; repeat < FLAGS_repeats; ++repeat) {
    // Read once through 'other' to clear cache effects.
    folly::doNotOptimizeAway(
        totalSum +=
        sum(reinterpret_cast<uint64_t*>(other), bytes / sizeof(int64_t)));
    uint64_t usec = 0;
    {
      MicrosecondTimer timer(&usec);
      for (auto i = 0; i < FLAGS_threads; ++i) {
        int64_t offset1 = chunk * i;
        ops[i].source = reinterpret_cast<char*>(source) + offset1;
        ops[i].destination = reinterpret_cast<char*>(destination) + offset1;
        ops[i].size = chunk;
        ops[i].sem = &sem;
        executor->add(ops[i]);
      }
      for (auto i = 0; i < FLAGS_threads; ++i) {
        sem.acquire();
      }
    }
    totalUsec += usec;
  }
  std::cout << fmt::format(
                   "{} repeats {} bytes {} threads: {} usec {} GB/s",
                   FLAGS_repeats,
                   bytes,
                   FLAGS_threads,
                   totalUsec,
                   bytes * FLAGS_repeats / static_cast<float>(1 << 30) /
                       (static_cast<float>(totalUsec + 1) / 1000000.0))
            << std::endl;
  free(source);
  free(destination);
  free(other);
  return 0;
}

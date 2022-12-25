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

#include <deque>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/common/memory/Memory.h"

DEFINE_int64(memory_allocation_count, 10'000, "The number of allocations");
DEFINE_int64(
    memory_allocation_bytes,
    1'000'000'000,
    "The cap of memory allocation bytes");
DEFINE_int64(
    allocation_size_seed,
    99887766,
    "Seed for random memory size generator");
DEFINE_int64(
    memory_free_every_n_operations,
    5,
    "Specifies memory free for every N operations. If it is 5, then we free one of existing memory allocation for every 5 memory operations");

using namespace facebook::velox;
using namespace facebook::velox::memory;

namespace {

enum class Type {
  kStd = 0,
  kMmap = 1,
};

template <uint16_t ALIGNMENT>
class MemoryPoolAllocationBenchMark {
 public:
  MemoryPoolAllocationBenchMark(Type type, size_t minSize, size_t maxSize)
      : type_(type), minSize_(minSize), maxSize_(maxSize) {
    switch (type_) {
      case Type::kMmap:
        manager_ = std::make_shared<MemoryManager<ALIGNMENT>>();
        break;
      case Type::kStd:
        manager_ = std::make_shared<MemoryManager<ALIGNMENT>>();
        break;
      default:
        VELOX_USER_FAIL("Unknown allocator type: {}", static_cast<int>(type_));
        break;
    }
    rng_.seed(FLAGS_allocation_size_seed);
    pool_ = manager_->getChild();
  }

  ~MemoryPoolAllocationBenchMark() {
    while (!empty()) {
      free();
    }
  }

  size_t runAllocate();

  size_t runAllocateZeroFilled();

  size_t runReallocate();

 private:
  struct Allocation {
    void* ptr;
    size_t size;

    Allocation(void* _ptr, size_t _size) : ptr(_ptr), size(_size) {}
  };

  void allocate() {
    const size_t size = allocSize();
    allocations_.emplace_back(pool_->allocate(size), size);
    ++numAllocs_;
    sumAllocBytes_ += size;
  }

  void allocateZeroFilled() {
    const size_t size = allocSize();
    const size_t numEntries = 1 + folly::Random::rand32(size, rng_);
    const size_t sizeEach = size / numEntries;
    allocations_.emplace_back(
        pool_->allocateZeroFilled(numEntries, sizeEach), numEntries * sizeEach);
    ++numAllocs_;
    sumAllocBytes_ += numEntries * sizeEach;
  }

  void reallocate() {
    const size_t oldSize = allocSize();
    void* oldPtr = pool_->allocate(oldSize);
    const size_t newSize = allocSize();
    void* newPtr = pool_->reallocate(oldPtr, oldSize, newSize);
    allocations_.emplace_back(newPtr, newSize);
    ++numAllocs_;
    sumAllocBytes_ += newSize;
  }

  void free() {
    Allocation allocation = allocations_.front();
    allocations_.pop_front();
    pool_->free(allocation.ptr, allocation.size);
    sumAllocBytes_ -= allocation.size;
  }

  bool full() const {
    return sumAllocBytes_ >= FLAGS_memory_allocation_bytes;
  }

  bool empty() const {
    return allocations_.empty();
  }

  size_t allocSize() {
    return minSize_ + folly::Random::rand32(maxSize_ - minSize_ + 1, rng_);
  }

  const Type type_;
  const size_t minSize_;
  const size_t maxSize_;
  folly::Random::DefaultGenerator rng_;
  std::shared_ptr<IMemoryManager> manager_;
  std::shared_ptr<MemoryPool> pool_;
  uint64_t sumAllocBytes_{0};
  uint64_t numAllocs_{0};
  std::deque<Allocation> allocations_;
};

template <uint16_t ALIGNMENT>
size_t MemoryPoolAllocationBenchMark<ALIGNMENT>::runAllocate() {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_memory_allocation_count; ++iter) {
    if (iter % FLAGS_memory_free_every_n_operations == 0 && !empty()) {
      free();
    }
    while (full()) {
      free();
    }
    allocate();
  }
  return FLAGS_memory_allocation_count;
}

template <uint16_t ALIGNMENT>
size_t MemoryPoolAllocationBenchMark<ALIGNMENT>::runAllocateZeroFilled() {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_memory_allocation_count; ++iter) {
    if (iter % FLAGS_memory_free_every_n_operations == 0 && !empty()) {
      free();
    }
    while (full()) {
      free();
    }
    allocateZeroFilled();
  }
  return FLAGS_memory_allocation_count;
}

template <uint16_t ALIGNMENT>
size_t MemoryPoolAllocationBenchMark<ALIGNMENT>::runReallocate() {
  folly::BenchmarkSuspender suspender;
  suspender.dismiss();
  for (auto iter = 0; iter < FLAGS_memory_allocation_count; ++iter) {
    if (iter % FLAGS_memory_free_every_n_operations == 0 && !empty()) {
      free();
    }
    while (full()) {
      free();
    }
    reallocate();
  }
  return FLAGS_memory_allocation_count;
}

// allocateBytes API.
BENCHMARK_MULTI(StdAllocateSmallNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 128, 3072);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateSmallNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 128, 3072);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateSmall64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 128, 3072);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateSmall64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 128, 3072);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateMidNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 4 << 10, 1 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateMidNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 4 << 10, 1 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateMid64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 4 << 10, 1 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateMid64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 4 << 10, 1 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateLargeNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 1 << 20, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateLargeNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 1 << 20, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateLarge64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 1 << 20, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateLarge64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 1 << 20, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateMixNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 128, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateMixNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 128, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateMix64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 128, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateMix64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 128, 32 << 20);
  return benchmark.runAllocate();
}

// allocateZeroFilled API.
BENCHMARK_MULTI(StdAllocateZeroFilledSmallNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 128, 3072);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledSmallNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 128, 3072);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_MULTI(StdAllocateZeroFilledSmall64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 128, 3072);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledSmall64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 128, 3072);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_MULTI(StdAllocateZeroFilledMidNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 4 << 10, 1 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledMidNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 4 << 10, 1 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_MULTI(StdAllocateZeroFilledMid64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 4 << 10, 1 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledMid64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 4 << 10, 1 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_MULTI(StdAllocateZeroFilledLargeNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 1 << 20, 32 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledLargeNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 1 << 20, 32 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_MULTI(StdAllocateZeroFilledLarge64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 1 << 20, 32 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledLarge64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 1 << 20, 32 << 20);
  return benchmark.runAllocateZeroFilled();
}

BENCHMARK_MULTI(StdAllocateZeroFilledMixNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 128, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledMixNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 128, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdAllocateZeroFilledMix64) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 128, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_RELATIVE_MULTI(MmapAllocateZeroFilledMix64) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 128, 32 << 20);
  return benchmark.runAllocate();
}

BENCHMARK_MULTI(StdReallocateSmallNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 128, 3072);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateSmallNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 128, 3072);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateSmall64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 128, 3072);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateSmall64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 128, 3072);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateMidNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 4 << 10, 1 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateMidNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 4 << 10, 1 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateMid64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 4 << 10, 1 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateMid64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 4 << 10, 1 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateLargeNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 1 << 20, 32 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateLargeNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 1 << 20, 32 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateLarge64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 1 << 20, 32 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateLarge64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 1 << 20, 32 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateMixNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kStd, 128, 32 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateMixNoAlignment) {
  MemoryPoolAllocationBenchMark<MemoryAllocator::kMinAlignment> benchmark(
      Type::kMmap, 128, 32 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_MULTI(StdReallocateMix64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kStd, 128, 32 << 20);
  return benchmark.runReallocate();
}

BENCHMARK_RELATIVE_MULTI(MmapReallocateMix64) {
  MemoryPoolAllocationBenchMark<64> benchmark(Type::kMmap, 128, 32 << 20);
  return benchmark.runReallocate();
}
} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  // TODO: add to run benchmark as a standalone program with multithreading as
  // well as actual memory access to trigger minor page faults in OS which traps
  // into kernel context to setup physical pages for the lazy-mapped virtual
  // process memory space.
  folly::runBenchmarks();
  return 0;
}

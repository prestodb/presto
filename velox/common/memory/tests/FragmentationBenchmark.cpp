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
#include <iostream>
#include <map>
#include <vector>

#include <folly/CPortability.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "folly/Random.h"

#include "velox/common/memory/MmapAllocator.h"

DEFINE_int64(volume_gb, 2048, "Total GB to allocate during test");
DEFINE_int64(size_cap_gb, 24, "Size cap: total GB resident at one time");
DEFINE_bool(use_mmap, true, "Use mmap and madvise to manage fragmentation");

using namespace facebook::velox;
using namespace facebook::velox::memory;

struct Block {
  ~Block() {
    if (data) {
      free(data);
    }
  }

  size_t size = 0;
  char* data = nullptr;
  std::shared_ptr<MappedMemory::Allocation> allocation;
  MmapAllocator::ContiguousAllocation contiguous;
};

class FragmentationTest {
 public:
  void SetUp() {
    rng_.seed(1);
    // 2G between 4K an 100K
    // 2G exactly 128K
    addSizes(2L << 30, 128 << 10, 128 << 10);
    // 2G between 1MB and 8MB
    addSizes(2L << 30, 1 << 20, 8 << 20);
    // 2G between 100MB and 400MB
    addSizes(2L << 30, 1L << 12, 100 << 12);
    // 1.9G of 128M, 256M, 512M and 1024M
    addSizes(1, 128 << 20, 128 << 20);
    addSizes(1, 128 << 20, 128 << 20);
    addSizes(1, 256 << 20, 256 << 20);
    addSizes(1, 512 << 20, 512 << 20);
    addSizes(1, 1024 << 20, 1024 << 20);
    buckets_.push_back(0);
    for (size_t bucket = 1 << 16; bucket <= 1L << 30; bucket *= 2) {
      buckets_.push_back(bucket);
    }
  }

  void addSizes(size_t totalBytes, size_t smallest, size_t largest) {
    size_t total = 0;
    while (total < totalBytes) {
      size_t size = bits::roundUp(
          smallest +
              (smallest == largest
                   ? 0
                   : (folly::Random::rand32(rng_) % (largest - smallest))),
          4096);
      sizes_.push_back(size);
      total += size;
    }
  }

  void allocate(size_t size) {
    auto block = std::make_unique<Block>();
    block->size = size;
    if (memory_) {
      if (size <= 8 << 20) {
        block->allocation =
            std::make_shared<MappedMemory::Allocation>(memory_.get());
        if (!memory_->allocate(size / 4096, 0, *block->allocation)) {
          VELOX_FAIL("allocate() faild");
        }
        for (int i = 0; i < block->allocation->numRuns(); ++i) {
          auto run = block->allocation->runAt(i);
          for (int64_t offset = 0; offset < run.numPages() * 4096;
               offset += 4096) {
            run.data<char>()[offset] = 1;
          }
        }
      } else {
        if (!memory_->allocateContiguous(
                size / 4096, nullptr, block->contiguous)) {
          VELOX_FAIL();
        }
        for (auto offset = 0; offset < block->contiguous.numPages() * 4096;
             offset += 4096) {
          block->contiguous.data()[offset] = 1;
        }
      }
    } else {
      block->data = reinterpret_cast<char*>(malloc(size));
      for (size_t offset = 0; offset < size; offset += 4096) {
        block->data[offset] = 1;
      }
    }
    outstanding_ += size;
    blocks_.push_back(std::move(block));
  }

  void makeSpace(size_t size) {
    while (outstanding_ + size > sizeCap_) {
      size_t numBlocks = blocks_.size();
      size_t candidate = folly::Random::rand32(rng_) % (1 + numBlocks / 10);
      outstanding_ -= blocks_[candidate]->size;
      blocks_.erase(blocks_.begin() + candidate);
    }
  }

  size_t sizeBucket(size_t size) {
    auto it = std::lower_bound(buckets_.begin(), buckets_.end(), size);
    if (it == buckets_.end()) {
      return static_cast<size_t>(-1);
    }
    if (it == buckets_.begin() || *it == size) {
      return *it >> 10;
    }
    --it;
    return *it >> 10;
  }

  void initMemory(size_t sizeCap) {
    MmapAllocatorOptions options;
    options.capacity = sizeCap + (64 << 20);
    memory_ = std::make_shared<MmapAllocator>(options);
  }

  void run(uint64_t total, size_t sizeCap, bool useMmap) {
    if (useMmap) {
      initMemory(sizeCap);
    }
    sizeCap_ = sizeCap;
    uint64_t allocated = 0;
    while (allocated < total) {
      auto size = sizes_[folly::Random::rand32(rng_) % sizes_.size()];
      makeSpace(size);
      allocate(size);
      stats_[sizeBucket(size)] += size >> 10;
      allocated += size;
    }
  }

  std::string sizeString(size_t size) {
    char str[20];
    if (size < 1 << 20) {
      sprintf(str, "%ldK", size >> 10);
    } else {
      sprintf(str, "%ldM", size >> 20);
    }
    return str;
  }

 public:
  void printStats() {
    for (auto& pair : stats_) {
      std::cout << sizeString(pair.first << 10) << " = "
                << sizeString(pair.second << 10) << std::endl;
    }
  }

 protected:
  std::shared_ptr<MmapAllocator> memory_;
  std::deque<std::unique_ptr<Block>> blocks_;
  std::vector<size_t> sizes_;
  std::vector<size_t> buckets_;
  size_t outstanding_ = 0;
  size_t sizeCap_;
  std::map<size_t, size_t> stats_;
  folly::Random::DefaultGenerator rng_;
};

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto test = std::make_unique<FragmentationTest>();
  test->SetUp();
  test->run(FLAGS_volume_gb << 30, FLAGS_size_cap_gb << 30, FLAGS_use_mmap);
  test->printStats();
  return 0;
}

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

#include "velox/dwio/common/ParallelFor.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/ExecutorBarrier.h"

namespace facebook::velox::dwio::common {

namespace {

std::vector<std::pair<size_t, size_t>>
splitRange(size_t from, size_t to, size_t factor) {
  VELOX_CHECK_LE(from, to);
  std::vector<std::pair<size_t, size_t>> ranges;

  if (from == to) {
    return ranges;
  }

  if (factor <= 1) {
    ranges.emplace_back(from, to);
    return ranges;
  }

  auto rangeSize = to - from;
  auto chunkSize = rangeSize / factor;
  auto remainder = rangeSize % factor;
  auto start = from;
  for (size_t i = 0; i < factor; ++i) {
    auto end = start + chunkSize;
    if (remainder > 0) {
      --remainder;
      ++end;
    }
    // If `factor > (to - from)`, the rest of the chunks will be empty
    if (end > start) {
      ranges.emplace_back(start, end);
    } else {
      break;
    }
    start = end;
  }
  return ranges;
}

} // namespace

ParallelFor::ParallelFor(
    folly::Executor* executor,
    size_t from,
    size_t to,
    size_t parallelismFactor)
    : executor_(executor),
      ranges_{splitRange(from, to, (executor_ ? parallelismFactor : 0))} {}

ParallelFor::ParallelFor(
    std::shared_ptr<folly::Executor> executor,
    size_t from,
    size_t to,
    size_t parallelismFactor)
    : ParallelFor{executor.get(), from, to, parallelismFactor} {
  owned_ = std::move(executor);
}

void ParallelFor::execute(std::function<void(size_t)> func) {
  // Otherwise from == to
  if (ranges_.empty()) {
    return;
  }
  if (ranges_.size() == 1) {
    for (size_t i = ranges_[0].first, end = ranges_[0].second; i < end; ++i) {
      func(i);
    }
  } else {
    VELOX_CHECK(
        executor_,
        "Executor wasn't provided so we shouldn't have more than 1 range");
    ExecutorBarrier barrier(*executor_);
    const size_t last = ranges_.size() - 1;
    // First N-1 ranges in executor threads
    for (size_t r = 0; r < last; ++r) {
      auto& range = ranges_[r];
      barrier.add([begin = range.first, end = range.second, &func]() {
        for (size_t i = begin; i < end; ++i) {
          func(i);
        }
      });
    }
    // Last range in calling thread
    auto& range = ranges_[last];
    for (size_t i = range.first, end = range.second; i < end; ++i) {
      func(i);
    }
    barrier.waitAll();
  }
}

} // namespace facebook::velox::dwio::common

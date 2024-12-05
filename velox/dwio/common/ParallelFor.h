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

#pragma once

#include <vector>
#include "folly/Executor.h"

namespace facebook::velox::dwio::common {

/*
 * A helper class that allows to run a function on a range of indices in
 * multiple threads.
 * The range (from, to] is split into equal-sized chunks and each chunk is
 * scheduled in a different thread. The number of threads is: parallelismFactor
 * It means that if parallelismFactor == 1 (or 0), the function will be executed
 * in the calling thread for the entire range. If parallelismFactor == 2, the
 * function will be called for half of the range in one thread in the executor,
 * and for the last half in the calling thread (and so on). If no executor is
 * passed (nullptr), the function will be executed in the calling thread for the
 * entire range.
 */
class ParallelFor {
 public:
  ParallelFor(
      folly::Executor* executor,
      size_t from, // start index
      size_t to, // past end index
      // number of threads.
      size_t parallelismFactor);

  ParallelFor(
      std::shared_ptr<folly::Executor> executor,
      size_t from, // start index
      size_t to, // past end index
      // number of threads
      size_t parallelismFactor);

  void execute(std::function<void(size_t)> func);

 private:
  std::shared_ptr<folly::Executor> owned_;
  folly::Executor* executor_;
  std::vector<std::pair<size_t, size_t>> ranges_;
};

} // namespace facebook::velox::dwio::common

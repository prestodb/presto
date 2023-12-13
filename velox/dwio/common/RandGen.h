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

#include <folly/Conv.h>
#include <random>

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

class RandGen {
 public:
  RandGen()
      : rand_{}, mt_{rand_()}, dist_(0, std::numeric_limits<int32_t>::max()) {}

  template <typename T>
  T gen(int32_t max) {
    return folly::to<T>(gen(max));
  }

  template <typename T>
  T gen(int32_t min, int32_t max) {
    return folly::to<T>(gen(min, max));
  }

  template <typename T>
  T gen() {
    return folly::to<T>(gen());
  }

  int32_t gen(int32_t min, int32_t max) {
    return std::uniform_int_distribution<int32_t>(min, max)(mt_);
  }

  int32_t gen(int32_t max) {
    return std::uniform_int_distribution<int32_t>(0, max)(mt_);
  }

  int32_t gen() {
    return dist_(mt_);
  }

 private:
  RandGen(const RandGen&) = delete;
  RandGen(RandGen&&) = delete;
  RandGen& operator=(const RandGen&) = delete;
  RandGen& operator=(RandGen&&) = delete;

 private:
  std::random_device rand_;
  std::mt19937 mt_;
  std::uniform_int_distribution<int32_t> dist_;
};

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook

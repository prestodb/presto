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

#include <cstdint>

namespace facebook::velox::wave {

/// A device arena for device side allocation.
struct ArenaWithFreeBase {
  static constexpr uint32_t kEmpty = ~0;

  ArenaWithFreeBase(char* data, uint32_t size, uint32_t rowSize, void* freeSet)
      : rowSize(rowSize),
        base(reinterpret_cast<uint64_t>(data)),
        capacity(size),
        stringOffset(capacity),
        freeSet(freeSet) {}

  const int32_t rowSize{0};
  const uint64_t base{0};
  uint32_t rowOffset{0};
  const uint32_t capacity{0};
  uint32_t stringOffset{0};
  void* freeSet{nullptr};
  int32_t numFromFree{0};
  int32_t numFull{0};
};

struct ArenaWithFree;
} // namespace facebook::velox::wave

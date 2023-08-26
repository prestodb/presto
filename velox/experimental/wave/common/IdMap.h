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

#include <fmt/format.h>

#include "velox/experimental/wave/common/Exception.h"
#include "velox/experimental/wave/common/Hash.h"

namespace facebook::velox::wave {

template <typename T, typename H = Hasher<T, uint32_t>>
class IdMap {
 public:
  void init(int capacity, T* values, int32_t* ids);

  __device__ void clearTable();

  __device__ int32_t makeId(T value);

  __device__ int cardinality() const {
    return lastId_;
  }

 private:
  __device__ static T casValue(T* address, T compare, T val);

  __device__ void storeNewId(volatile int32_t* id);

  // `ensureIdReady' cannot be executed in the same lockstep with `storeNewId'
  // (e.g. in the else branch of `storeNewId'), which will cause deadlock if
  // both branches are executed on the same warp (making one single thread wait
  // will cause the whole warp to wait).
  __device__ static void ensureIdReady(
      volatile const int32_t* id,
      int32_t placeholder);

  static constexpr T kEmptyMarker = {};
  int capacity_;
  T* values_;
  int32_t* ids_;
  volatile int emptyId_;
  volatile int lastId_;
};

// Non-trivial class does not play well in device code.
static_assert(std::is_trivial_v<IdMap<StringView>>);

template <typename T, typename H>
void IdMap<T, H>::init(int capacity, T* values, int32_t* ids) {
  if ((capacity & (capacity - 1)) != 0) {
    waveError(fmt::format("Capacity must be power of two, got {}", capacity));
  }
  if ((uintptr_t)values % sizeof(T) != 0) {
    waveError("Values buffer must be aligned");
  }
  if ((uintptr_t)ids % sizeof(int32_t) != 0) {
    waveError("Ids buffer must be aligned");
  }
  capacity_ = capacity;
  values_ = values;
  ids_ = ids;
  emptyId_ = 0;
  lastId_ = 0;
}

} // namespace facebook::velox::wave

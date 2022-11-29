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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

inline size_t count_trailing_zeros(uint64_t x) {
  return x == 0 ? 64 : __builtin_ctzll(x);
}

inline size_t count_leading_zeros(uint64_t x) {
  return x == 0 ? 64 : __builtin_clzll(x);
}

namespace facebook::velox {

#if defined(__GNUC__) || defined(__clang__)
#define INLINE_LAMBDA __attribute__((__always_inline__))
#else
#define INLINE_LAMBDA
#endif

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define TSAN_BUILD 1
#endif
#endif

/// Define tsan_atomic<T> to be std::atomic<T?> for tsan builds and
/// T otherwise. This allows declaring variables like statistics
/// counters that do not have to be exact nor have synchronized
/// semantics. This deals with san errors while not incurring the
/// bus lock overhead at run time in regular builds.
#ifdef TSAN_BUILD
template <typename T>
using tsan_atomic = std::atomic<T>;

template <typename T>
inline T tsanAtomicValue(const std::atomic<T>& x) {
  return x;
}

/// Lock guard in tsan build and no-op otherwise.
template <typename T>
using tsan_lock_guard = std::lock_guard<T>;

#else

template <typename T>
using tsan_atomic = T;

template <typename T>
inline T tsanAtomicValue(T x) {
  return x;
}
template <typename T>
struct TsanEmptyLockGuard {
  TsanEmptyLockGuard(T& /*ignore*/) {}
};

template <typename T>
using tsan_lock_guard = TsanEmptyLockGuard<T>;

#endif

template <typename T>
inline void resizeTsanAtomic(
    std::vector<tsan_atomic<T>>& vector,
    int32_t newSize) {
  std::vector<tsan_atomic<T>> newVector(newSize);
  auto numCopy = std::min<int32_t>(newSize, vector.size());
  for (auto i = 0; i < numCopy; ++i) {
    newVector[i] = tsanAtomicValue(vector[i]);
  }
  vector = std::move(newVector);
}
} // namespace facebook::velox

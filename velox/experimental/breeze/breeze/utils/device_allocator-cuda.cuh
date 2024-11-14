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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <cuda.h>

#include <cstdlib>
#include <new>

#ifdef __EXCEPTIONS
#include "breeze/utils/types.h"
#else
#include <iostream>
#endif

namespace breeze {
namespace utils {

template <typename T>
struct device_allocator {
  typedef T value_type;

  device_allocator() = default;
  template <typename U>
  constexpr device_allocator(const device_allocator<U>&) noexcept {}

  [[nodiscard]] T* allocate(size_t n) {
    T* p;
    cudaError_t status = cudaMalloc((void**)&p, n * sizeof(T));
    if (status == cudaSuccess) {
      return p;
    }
    size_t free, total;
    cudaMemGetInfo(&free, &total);
#ifdef __EXCEPTIONS
    throw BadDeviceAlloc(n * sizeof(T), free, total);
#else
    std::cerr << "cudaMalloc failed for size=" << n * sizeof(T)
              << " free=" << free << " total=" << total << std::endl;
    std::abort();
#endif
  }
  void deallocate(T* p, size_t) noexcept { cudaFree(p); }

  // Member type other is the equivalent allocator type to allocate
  // elements of type U
  template <typename U>
  struct rebind {
    typedef device_allocator<U> other;
  };
};

}  // namespace utils
}  // namespace breeze

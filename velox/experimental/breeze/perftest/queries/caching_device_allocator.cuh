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

#include <cstdlib>
#include <map>
#include <memory>

#ifdef __EXCEPTIONS
#include "breeze/utils/types.h"
#else
#include <iostream>
#endif

#include "breeze/platforms/cuda.cuh"

// caching device allocator that can be used to eliminate
// allocation cost from benchmarks by handling all neccessary
// allocations in a warm-up run.
template <class T>
struct caching_device_allocator {
  typedef T value_type;
  typedef std::multimap<size_t, void*> free_list_type;

  caching_device_allocator(std::shared_ptr<free_list_type> free_list)
      : free_list(free_list) {}
  template <class U>
  constexpr caching_device_allocator(const caching_device_allocator<U>& other)
      : free_list(other.free_list) {}

  [[nodiscard]] T* allocate(size_t n) {
    size_t num_bytes = n * sizeof(T);
    auto it = free_list->find(num_bytes);
    if (it != free_list->end()) {
      auto p = it->second;
      free_list->erase(it);
      return reinterpret_cast<T*>(p);
    }
    void* p = nullptr;
    cudaError_t status = cudaMalloc((void**)&p, num_bytes);
    if (status != cudaSuccess) {
      size_t free, total;
      cudaMemGetInfo(&free, &total);
#ifdef __EXCEPTIONS
      throw breeze::utils::BadDeviceAlloc(num_bytes, free, total);
#else
      std::cerr << "cudaMalloc failed for size=" << num_bytes
                << " free=" << free << " total=" << total << std::endl;
      std::abort();
#endif
    }
    return reinterpret_cast<T*>(p);
  }
  void deallocate(T* p, size_t n) noexcept {
    free_list->insert({n * sizeof(T), p});
  }

  template <typename U>
  struct rebind {
    typedef caching_device_allocator<U> other;
  };

  std::shared_ptr<free_list_type> free_list;
};

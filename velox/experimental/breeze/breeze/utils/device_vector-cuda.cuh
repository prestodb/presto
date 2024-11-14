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

#include "breeze/utils/device_allocator.h"

namespace breeze {
namespace utils {

template <typename T, typename Allocator = device_allocator<T>>
class device_vector {
 public:
  explicit device_vector(const Allocator& allocator = Allocator())
      : allocator_(Allocator(allocator)) {}
  explicit device_vector(size_t size, const Allocator& allocator = Allocator())
      : allocator_(Allocator(allocator)) {
    resize(size);
  }

  // delete copy constructor
  device_vector(const device_vector&) = delete;
  device_vector& operator=(const device_vector&) = delete;

  virtual ~device_vector() { free(); }

  device_vector(device_vector&& other) {
    allocator_ = std::move(other.allocator_);
    data_ = std::move(other.data_);
    size_ = std::move(other.size_);

    // make sure it doesn't get released in destructor
    other.data_ = nullptr;
  }

  device_vector& operator=(device_vector&& other) {
    if (this != &other) {
      free();

      allocator_ = std::move(other.allocator_);
      data_ = std::move(other.data_);
      size_ = std::move(other.size_);

      // make sure it doesn't get released in destructor
      other.data_ = nullptr;
    }

    return *this;
  }

  void resize(size_t size) {
    free();
    if (size) {
      data_ = allocator_.allocate(size);
      size_ = size;
    }
  }
  template <bool ASYNCHRONOUS = false>
  void copy_from_host(const T* host_data, size_t n) {
    if constexpr (ASYNCHRONOUS) {
      cudaMemcpyAsync(data_, host_data, n * sizeof(T), cudaMemcpyHostToDevice);
    } else {
      cudaMemcpy(data_, host_data, n * sizeof(T), cudaMemcpyHostToDevice);
    }
  }
  template <bool ASYNCHRONOUS = false>
  void copy_to_host(T* host_data, size_t n) const {
    if constexpr (ASYNCHRONOUS) {
      cudaMemcpyAsync(host_data, data_, n * sizeof(T), cudaMemcpyDeviceToHost);
    } else {
      cudaMemcpy(host_data, data_, n * sizeof(T), cudaMemcpyDeviceToHost);
    }
  }
  template <bool ASYNCHRONOUS = false>
  void memset(int value, size_t n) {
    if constexpr (ASYNCHRONOUS) {
      cudaMemsetAsync(data_, value, n * sizeof(T));
    } else {
      cudaMemset(data_, value, n * sizeof(T));
    }
  }
  size_t size() const { return size_; }
  T* data() { return data_; }
  const T* data() const { return data_; }
  bool empty() const { return size_ == 0; }

 private:
  Allocator allocator_;
  T* data_ = nullptr;
  size_t size_ = 0;

  void free() {
    if (data_) {
      allocator_.deallocate(data_, size_);
      data_ = nullptr;
      size_ = 0;
    }
  }
};

}  // namespace utils
}  // namespace breeze

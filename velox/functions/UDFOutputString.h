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

#include <cstring>
#include <string_view>

namespace facebook {
namespace velox {

// An abstract class  that can be passed to the UDFs as output string parameter.
// Public functions have std::string semantics, except that resize does not
// reset the underlying string values on size extension.
class UDFOutputString {
 public:
  virtual ~UDFOutputString() {}

  size_t size() const noexcept {
    return size_;
  }

  size_t capacity() const noexcept {
    return capacity_;
  }

  char* data() const noexcept {
    return data_;
  }

  /// Has the semantics as std::string, except that it does not fill the
  /// space[size(), newSize] with 0 but rather leaves it as is
  void resize(size_t newSize) {
    if (newSize > capacity_) {
      reserve(newSize);
    }
    size_ = newSize;
    return;
  }

  /// Reserve a sequential space for the string with at least size bytes.
  /// Implemented by derived classes, should have the semantics as std::string
  virtual void reserve(size_t newSize) = 0;

  template <
      typename T,
      typename = std::enable_if_t<std::is_base_of_v<UDFOutputString, T>>>
  static void assign(T& output, const std::string_view& input) {
    output.resize(input.size());
    std::copy(input.begin(), input.end(), output.data());
  }

 protected:
  void setData(char* address) noexcept {
    data_ = address;
  }

  void setSize(size_t newSize) noexcept {
    size_ = newSize;
  }

  void setCapacity(size_t newCapacity) noexcept {
    capacity_ = newCapacity;
  }

 private:
  /// Address to the start of the string
  char* data_ = nullptr;

  /// Size of the string in bytes
  size_t size_ = 0;

  /// The capacity of the string in bytes
  size_t capacity_ = 0;
};

} // namespace velox
} // namespace facebook

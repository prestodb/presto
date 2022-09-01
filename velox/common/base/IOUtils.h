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
#include <cstring>

namespace facebook::velox::common {
struct OutputByteStream {
  explicit OutputByteStream(char* data, int32_t offset = 0)
      : data_(data), offset_{offset} {}

  template <typename T>
  void appendOne(T value) {
    *reinterpret_cast<T*>(data_ + offset_) = value;
    offset_ += sizeof(T);
  }

  void append(const char* data, int32_t size) {
    memcpy(data_ + offset_, data, size);
    offset_ += size;
  }

  int32_t offset() const {
    return offset_;
  }

 private:
  char* data_;
  int32_t offset_;
};

struct InputByteStream {
  explicit InputByteStream(const char* data) : data_(data) {}

  template <typename T>
  T read() {
    T value = *reinterpret_cast<const T*>(data_ + offset_);
    offset_ += sizeof(T);
    return value;
  }

  template <typename T>
  void copyTo(T* destination, int size) {
    memcpy(
        reinterpret_cast<char*>(destination),
        data_ + offset_,
        size * sizeof(T));
    offset_ += size * sizeof(T);
  }

  template <typename T>
  const T* read(int size) {
    auto result = reinterpret_cast<const T*>(data_ + offset_);
    offset_ += size * sizeof(T);
    return result;
  }

  int32_t offset() const {
    return offset_;
  }

 private:
  const char* data_;
  int32_t offset_{0};
};
} // namespace facebook::velox::common

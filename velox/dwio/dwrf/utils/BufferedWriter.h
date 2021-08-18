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

#include "velox/dwio/common/DataBuffer.h"

namespace facebook::velox::dwrf {

// helper class to write data in batches
template <typename T, typename F>
class BufferedWriter {
 public:
  BufferedWriter(dwio::common::DataBuffer<char>& buffer, F fn)
      : fn_{fn},
        buf_{nullptr},
        start_{reinterpret_cast<T*>(buffer.data())},
        capacity_{buffer.capacity() / sizeof(T)} {
    DWIO_ENSURE_GT(capacity_, 0, "invalid capacity");
  }

  BufferedWriter(char* buffer, size_t size, F fn)
      : fn_{fn},
        buf_{nullptr},
        start_{reinterpret_cast<T*>(buffer)},
        capacity_{size / sizeof(T)} {
    DWIO_ENSURE_GT(capacity_, 0, "invalid capacity");
  }

  BufferedWriter(memory::MemoryPool& pool, size_t size, F fn)
      : fn_{fn},
        buf_{std::make_unique<dwio::common::DataBuffer<char>>(pool, size)},
        start_{reinterpret_cast<T*>(buf_->data())},
        capacity_{size / sizeof(T)} {
    DWIO_ENSURE_GT(capacity_, 0, "invalid capacity");
  }

  ~BufferedWriter() {
    flush();
  }

  void add(T t) {
    start_[pos_++] = t;
    if (UNLIKELY(pos_ == capacity_)) {
      fn_(start_, pos_);
      pos_ = 0;
    }
  }

  void flush() {
    if (pos_ > 0) {
      fn_(start_, pos_);
      pos_ = 0;
    }
  }

 private:
  F fn_;
  std::unique_ptr<dwio::common::DataBuffer<char>> buf_;
  T* const start_;
  const size_t capacity_;

  size_t pos_{0};
};

template <typename T, typename F>
static auto createBufferedWriter(dwio::common::DataBuffer<char>& buffer, F fn) {
  return BufferedWriter<T, F>{buffer, fn};
}

template <typename T, typename F>
static auto createBufferedWriter(char* buffer, size_t size, F fn) {
  return BufferedWriter<T, F>{buffer, size, fn};
}

template <typename T, typename F>
static auto createBufferedWriter(memory::MemoryPool& pool, size_t size, F fn) {
  return BufferedWriter<T, F>{pool, size, fn};
}

// helper method to write data in batches
template <typename T, typename G, typename W>
void bufferedWrite(
    dwio::common::DataBuffer<char>& buffer,
    size_t start,
    size_t end,
    G get,
    W write) {
  BufferedWriter<T, W> writer{buffer, write};
  for (size_t i = start; i < end; ++i) {
    writer.add(get(i));
  }
}

template <typename T, typename G, typename W>
void bufferedWrite(
    memory::MemoryPool& pool,
    size_t size,
    size_t start,
    size_t end,
    G get,
    W write) {
  BufferedWriter<T, W> writer{pool, size, write};
  for (size_t i = start; i < end; ++i) {
    writer.add(get(i));
  }
}

} // namespace facebook::velox::dwrf

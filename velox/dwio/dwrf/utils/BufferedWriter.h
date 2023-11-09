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
      : buf_{nullptr},
        start_{reinterpret_cast<T*>(buffer.data())},
        capacity_{buffer.capacity() / sizeof(T)},
        fn_{std::move(fn)} {
    VELOX_CHECK_GT(capacity_, 0, "invalid capacity");
  }

  BufferedWriter(char* buffer, size_t size, F fn)
      : buf_{nullptr},
        start_{reinterpret_cast<T*>(buffer)},
        capacity_{size / sizeof(T)},
        fn_{std::move(fn)} {
    VELOX_CHECK_GT(capacity_, 0, "invalid capacity");
  }

  BufferedWriter(memory::MemoryPool& pool, size_t size, F fn)
      : buf_{std::make_unique<dwio::common::DataBuffer<char>>(pool, size)},
        start_{reinterpret_cast<T*>(buf_->data())},
        capacity_{size / sizeof(T)},
        fn_{std::move(fn)} {
    VELOX_CHECK_GT(capacity_, 0, "invalid capacity");
  }

  ~BufferedWriter() {
    VELOX_CHECK(empty(), toString());
  }

  void add(T t) {
    VELOX_CHECK(!closed_);

    start_[pos_++] = t;
    if (FOLLY_UNLIKELY(full())) {
      flush();
    }
    VELOX_CHECK_LT(pos_, capacity_);
  }

  void flush() {
    VELOX_CHECK(!closed_);

    if (!empty()) {
      // Ensures 'pos_' to be reset even if the flush fails as we don't retry.
      const auto flushPos = pos_;
      pos_ = 0;
      fn_(start_, flushPos);
    }
  }

  /// Invoked to close this buffered writer which flush all its pending buffers.
  ///
  /// NOTE: this can only be called once and we don't expect any call on this
  /// buffered writer after close.
  void close() {
    VELOX_CHECK(!closed_);
    const auto cleanup = folly::makeGuard([this]() { closed_ = true; });
    flush();
  }

  /// Invoked to abort this buffered write on error. It closes the buffered
  /// writer without flushing pending buffers.
  void abort() {
    VELOX_CHECK(!closed_);
    pos_ = 0;
    closed_ = true;
  }

  std::string toString() const {
    return fmt::format(
        "BufferedWriter[pos[{}] capacity[{}] closed[{}]]",
        pos_,
        capacity_,
        closed_);
  }

 private:
  // Indicates if this buffered writer has pending data to flush or not.
  bool empty() const {
    return pos_ == 0;
  }

  // Indicates if this buffered writer is full or not.
  bool full() const {
    return pos_ == capacity_;
  }

  const std::unique_ptr<dwio::common::DataBuffer<char>> buf_;
  T* const start_;
  const size_t capacity_;

  F fn_;
  size_t pos_{0};
  bool closed_{false};
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
  writer.close();
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
  writer.close();
}

} // namespace facebook::velox::dwrf

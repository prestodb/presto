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
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <variant>
#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"
#pragma once

namespace facebook::velox {
namespace codegen {

struct TempBuffer {
  std::unique_ptr<char*> data_ = nullptr;
  size_t capacity_;
  size_t currentValid_;

  explicit TempBuffer(size_t capacity) {
    data_ = std::make_unique<char*>(new char[capacity]);
    capacity_ = capacity;
    currentValid_ = 0;
  }

  ~TempBuffer() {
    if (data_) {
      delete[] * data_;
    }
  }

  size_t available() const {
    return capacity_ - currentValid_;
  }
};

class TempsAllocator {
 public:
  // TODO we can allocate on the pool
  [[nodiscard]] char* allocate(std::size_t n) {
    if (currentBuffer == -1 || buffers_[currentBuffer]->available() < n) {
      currentBuffer++;
      if (currentBuffer == buffers_.size()) {
        buffers_.push_back(std::make_unique<TempBuffer>(allocationSize));
      }
    }
    auto& buff = *buffers_[currentBuffer];
    assert(buff.available() >= n);

    buff.currentValid_ += n;
    return *buff.data_ + buff.currentValid_;
  }

  void reset() {
    for (auto& buff : buffers_) {
      buff->currentValid_ = 0;
    }
    currentBuffer = 0;
  }

 private:
  // Manage lifetime of the allocated buffers
  std::vector<std::unique_ptr<TempBuffer>> buffers_;

  // Must be larger than max string size, 1G for now
  static constexpr size_t allocationSize = 1000000000;

  // Point to the current buffer used for allocation
  int currentBuffer = -1;
};

// - All string types should be accessed using data() and size()
// - String types that can be written to should also have resize(),
// reserve().
// - There are five different type classes to deal with for strings:

// InputReference strings.
// - Those are coming from a Velox input vector.
// - Those are read-only.
// - We will avoid copying those when assigned to temp or reader::pointerType.
struct InputReferenceString : public std::reference_wrapper<const StringView> {
  explicit InputReferenceString(
      const std::reference_wrapper<const StringView>& string)
      : std::reference_wrapper<const StringView>(string) {}
};

struct InputReferenceStringNullable
    : public std::optional<InputReferenceString> {
  void operator=(const std::optional<InputReferenceString>& other) {
    std::optional<InputReferenceString>::operator=(other);
  }
};

// ConstantStringNullable strings.
// - Those are coming from a Velox input vector.
// - Those are read-only.
// - We will avoid copying those assigned to temp.
struct ConstantString : public std::reference_wrapper<const StringView> {
  explicit ConstantString(
      const std::reference_wrapper<const StringView>& string)
      : std::reference_wrapper<const StringView>(string) {}
};

struct ConstantStringNullable : public std::optional<ConstantString> {
  void operator=(const std::optional<ConstantString>& other) {
    std::optional<ConstantString>::operator=(other);
  }
};

// Temp strings:
// - Those do not reach output vector in the current codegen design.
// - InputReference and ConstantStringNullable and other temp strings can be
// assigned to those.
// - We want to avoid copying ConstantStringNullable and InputReference to those
// for most cases. except when explicitly requested (concat).
// - Each temp string is written only once in the current codegen design (if
// written using assignment).
template <typename Allocator = TempsAllocator>
struct TempString {
  std::reference_wrapper<TempsAllocator> allocator_;

  TempString(const TempString& other) = default;

  explicit TempString(Allocator& allocator) : allocator_(allocator) {}

  char* data_ = nullptr;

  size_t size_ = 0;

  /// Has the semantics as std::string, except that it does not fill the
  /// space[size(), newSize] with 0 but rather leaves it as is
  void resize(size_t newSize) {
    if (newSize <= size_) {
      // shrinking
      size_ = newSize;
      return;
    }

    // newSize > size
    reserve(newSize);
    size_ = newSize;
  }

  /// Reserve a sequential space for the string with at least size bytes.
  /// should have the semantics as std::string
  void reserve(size_t newCapacity) {
    auto& buffer = data_;

    if (buffer == nullptr) {
      buffer = allocator_.get().allocate(newCapacity);
      return;
    }

    // Need to allocate larger string
    char* oldBuffer = buffer;
    char* newBuffer = allocator_.get().allocate(newCapacity);
    buffer = newBuffer;

    // no need to copy old buffer
    if (size_ == 0) {
      return;
    }

    // TODO: avoid this copy when oldBuffer and newBuffer are sequential in
    // memory
    std::memcpy(newBuffer, oldBuffer, size_);
    return;
  }

  size_t size() const {
    return size_;
  }

  char* data() const {
    return data_;
  }

  template <typename T>
  typename std::enable_if<
      std::is_same_v<T, InputReferenceString> ||
          std::is_same_v<T, ConstantString>,
      void>::type
  operator=(const T& other_) {
    auto& other = other_.get();
    data_ = const_cast<char*>(other.data());
    size_ = other.size();
  }

  template <typename T>
  bool operator==(const T& rhs) const {
    if (rhs.size() != size()) {
      return false;
    }

    return memcmp(data(), rhs.data(), size()) == 0;
  }

  void operator=(const TempString<Allocator>& other) {
    this->data_ = other.data();
    this->size_ = other.size();
    this->allocator_ = other.allocator_;
  }
  void finalize() {}
};

template <typename Allocator = TempsAllocator>
struct TempStringNullable {
  TempString<Allocator> tempString;

  TempStringNullable(std::in_place_t, Allocator& allocator)
      : tempString(allocator) {
    isNull = false;
  }

  TempStringNullable(const TempStringNullable<Allocator>& other) = default;

  explicit TempStringNullable(Allocator& allocator) : tempString(allocator) {}

  bool isNull = true;

  bool has_value() const {
    return !isNull;
  }

  void operator=(const std::nullopt_t&) {
    isNull = true;
  }

  void operator=(const TempStringNullable<Allocator>& rhs) {
    isNull = rhs.isNull;
    if (!isNull) {
      tempString = rhs.tempString;
    }
  }
  // Assigns temp to inputRef or to constant
  template <typename T>
  typename std::enable_if<
      std::is_same_v<T, InputReferenceStringNullable> ||
          std::is_same_v<T, ConstantStringNullable>,
      void>::type
  operator=(const T& other) {
    if UNLIKELY (!other.has_value()) {
      // since this is assigned only once we dont even need
      isNull = true;
      return;
    }
    isNull = false;
    tempString = *other;
  }

  TempString<Allocator>& operator*() {
    assert(!isNull);
    return tempString;
  }

  const TempString<Allocator>& operator*() const {
    assert(!isNull);
    return tempString;
  }
};

// After this line is under construction

// TODO:
// Special string that is used to for the results of concat.
// this class can wrap any other writable string type . The wrapping changes the
// semantics of the assignments to always append the rhs to the string held
// inside T.
// it also change the semantics of resize, reserve, data to the append version.
// i.e: resize (x) = increase size by x.
// more details once this is implemented. For now this optimization is disabled.
template <typename T>
struct ConcatOutputString {};

// Substring in-place 0-copy when possible.
// TODO:// coming have some thoughts

} // namespace codegen
} // namespace facebook::velox

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

#include <functional>
#include <string>

#include <folly/FBString.h>
#include <folly/Range.h>
#include <folly/dynamic.h>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

// Variable length string or binary type for use in vectors. This has
// semantics similar to std::string_view or folly::StringPiece and
// exposes a subset of the interface. If the string is 12 characters
// or less, it is inlined and no reference is held. If it is longer, a
// reference to the string is held and the 4 first characters are
// cached in the StringView. This allows failing comparisons early and
// reduces the CPU cache working set when dealing with short strings.
//
// Adapted from TU Munich Umbra and CWI DuckDB.
//
// TODO: Extend the interface to parity with folly::StringPiece as needed.
struct StringView {
 public:
  using value_type = char;

  static constexpr size_t kPrefixSize = 4 * sizeof(char);
  static constexpr size_t kInlineSize = 12;

  StringView() {
    static_assert(sizeof(StringView) == 16);
    memset(this, 0, sizeof(StringView));
  }

  explicit StringView(uint32_t size) : size_(size) {
    VELOX_DCHECK(isInline(size));
    memset(prefix_, 0, kPrefixSize);
    value_.data = nullptr;
  }
  StringView(const char* data, size_t len) : size_(len) {
    VELOX_DCHECK(data || size_ == 0);
    if (isInline()) {
      // Zero the inline part.
      // this makes sure that inline strings can be compared for equality with 2
      // int64 compares.
      memset(prefix_, 0, kPrefixSize);
      if (size_ == 0) {
        return;
      }
      // small string: inlined. Zero the last 8 bytes first to allow for whole
      // word comparison.
      value_.data = nullptr;
      memcpy(prefix_, data, size_);
    } else {
      // large string: store pointer
      memcpy(prefix_, data, kPrefixSize);
      value_.data = data;
    }
  }

  // Making StringView implicitly constructible/convertible from char* and
  // string literals, in order to allow for a more flexible API and optional
  // interoperability. E.g:
  //
  //   StringView sv = "literal";
  //   std::optional<StringView> osv = "literal";
  //
  /* implicit */ StringView(const char* data)
      : StringView(data, strlen(data)) {}
  explicit StringView(const folly::fbstring& value)
      : StringView(value.data(), value.size()) {}
  explicit StringView(const std::string& value)
      : StringView(value.data(), value.size()) {}
  explicit StringView(const std::string_view& value)
      : StringView(value.data(), value.size()) {}

  bool isInline() const {
    return isInline(size_);
  }

  FOLLY_ALWAYS_INLINE static constexpr bool isInline(uint32_t size) {
    return size <= kInlineSize;
  }

  const char* data() const {
    return isInline() ? prefix_ : value_.data;
  }

  size_t size() const {
    return size_;
  }

  size_t capacity() const {
    return size_;
  }

  friend std::ostream& operator<<(
      std::ostream& os,
      const StringView& stringView) {
    os.write(stringView.data(), stringView.size());
    return os;
  }

  bool operator==(const StringView& other) const {
    // Compare lengths and first 4 characters.
    if (sizeAndPrefixAsInt64() != other.sizeAndPrefixAsInt64()) {
      return false;
    }
    if (isInline()) {
      // The inline part is zeroed at construction, so we can compare
      // a word at a time if data extends past 'prefix_'.
      return size_ <= kPrefixSize || inlinedAsInt64() == other.inlinedAsInt64();
    }
    // Sizes are equal and this is not inline, therefore both are out
    // of line and have kPrefixSize first in common.
    return memcmp(
               value_.data + kPrefixSize,
               other.value_.data + kPrefixSize,
               size_ - kPrefixSize) == 0;
  }

  bool operator!=(const StringView& other) const {
    return !(*this == other);
  }

  // Returns 0, if this == other
  //       < 0, if this < other
  //       > 0, if this > other
  int32_t compare(const StringView& other) const {
    if (prefixAsInt() != other.prefixAsInt()) {
      // The result is decided on prefix. The shorter will be less
      // because the prefix is padded with zeros.
      return memcmp(prefix_, other.prefix_, kPrefixSize);
    }
    int32_t size = std::min(size_, other.size_) - kPrefixSize;
    if (size <= 0) {
      // One ends within the prefix.
      return size_ - other.size_;
    }
    if (size <= kInlineSize && isInline() && other.isInline()) {
      int32_t result = memcmp(value_.inlined, other.value_.inlined, size);
      return (result != 0) ? result : size_ - other.size_;
    }
    int32_t result =
        memcmp(data() + kPrefixSize, other.data() + kPrefixSize, size);
    return (result != 0) ? result : size_ - other.size_;
  }

  bool operator<(const StringView& other) const {
    return compare(other) < 0;
  }

  bool operator<=(const StringView& other) const {
    return compare(other) <= 0;
  }

  bool operator>(const StringView& other) const {
    return compare(other) > 0;
  }

  bool operator>=(const StringView& other) const {
    return compare(other) >= 0;
  }

  operator folly::StringPiece() const {
    return folly::StringPiece(data(), size());
  }

  operator std::string() const {
    return std::string(data(), size());
  }

  std::string str() const {
    return *this;
  }

  std::string getString() const {
    return *this;
  }

  std::string materialize() const {
    return *this;
  }

  operator folly::dynamic() const {
    return folly::dynamic(folly::StringPiece(data(), size()));
  }

  explicit operator std::string_view() const {
    return std::string_view(data(), size());
  }

  const char* begin() const {
    return data();
  }

  const char* end() const {
    return data() + size();
  }

  bool empty() const {
    return size() == 0;
  }

 private:
  inline int64_t sizeAndPrefixAsInt64() const {
    return reinterpret_cast<const int64_t*>(this)[0];
  }

  inline int64_t inlinedAsInt64() const {
    return reinterpret_cast<const int64_t*>(this)[1];
  }

  int32_t prefixAsInt() const {
    return *reinterpret_cast<const int32_t*>(&prefix_);
  }

  // We rely on all members being laid out top to bottom . C++
  // guarantees this.
  uint32_t size_;
  char prefix_[4];
  union {
    char inlined[8];
    const char* data;
  } value_;
};

// This creates a user-defined literal for StringView. You can use it as:
//
//   auto myStringView = "my string"_sv;
//   auto vec = {"str1"_sv, "str2"_sv};
inline StringView operator"" _sv(const char* str, size_t len) {
  return StringView(str, len);
}

} // namespace facebook::velox

namespace std {
template <>
struct hash<::facebook::velox::StringView> {
  size_t operator()(const ::facebook::velox::StringView view) const {
    return facebook::velox::bits::hashBytes(1, view.data(), view.size());
  }
};
} // namespace std

namespace folly {
template <>
struct hasher<::facebook::velox::StringView> {
  size_t operator()(const ::facebook::velox::StringView view) const {
    return hash::SpookyHashV2::Hash64(view.data(), view.size(), 0);
    // return facebook::velox::bits::hashBytes(1, view.data(), view.size());
  }
};

} // namespace folly

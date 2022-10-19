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

#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::common {

/** Utility class to represent ranges of input used by DWRF writer.
This class does not dedepe overlapping ranges because for encoded input, the
overlapping range should be processed the same amount of time as specified
rather than just once.
This class does not support representing empty ranges. It
is the Caller's responsibility to avoid using the class when empty ranges are
possible.
*/
class Ranges {
 public:
  void add(size_t begin, size_t end) {
    DWIO_ENSURE_LT(begin, end);
    size_ += (end - begin);
    if (ranges_.size()) {
      // try merge with last
      auto& last = ranges_.back();
      auto& e = std::get<1>(last);
      if (e == begin) {
        e = end;
        return;
      }
    }

    // add new one
    ranges_.emplace_back(begin, end);
  }

  // returns another instance with ranges meet the filter criteria
  Ranges filter(std::function<bool(size_t)> func) const;

  class Iterator {
   public:
    Iterator(
        std::vector<std::tuple<size_t, size_t>>::const_iterator cur,
        std::vector<std::tuple<size_t, size_t>>::const_iterator end)
        : cur_{cur}, end_{end}, val_{0} {
      if (cur_ != end_) {
        val_ = std::get<0>(*cur_);
      }
    }

    bool operator==(const Iterator& other) const {
      return cur_ == other.cur_ && end_ == other.end_ && val_ == other.val_;
    }

    bool operator!=(const Iterator& other) const {
      return !operator==(other);
    }

    Iterator& operator++() {
      DCHECK(cur_ != end_);
      if (++val_ == std::get<1>(*cur_)) {
        val_ = (++cur_ != end_ ? std::get<0>(*cur_) : 0);
      }
      return *this;
    }

    const size_t& operator*() const {
      DCHECK(cur_ != end_);
      return val_;
    }

   private:
    std::vector<std::tuple<size_t, size_t>>::const_iterator cur_;
    std::vector<std::tuple<size_t, size_t>>::const_iterator end_;
    size_t val_;
  };

  Iterator begin() const {
    return Iterator{ranges_.cbegin(), ranges_.cend()};
  }

  Iterator end() const {
    return Iterator{ranges_.cend(), ranges_.cend()};
  }

  size_t size() const {
    return size_;
  }

  void clear() {
    ranges_.clear();
    size_ = 0;
  }

  const std::vector<std::tuple<size_t, size_t>>& getRanges() const {
    return ranges_;
  }

  static Ranges of(size_t begin, size_t end) {
    Ranges r;
    r.add(begin, end);
    return r;
  }

 private:
  std::vector<std::tuple<size_t, size_t>> ranges_;
  size_t size_{0};

  VELOX_FRIEND_TEST(RangeTests, Add);
  VELOX_FRIEND_TEST(RangeTests, Filter);
};

} // namespace facebook::velox::common

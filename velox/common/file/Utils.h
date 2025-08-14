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

#include "folly/io/Cursor.h"
#include "velox/common/file/File.h"
#include "velox/common/file/Region.h"

namespace facebook::velox::file::utils {

// Iterable class that produces pairs of iterators pointing to the beginning and
// end of the segments that are coalesced from the the input range, according to
// the ShouldCoalesce condition
template <typename RegionIter, typename ShouldCoalesce>
class CoalesceRegions {
 public:
  class Iter {
   public:
    Iter(RegionIter begin, RegionIter end, ShouldCoalesce shouldCoalesce)
        : begin_{begin},
          end_{end},
          theEnd_{end},
          shouldCoalesce_(std::move(shouldCoalesce)) {
      findNextEnd();
    }

    friend bool operator==(const Iter& lhs, const Iter& rhs) {
      return lhs.begin_ == rhs.begin_ && lhs.end_ == rhs.end_;
    }

    std::pair<RegionIter, RegionIter> operator*() const {
      return {begin_, end_};
    }

    Iter operator++() {
      begin_ = end_;
      end_ = theEnd_;
      findNextEnd();
      return *this;
    }

    Iter operator++(int) {
      Iter tmp(*this);
      ++(*this);
      return tmp;
    }

   private:
    void findNextEnd() {
      if (begin_ != theEnd_) {
        for (auto itA = begin_, itB = std::next(itA); itB != theEnd_;
             itA = itB, ++itB) {
          if (!shouldCoalesce_(*itA, *itB)) {
            end_ = itB;
            break;
          }
        }
      }
    }

    RegionIter begin_;
    RegionIter end_;
    RegionIter theEnd_;
    ShouldCoalesce shouldCoalesce_;
  };

  CoalesceRegions(
      RegionIter begin,
      RegionIter end,
      ShouldCoalesce shouldCoalesce)
      : begin_{begin}, end_{end}, shouldCoalesce_(std::move(shouldCoalesce)) {}

  Iter begin() {
    return Iter{begin_, end_, shouldCoalesce_};
  }

  Iter end() {
    return Iter{end_, end_, shouldCoalesce_};
  }

 private:
  RegionIter begin_;
  RegionIter end_;
  ShouldCoalesce shouldCoalesce_;
};

class CoalesceIfDistanceLE {
 public:
  explicit CoalesceIfDistanceLE(
      uint64_t maxCoalescingDistance,
      uint64_t* FOLLY_NULLABLE coalescedBytes = nullptr)
      : maxCoalescingDistance_{maxCoalescingDistance},
        coalescedBytes_{coalescedBytes} {}

  bool operator()(
      const velox::common::Region& a,
      const velox::common::Region& b) const;

 private:
  uint64_t maxCoalescingDistance_;
  uint64_t* coalescedBytes_;
};

template <typename RegionIter, typename OutputIter, typename Reader>
class ReadToIOBufs {
 public:
  ReadToIOBufs(
      RegionIter begin,
      RegionIter end,
      OutputIter output,
      Reader reader)
      : begin_{begin}, end_{end}, output_{output}, reader_{std::move(reader)} {}

  void operator()() {
    if (begin_ == end_) {
      return;
    }

    auto fileOffset = begin_->offset;
    const auto last = std::prev(end_);
    const auto readSize = last->offset + last->length - fileOffset;
    std::unique_ptr<folly::IOBuf> result = reader_(fileOffset, readSize);

    folly::io::Cursor cursor(result.get());
    for (auto region = begin_; region != end_; ++region) {
      if (fileOffset < region->offset) {
        cursor.skip(region->offset - fileOffset);
        fileOffset = region->offset;
      }
      // This clone won't copy the underlying buffer. It will just create an
      // IOBuf pointing to the right section of the existing shared buffer
      // of the original IOBuf. It can create a chained IOBuf if the original
      // IOBuf is chained, and the length of the current read spreads to the
      // next IOBuf in the chain.
      folly::IOBuf buf;
      cursor.clone(buf, region->length);
      *output_++ = std::move(buf);
      fileOffset += region->length;
    }
  }

 private:
  RegionIter begin_;
  RegionIter end_;
  OutputIter output_;
  Reader reader_;
};
} // namespace facebook::velox::file::utils

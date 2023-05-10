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

#include "velox/common/file/File.h"

namespace facebook::velox::file::utils {

// Iterable class that produces pairs of iterators pointing to the beginning and
// end of the segments that are coalesced from the the input range, according to
// the ShouldCoalesce condition
template <typename SegmentIter, typename ShouldCoalesce>
class CoalesceSegments {
 public:
  class Iter {
   public:
    Iter(SegmentIter begin, SegmentIter end, ShouldCoalesce shouldCoalesce)
        : begin_{begin},
          end_{end},
          theEnd_{end},
          shouldCoalesce_(shouldCoalesce) {
      findNextEnd();
    }

    friend bool operator==(const Iter& lhs, const Iter& rhs) {
      return lhs.begin_ == rhs.begin_ && lhs.end_ == rhs.end_;
    }

    friend bool operator!=(const Iter& lhs, const Iter& rhs) {
      return !(lhs == rhs);
    }

    std::pair<SegmentIter, SegmentIter> operator*() const {
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

    SegmentIter begin_;
    SegmentIter end_;
    SegmentIter theEnd_;
    ShouldCoalesce shouldCoalesce_;
  };

  CoalesceSegments(
      SegmentIter begin,
      SegmentIter end,
      ShouldCoalesce& shouldCoalesce)
      : begin_{begin}, end_{end}, shouldCoalesce_(shouldCoalesce) {}

  Iter begin() {
    return Iter{begin_, end_, shouldCoalesce_};
  }

  Iter end() {
    return Iter{end_, end_, shouldCoalesce_};
  }

 private:
  SegmentIter begin_;
  SegmentIter end_;
  ShouldCoalesce shouldCoalesce_;
};

class CoalesceIfDistanceLE {
 public:
  explicit CoalesceIfDistanceLE(uint64_t maxCoalescingDistance)
      : maxCoalescingDistance_(maxCoalescingDistance) {}

  bool operator()(const ReadFile::Segment* a, const ReadFile::Segment* b) const;

 private:
  uint64_t maxCoalescingDistance_;
};

} // namespace facebook::velox::file::utils

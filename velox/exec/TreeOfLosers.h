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

#include <algorithm>
#include <memory>
#include <optional>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"

#include <folly/Likely.h>

namespace facebook::velox {

// Abstract class defining the interface for a stream of values to be
// merged by TreeOfLosers or MergeArray. In addition to these, the
// MergeStream must have a way of accessing its first value and
// popping off the first value. TreeOfLosers and similar do not call
// these, so these are left out of this interface.
class MergeStream {
 public:
  virtual ~MergeStream() = default;

  // True if this has a value. If this returns true, it is valid to
  // call <. A false value means that there will not be any more data
  // in 'this'.
  virtual bool hasData() const = 0;

  // Returns true if the first element of 'this' is less than the first element
  // of 'other'. hasData() must be true of 'this' and 'other'.
  virtual bool operator<(const MergeStream& other) const {
    return compare(other) < 0;
  }

  // Returns < 0 if 'this' is < 'other, '0' if equal and > 0 otherwise. This is
  // not required for TreeOfLosers::next() but is required for
  // TreeOfLosers::nextWithEquals().
  virtual int32_t compare(const MergeStream& /*other*/) const {
    VELOX_UNSUPPORTED();
  }
};

// Implements a tree of losers algorithm for merging ordered
// streams. The TreeOfLosers owns one or more instances of
// Stream. At each call of next(), it returns the Stream that has
// the lowest value as first value from the set of Streams. It
// returns nullptr when all Streams are at end. The order is
// determined by Stream::operator<.
template <typename Stream, typename TIndex = uint16_t>
class TreeOfLosers {
 public:
  using IndexAndFlag = std::pair<TIndex, bool>;

  explicit TreeOfLosers(std::vector<std::unique_ptr<Stream>> streams)
      : streams_(std::move(streams)) {
    static_assert(std::is_base_of<MergeStream, Stream>::value);
    VELOX_CHECK_LT(streams_.size(), std::numeric_limits<TIndex>::max());
    VELOX_CHECK_GE(streams_.size(), 1);

    int32_t size = 0;
    int32_t levelSize = 1;
    int32_t numStreams = streams_.size();
    while (numStreams > levelSize) {
      size += levelSize;
      levelSize *= 2;
    }

    if (numStreams == bits::nextPowerOfTwo(numStreams)) {
      // All leaves are on last level.
      firstStream_ = size;
    } else {
      // Some of the streams are on the last level and some on the level before.
      // The first stream follows the last inner node in the node numbering.

      auto secondLastSize = levelSize / 2;
      auto overflow = numStreams - secondLastSize;
      // Suppose 12 streams. The last level has 16 places, the second
      // last 8. If we fill the second last level we have 8 streams
      // and 4 left over. These 4 need parents on the second last
      // level. So, we end up with 4 inner nodes on the second last
      // level and 8 nodes on the last level. The streams at the left
      // of the second last level become inner nodes and their streams
      // move to the level below.
      firstStream_ = (size - secondLastSize) + overflow;
    }
    values_.resize(firstStream_, kEmpty);
    equals_.resize(firstStream_, false);
  }

  // Returns the stream with the lowest first element. The caller is
  // expected to pop off the first element of the stream before
  // calling this again. Returns nullptr when all streams are at end.
  Stream* next() {
    if (UNLIKELY(lastIndex_ == kEmpty)) {
      lastIndex_ = first(0);
    } else {
      lastIndex_ = propagate(
          parent(firstStream_ + lastIndex_),
          streams_[lastIndex_]->hasData() ? lastIndex_ : kEmpty);
    }
    return lastIndex_ == kEmpty ? nullptr : streams_[lastIndex_].get();
  }

  // Returns the stream with the lowest first element and a flag that
  // is true if there is another equal value to come from some other
  // stream. The streams should have ordered unique values when using
  // this function. This is useful for merging aggregate states that
  // are unique by their key in each stream.  The caller is
  // expected to pop off the first element of the stream before
  // calling this again. Returns {nullptr, false} when all streams are at end.
  std::pair<Stream*, bool> nextWithEquals() {
    IndexAndFlag result;
    if (UNLIKELY(lastIndex_ == kEmpty)) {
      result = firstWithEquals(0);
    } else {
      result = propagateWithEquals(
          parent(firstStream_ + lastIndex_),
          streams_[lastIndex_]->hasData() ? lastIndex_ : kEmpty);
    }
    lastIndex_ = result.first;

    return lastIndex_ == kEmpty
        ? std::make_pair(nullptr, false)
        : std::make_pair(streams_[lastIndex_].get(), result.second);
    ;
  }

 private:
  static constexpr TIndex kEmpty = std::numeric_limits<TIndex>::max();

  IndexAndFlag indexAndFlag(TIndex index, bool flag) {
    return std::pair<TIndex, bool>{index, flag};
  }

  TIndex first(TIndex node) {
    if (node >= firstStream_) {
      return streams_[node - firstStream_]->hasData() ? node - firstStream_
                                                      : kEmpty;
    }
    auto left = first(leftChild(node));
    auto right = first(rightChild(node));
    if (left == kEmpty) {
      return right;
    } else if (right == kEmpty) {
      return left;
    } else if (*streams_[left] < *streams_[right]) {
      values_[node] = right;
      return left;
    } else {
      values_[node] = left;
      return right;
    }
  }

  FOLLY_ALWAYS_INLINE TIndex propagate(TIndex node, TIndex value) {
    while (UNLIKELY(values_[node] == kEmpty)) {
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
    for (;;) {
      if (UNLIKELY(values_[node] == kEmpty)) {
        // The value goes past the node and the node stays empty.
      } else if (UNLIKELY(value == kEmpty)) {
        value = values_[node];
        values_[node] = kEmpty;
      } else if (*streams_[values_[node]] < *streams_[value]) {
        // The node had the lower value, the value stays here and the previous
        // value goes up.
        std::swap(value, values_[node]);
      } else {
        // The value is less than the value in the node, No action, the value
        // goes up.
        ;
      }
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
  }

  IndexAndFlag firstWithEquals(TIndex node) {
    if (node >= firstStream_) {
      return indexAndFlag(
          streams_[node - firstStream_]->hasData() ? node - firstStream_
                                                   : kEmpty,
          false);
    }
    auto left = firstWithEquals(leftChild(node));
    auto right = firstWithEquals(rightChild(node));
    if (left.first == kEmpty) {
      return right;
    } else if (right.first == kEmpty) {
      return left;
    } else {
      auto comparison = streams_[left.first]->compare(*streams_[right.first]);
      if (comparison == 0) {
        values_[node] = right.first;
        equals_[node] = right.second;
        return indexAndFlag(left.first, true);
      } else if (comparison < 0) {
        values_[node] = right.first;
        equals_[node] = right.second;
        return left;
      } else {
        values_[node] = left.first;
        equals_[node] = right.second;
        return right;
      }
    }
  }

  FOLLY_ALWAYS_INLINE IndexAndFlag
  propagateWithEquals(TIndex node, TIndex valueIndex) {
    auto value = indexAndFlag(valueIndex, false);
    while (UNLIKELY(values_[node] == kEmpty)) {
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
    for (;;) {
      if (UNLIKELY(values_[node] == kEmpty)) {
        // The value goes past the node and the node stays empty.
      } else if (UNLIKELY(value.first == kEmpty)) {
        value = indexAndFlag(values_[node], equals_[node]);
        values_[node] = kEmpty;
        equals_[node] = false;
      } else {
        auto comparison =
            streams_[values_[node]]->compare(*streams_[value.first]);
        if (comparison == 0) {
          // the value goes up with equals set.
          value.second = true;
        } else if (comparison < 0) {
          // The node had the lower value, the value stays here and the previous
          // value goes up.
          auto newValue = indexAndFlag(values_[node], equals_[node]);
          values_[node] = value.first;
          equals_[node] = value.second;
          value = newValue;
        } else {
          // The value is less than the value in the node, No action, the value
          // goes up.
          ;
        }
      }
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
  }

  static TIndex parent(TIndex node) {
    return (node - 1) / 2;
  }

  static TIndex leftChild(TIndex node) {
    return node * 2 + 1;
  }

  static TIndex rightChild(TIndex node) {
    return node * 2 + 2;
  }
  std::vector<TIndex> values_;
  // 'true' if the corresponding element of 'values_' has met an equal
  // element on its way to its present position. Used only in nextWithEquals().
  // A byte vector is in this case faster than one of bool.
  std::vector<uint8_t> equals_;
  std::vector<std::unique_ptr<Stream>> streams_;
  TIndex lastIndex_ = kEmpty;
  int32_t firstStream_;
};

// Array-based merging structure implementing the same interface as
// TreOfLosers. The streams are sorted on their first value. The
// first stream is returned and then reinserted in the array at the
// position corresponding to the new element after the caller has
// popped off the previous first value.
template <typename Stream>
class MergeArray {
 public:
  explicit MergeArray(std::vector<std::unique_ptr<Stream>> streams) {
    static_assert(std::is_base_of<MergeStream, Stream>::value);
    for (auto& stream : streams) {
      if (stream->hasData()) {
        streams_.push_back(std::move(stream));
      }
    }
    std::sort(
        streams_.begin(),
        streams_.end(),
        [](const auto& left, const auto& right) { return *left < *right; });
  }

  // Returns the stream with the lowest first element. The caller is
  // expected to pop off the first element of the stream before
  // calling this again. Returns nullptr when all streams are at end.
  Stream* next() {
    if (UNLIKELY(isFirst_)) {
      isFirst_ = false;
      if (streams_.empty()) {
        return nullptr;
      }
      // stream has data, else it would not be here after construction.
      return streams_[0].get();
    }
    if (!streams_[0]->hasData()) {
      streams_.erase(streams_.begin());
      return streams_.empty() ? nullptr : streams_[0].get();
    }
    auto rawStreams = reinterpret_cast<Stream**>(streams_.data());
    auto first = rawStreams[0];
    auto it = std::lower_bound(
        rawStreams + 1,
        rawStreams + streams_.size(),
        first,
        [](const Stream* left, const Stream* right) { return *left < *right; });
    auto offset = it - rawStreams;
    if (offset > 1) {
      simd::memcpy(rawStreams, rawStreams + 1, (offset - 1) * sizeof(Stream*));
      it[-1] = first;
    }
    return streams_[0].get();
  }

 private:
  bool isFirst_{true};
  std::vector<std::unique_ptr<Stream>> streams_;
};

} // namespace facebook::velox

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
#include <cstring>
#include <ostream>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Range.h"
#include "velox/vector/TypeAliases.h"

namespace facebook {
namespace velox {

// A selectivityVector is used to logically filter / select data in place.
// The goal here is to be able to pass this vector between filter stages on
// different vectors while only maintaining a single copy of state and more
// importantly not ever having to re-layout the physical data. Further the
// SelectivityVector can be used to optimize filtering by skipping elements
// that where previously filtered by another filter / column
class SelectivityVector {
 public:
  SelectivityVector() {}

  explicit SelectivityVector(vector_size_t length, bool allSelected = true) {
    bits_.resize(bits::nwords(length), allSelected ? ~0ULL : 0);
    size_ = length;
    begin_ = 0;
    end_ = allSelected ? size_ : 0;
    allSelected_ = allSelected;
  }

  // Returns a statically allocated reference to an empty selectivity vector
  // (size zero).
  static const SelectivityVector& empty();

  // Returns a new allocated selectivity vector of size `size`, where all bits
  // are set to false.
  static SelectivityVector empty(vector_size_t size);

  /// Return a summary of all selected rows and row numbers of the first few
  /// selected rows. To be used for debugging.
  /// @param maxSelectedRowsToPrint Maximum number of the first selected rows to
  /// include in the output.
  std::string toString(vector_size_t maxSelectedRowsToPrint = 10) const;

  /// Resizes the vector to new size and sets the new bits with value `value`.
  void resize(int32_t size, bool value = true) {
    auto numWords = bits::nwords(size);
    // Set bits from size_ to end of the word.
    if (size > size_ && !bits_.empty()) {
      const auto start = size_ % 64;
      if (start) {
        bits::fillBits(&bits_.back(), start, 64, value);
      }
    }

    bits_.resize(numWords, value ? -1 : 0);
    size_ = size;

    updateBounds();
  }

  /// Resizes the vector to new size and sets all bits to `value`.
  void resizeFill(int32_t size, bool value = true) {
    auto numWords = bits::nwords(size);
    bits_.resize(numWords);
    std::fill(bits_.begin(), bits_.end(), value ? ~0LL : 0);
    size_ = size;
    begin_ = 0;
    end_ = value ? size_ : 0;
    allSelected_ = value;
  }

  /**
   * Set whether given index is selected. updateBounds() need to be called
   * explicitly after setValid() call, it can be called only once after multiple
   * setValid() calls in a row.
   */
  void setValid(vector_size_t idx, bool valid) {
    VELOX_DCHECK_LT(idx, bits_.size() * sizeof(bits_[0]) * 8);
    bits::setBit(bits_.data(), idx, valid);
    allSelected_.reset();
  }

  /**
   * Set a range of values to valid from [start, end). updateBounds() need to be
   * called explicitly after setValidRange() call, it can be called only once
   * after multiple setValidRange() calls in a row.
   */
  void setValidRange(vector_size_t begin, vector_size_t end, bool valid) {
    VELOX_DCHECK_LE(end, bits_.size() * sizeof(bits_[0]) * 8);
    bits::fillBits(bits_.data(), begin, end, valid);
    allSelected_.reset();
  }

  /**
   * @return true if given index is selected, false if not
   */
  bool isValid(vector_size_t idx) const {
    return bits::isBitSet(bits_.data(), idx);
  }

  const Range<bool> asRange() const {
    return Range<bool>(bits_.data(), begin_, end_);
  }

  /**
   * updateBounds() need to be called explicitly if data is modified.
   */
  MutableRange<bool> asMutableRange() {
    return MutableRange<bool>(bits_.data(), begin_, end_);
  }

  void setActiveRange(vector_size_t begin, vector_size_t end) {
    VELOX_DCHECK_LE(begin, end, "Setting begin after end");
    VELOX_DCHECK_LE(end, size_, "Range end out of range");
    begin_ = begin;
    end_ = end;
    allSelected_.reset();
  }

  vector_size_t begin() const {
    return begin_;
  }

  vector_size_t end() const {
    return end_;
  }

  /**
   * @return true if the vector has anything selected, false otherwise
   */
  bool hasSelections() const {
    return begin_ < end_;
  }

  /**
   * Sets the vector to all not selected.
   */
  void clearAll() {
    bits::fillBits(bits_.data(), 0, size_, false);
    begin_ = 0;
    end_ = 0;
    allSelected_ = false;
  }

  /**
   * Sets the vector to all selected.
   */
  void setAll() {
    bits::fillBits(bits_.data(), 0, size_, true);
    begin_ = 0;
    end_ = size_;
    allSelected_ = true;
  }

  void setFromBits(const uint64_t* bits, int32_t size) {
    auto numWords = bits::nwords(size);
    if (numWords > bits_.size()) {
      bits_.resize(numWords);
    }
    memcpy(bits_.data(), bits, numWords * 8);
    size_ = size;
    end_ = size;
    begin_ = 0;
    updateBounds();
  }

  /**
   * Removes rows that are not present in the 'other' vector.
   */
  void intersect(const SelectivityVector& other) {
    bits::andBits(
        bits_.data(), other.bits_.data(), begin_, std::min(end_, other.size()));
    updateBounds();
  }

  /**
   * Merges the valid vector of another SelectivityVector by !AND'ing them
   * together. This is used to support logical deletes where
   * any keys passing should actually be inverted
   */
  void deselect(const SelectivityVector& other) {
    bits::andWithNegatedBits(
        bits_.data(), other.bits_.data(), begin_, std::min(end_, other.size()));
    updateBounds();
  }

  void deselect(const uint64_t* bits, int32_t begin, int32_t end) {
    bits::andWithNegatedBits(
        bits_.data(),
        reinterpret_cast<const uint64_t*>(bits),
        std::max<int32_t>(begin_, begin),
        std::min<int32_t>(end_, end));
    updateBounds();
  }

  void deselectNulls(const uint64_t* bits, int32_t begin, int32_t end) {
    bits::andBits(
        bits_.data(),
        reinterpret_cast<const uint64_t*>(bits),
        std::max<int32_t>(begin_, begin),
        std::min<int32_t>(end_, end));
    updateBounds();
  }

  void deselectNonNulls(const uint64_t* bits, int32_t begin, int32_t end) {
    bits::andWithNegatedBits(
        bits_.data(),
        reinterpret_cast<const uint64_t*>(bits),
        std::max<int32_t>(begin_, begin),
        std::min<int32_t>(end_, end));
    updateBounds();
  }

  /// Clear null bits in 'nulls' for active rows.
  void clearNulls(BufferPtr& nulls) const {
    if (nulls) {
      bits::orBits(nulls->asMutable<uint64_t>(), bits_.data(), begin_, end_);
    }
  }

  void clearNulls(uint64_t* rawNulls) const {
    if (rawNulls) {
      bits::orBits(rawNulls, bits_.data(), begin_, end_);
    }
  }

  /// Set null bits in 'nulls' for active rows.
  void setNulls(BufferPtr& nulls) const {
    VELOX_CHECK_NOT_NULL(nulls);
    bits::andWithNegatedBits(
        nulls->asMutable<uint64_t>(), bits_.data(), begin_, end_);
  }

  /// Merges the valid vector of another SelectivityVector by or'ing
  /// them together. This is used to support memoization where a state
  /// may acquire new values over time. Grows 'this' if size of 'other' exceeds
  /// this size.
  void select(const SelectivityVector& other) {
    if (size_ < other.size()) {
      resize(other.size(), false);
    }
    bits::orBits(
        bits_.data(), other.bits_.data(), 0, std::min(size_, other.size()));
    updateBounds();
  }

  // Returns true if 'this' is equal to or a subset of 'other'.
  bool isSubset(const SelectivityVector& other) const {
    if (begin_ >= other.begin_ && end_ <= other.end_) {
      return bits::isSubset(bits_.data(), other.bits_.data(), begin_, end_);
    }
    return false;
  }

  /**
   * Updates the begin_ and end_ values to match the
   * current bounds of the minimum selected index and the maximum selected
   * index (noting that the range in between may contain not selected indices).
   */
  void updateBounds() {
    begin_ = bits::findFirstBit(bits_.data(), 0, size_);
    if (begin_ == -1) {
      begin_ = 0;
      end_ = 0;
      allSelected_ = false;
      return;
    }
    end_ = bits::findLastBit(bits_.data(), begin_, size_) + 1;
    allSelected_.reset();
  }

  bool isAllSelected() const {
    if (allSelected_.has_value()) {
      return allSelected_.value();
    }
    allSelected_ = begin_ == 0 && end_ == size_ &&
        bits::isAllSet(bits_.data(), 0, size_, true);
    return allSelected_.value();
  }
  /**
   * Iterate and count the number of selected values in this SelectivityVector
   */
  vector_size_t countSelected() const {
    if (allSelected_.has_value() && *allSelected_) {
      return size();
    }
    auto count = bits::countBits(bits_.data(), begin_, end_);
    allSelected_ = count == size();
    return count;
  }

  vector_size_t size() const {
    return size_;
  }

  bool operator==(const SelectivityVector& other) const {
    return begin_ == other.begin_ && end_ == other.end_ &&
        bits::testWords(
               begin_,
               end_,
               [&](int32_t index, uint64_t mask) {
                 return (bits_[index] & mask) == (other.bits_[index] & mask);
               },
               [&](int32_t index) {
                 return bits_[index] == other.bits_[index];
               });
  }
  bool operator!=(const SelectivityVector& other) const {
    return !(*this == other);
  }

  /// Invokes a function on each selected row. The function must take a single
  /// "row" argument of type vector_size_t and return void.
  template <typename Callable>
  void applyToSelected(Callable func) const;

  /// Invokes a function on each selected row sequentially in order starting
  /// from the lowest row number until a function returns 'false' or all
  /// selected rows have been processed. The function must take a single "row"
  /// argument of type vector_size_t and return a boolean indicating whether to
  /// continue (true) or stop (false).
  template <typename Callable>
  bool testSelected(Callable func) const;

  friend std::ostream& operator<<(
      std::ostream& os,
      const SelectivityVector& selectivityVector) {
    os << "SelectivityVector(begin: " << selectivityVector.begin()
       << ", end: " << selectivityVector.end() << ", selected: [";
    bool firstValidEncountered = true;

    // Intentionally going from zero to avoid surprises debugging if begin() and
    // end() not correct
    for (size_t i = 0; i < selectivityVector.size(); ++i) {
      if (selectivityVector.isValid(i)) {
        if (!firstValidEncountered) {
          os << ", ";
        }
        firstValidEncountered = false;

        os << i;
      }
    }
    os << "])";
    return os;
  }

 private:
  // the vector of bits for what is selected vs not (1 is selected)
  std::vector<uint64_t> bits_;
  // The number of leading bits used in 'bits_'.
  vector_size_t size_ = 0;
  // the minimum index of a selected value, if there are any selected
  vector_size_t begin_ = 0;
  // one past the last selected value, if there are any selected
  vector_size_t end_ = 0;
  mutable std::optional<bool> allSelected_;

  friend class SelectivityIterator;
};

class SelectivityIterator {
 public:
  explicit SelectivityIterator(const SelectivityVector& rows)
      : end_(rows.end()) {
    vector_size_t begin = rows.begin();
    if (begin == end_) {
      bits_ = nullptr;
      current_ = 0;
      index_ = 0;
      return;
    }
    bits_ = rows.bits_.data();
    current_ = rows.bits_[begin / 64] & ~bits::lowMask(begin & 63);
    if (end_ / 64 == begin / 64) {
      current_ &= bits::lowMask(end_ & 63);
    }
    index_ = begin / 64;
  }

  inline bool next(vector_size_t& row) {
    while (current_ == 0) {
      if ((index_ + 1) * 64 >= end_) {
        // The current word is done and the next starts above 'end_'.
        return false;
      }
      current_ = bits_[++index_];
      if ((index_ + 1) * 64 > end_) {
        current_ &= bits::lowMask(end_ & 63);
      }
    }
    row = (index_ * 64) + __builtin_ctzll(current_);
    current_ &= current_ - 1;
    return true;
  }

 private:
  const uint64_t* bits_;
  uint64_t current_;
  vector_size_t index_;
  const vector_size_t end_;
};

template <typename Callable>
inline void SelectivityVector::applyToSelected(Callable func) const {
  if (isAllSelected()) {
    for (vector_size_t row = begin_; row < end_; ++row) {
      func(row);
    }
  } else {
    bits::forEachSetBit(bits_.data(), begin_, end_, func);
  }
}

template <typename Callable>
inline bool SelectivityVector::testSelected(Callable func) const {
  if (isAllSelected()) {
    for (vector_size_t row = begin_; row < end_; ++row) {
      if (!func(row)) {
        return false;
      }
    }
    return true;
  }
  return bits::testSetBits(bits_.data(), begin_, end_, func);
}
} // namespace velox
} // namespace facebook

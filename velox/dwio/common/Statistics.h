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

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwio::common {

/**
 * Statistics that are available for all types of columns.
 */
class ColumnStatistics {
 public:
  ColumnStatistics(
      std::optional<uint64_t> valueCount,
      std::optional<bool> hasNull,
      std::optional<uint64_t> rawSize,
      std::optional<uint64_t> size)
      : valueCount_(valueCount),
        hasNull_(hasNull),
        rawSize_(rawSize),
        size_(size) {}

  virtual ~ColumnStatistics() = default;

  /**
   * Get the number of values in this column. It will differ from the number
   * of rows because of NULL values and repeated (list/map) values.
   */
  std::optional<uint64_t> getNumberOfValues() const {
    return valueCount_;
  }

  /**
   * Get whether column has null value
   */
  std::optional<bool> hasNull() const {
    return hasNull_;
  }

  /**
   * Get uncompressed size of all data including child
   */
  std::optional<uint64_t> getRawSize() const {
    return rawSize_;
  }

  /**
   * Get total length of all streams including child.
   */
  std::optional<uint64_t> getSize() const {
    return size_;
  }

  /**
   * return string representation of this stats object
   */
  virtual std::string toString() const {
    return folly::to<std::string>(
        "RawSize: ",
        (rawSize_ ? folly::to<std::string>(rawSize_.value()) : "unknown"),
        ", Size: ",
        (size_ ? folly::to<std::string>(size_.value()) : "unknown"),
        ", Values: ",
        (valueCount_ ? folly::to<std::string>(valueCount_.value()) : "unknown"),
        ", hasNull: ",
        (hasNull_ ? (hasNull_.value() ? "yes" : "no") : "unknown"));
  }

 protected:
  ColumnStatistics() {}

  std::optional<uint64_t> valueCount_;
  std::optional<bool> hasNull_;
  std::optional<uint64_t> rawSize_;
  std::optional<uint64_t> size_;
};

/**
 * Statistics for binary columns.
 */
class BinaryColumnStatistics : public virtual ColumnStatistics {
 public:
  BinaryColumnStatistics(
      std::optional<uint64_t> valueCount,
      std::optional<bool> hasNull,
      std::optional<uint64_t> rawSize,
      std::optional<uint64_t> size,
      std::optional<uint64_t> length)
      : ColumnStatistics(valueCount, hasNull, rawSize, size), length_(length) {}

  BinaryColumnStatistics(
      const ColumnStatistics& colStats,
      std::optional<uint64_t> length)
      : ColumnStatistics(colStats), length_(length) {}

  ~BinaryColumnStatistics() override = default;

  /**
   * get optional total length
   */
  std::optional<uint64_t> getTotalLength() const {
    return length_;
  }

  std::string toString() const override {
    return folly::to<std::string>(
        ColumnStatistics::toString(),
        ", Length: ",
        (length_.has_value() ? folly::to<std::string>(length_.value())
                             : "unknown"));
  }

 protected:
  BinaryColumnStatistics() {}

  std::optional<uint64_t> length_;
};

/**
 * Statistics for boolean columns.
 */
class BooleanColumnStatistics : public virtual ColumnStatistics {
 public:
  BooleanColumnStatistics(
      std::optional<uint64_t> valueCount,
      std::optional<bool> hasNull,
      std::optional<uint64_t> rawSize,
      std::optional<uint64_t> size,
      std::optional<uint64_t> trueCount)
      : ColumnStatistics(valueCount, hasNull, rawSize, size),
        trueCount_(trueCount) {}

  BooleanColumnStatistics(
      const ColumnStatistics& colStats,
      std::optional<uint64_t> trueCount)
      : ColumnStatistics(colStats), trueCount_(trueCount) {}

  ~BooleanColumnStatistics() override = default;

  /*
   * get optional true count
   */
  std::optional<uint64_t> getTrueCount() const {
    return trueCount_;
  }

  /*
   * get optional false count
   */
  std::optional<uint64_t> getFalseCount() const {
    auto valueCount = getNumberOfValues();
    return trueCount_.has_value() && valueCount.has_value()
        ? valueCount.value() - trueCount_.value()
        : std::optional<uint64_t>();
  }

  std::string toString() const override {
    return folly::to<std::string>(
        ColumnStatistics::toString(),
        ", trueCount: ",
        (trueCount_.has_value() ? folly::to<std::string>(trueCount_.value())
                                : "unknown"));
  }

 protected:
  BooleanColumnStatistics() {}

  std::optional<uint64_t> trueCount_;
};

/**
 * Statistics for float and double columns.
 */
class DoubleColumnStatistics : public virtual ColumnStatistics {
 public:
  DoubleColumnStatistics(
      std::optional<uint64_t> valueCount,
      std::optional<bool> hasNull,
      std::optional<uint64_t> rawSize,
      std::optional<uint64_t> size,
      std::optional<double> min,
      std::optional<double> max,
      std::optional<double> sum)
      : ColumnStatistics(valueCount, hasNull, rawSize, size),
        min_(min),
        max_(max),
        sum_(sum) {}

  DoubleColumnStatistics(
      const ColumnStatistics& colStats,
      std::optional<double> min,
      std::optional<double> max,
      std::optional<double> sum)
      : ColumnStatistics(colStats), min_(min), max_(max), sum_(sum) {}

  ~DoubleColumnStatistics() override = default;

  /**
   * Get optional smallest value in the column. Only defined if
   * getNumberOfValues is non-zero.
   */
  std::optional<double> getMinimum() const {
    return min_;
  }

  /**
   * Get optional largest value in the column. Only defined if getNumberOfValues
   * is non-zero.
   */
  std::optional<double> getMaximum() const {
    return max_;
  }

  /**
   * Get optional sum of the values in the column.
   */
  std::optional<double> getSum() const {
    return sum_;
  }

  std::string toString() const override {
    return folly::to<std::string>(
        ColumnStatistics::toString(),
        ", min: ",
        (min_.has_value() ? folly::to<std::string>(min_.value()) : "unknown"),
        ", max: ",
        (max_.has_value() ? folly::to<std::string>(max_.value()) : "unknown"),
        ", sum: ",
        (sum_.has_value() ? folly::to<std::string>(sum_.value()) : "unknown"));
  }

 protected:
  DoubleColumnStatistics() {}

  std::optional<double> min_;
  std::optional<double> max_;
  std::optional<double> sum_;
};

/**
 * Statistics for all of the integer columns, such as byte, short, int, and
 * long.
 */
class IntegerColumnStatistics : public virtual ColumnStatistics {
 public:
  IntegerColumnStatistics(
      std::optional<uint64_t> valueCount,
      std::optional<bool> hasNull,
      std::optional<uint64_t> rawSize,
      std::optional<uint64_t> size,
      std::optional<int64_t> min,
      std::optional<int64_t> max,
      std::optional<int64_t> sum)
      : ColumnStatistics(valueCount, hasNull, rawSize, size),
        min_(min),
        max_(max),
        sum_(sum) {}

  IntegerColumnStatistics(
      const ColumnStatistics& colStats,
      std::optional<int64_t> min,
      std::optional<int64_t> max,
      std::optional<int64_t> sum)
      : ColumnStatistics(colStats), min_(min), max_(max), sum_(sum) {}

  ~IntegerColumnStatistics() override = default;

  /**
   * Get optional smallest value in the column. Only defined if
   * getNumberOfValues is non-zero.
   */
  std::optional<int64_t> getMinimum() const {
    return min_;
  }

  /**
   * Get optional largest value in the column. Only defined if getNumberOfValues
   * is non-zero.
   */
  std::optional<int64_t> getMaximum() const {
    return max_;
  }

  /**
   * Get optional sum of the column. Only valid if getNumberOfValues is non-zero
   * and sum doesn't overflow
   */
  std::optional<int64_t> getSum() const {
    return sum_;
  }

  std::string toString() const override {
    return folly::to<std::string>(
        ColumnStatistics::toString(),
        ", min: ",
        (min_.has_value() ? folly::to<std::string>(min_.value()) : "unknown"),
        ", max: ",
        (max_.has_value() ? folly::to<std::string>(max_.value()) : "unknown"),
        ", sum: ",
        (sum_.has_value() ? folly::to<std::string>(sum_.value()) : "unknown"));
  }

 protected:
  IntegerColumnStatistics() {}

  std::optional<int64_t> min_;
  std::optional<int64_t> max_;
  std::optional<int64_t> sum_;
};

/**
 * Statistics for string columns.
 */
class StringColumnStatistics : public virtual ColumnStatistics {
 public:
  StringColumnStatistics(
      std::optional<uint64_t> valueCount,
      std::optional<bool> hasNull,
      std::optional<uint64_t> rawSize,
      std::optional<uint64_t> size,
      std::optional<std::string> min,
      std::optional<std::string> max,
      std::optional<int64_t> length)
      : ColumnStatistics(valueCount, hasNull, rawSize, size),
        min_(min),
        max_(max),
        length_(length) {}

  StringColumnStatistics(
      const ColumnStatistics& colStats,
      std::optional<std::string> min,
      std::optional<std::string> max,
      std::optional<int64_t> length)
      : ColumnStatistics(colStats), min_(min), max_(max), length_(length) {}

  ~StringColumnStatistics() override = default;

  /**
   * Get optional minimum value for the column.
   */
  const std::optional<std::string>& getMinimum() const {
    return min_;
  }

  /**
   * Get optional maximum value for the column.
   */
  const std::optional<std::string>& getMaximum() const {
    return max_;
  }

  /**
   * Get optional total length of all values.
   */
  std::optional<uint64_t> getTotalLength() const {
    return length_;
  }

  std::string toString() const override {
    return folly::to<std::string>(
        ColumnStatistics::toString(),
        ", min: ",
        min_.value_or("unknown"),
        ", max: ",
        max_.value_or("unknown"),
        ", length: ",
        (length_.has_value() ? folly::to<std::string>(length_.value())
                             : "unknown"));
  }

 protected:
  StringColumnStatistics() {}

  std::optional<std::string> min_;
  std::optional<std::string> max_;
  std::optional<uint64_t> length_;
};

class Statistics {
 public:
  virtual ~Statistics() = default;

  /**
   * Get the statistics of the given column.
   * @param colId id of the column
   * @return one column's statistics
   */
  virtual const ColumnStatistics& getColumnStatistics(uint32_t colId) const = 0;

  /**
   * Get the number of columns
   * @return the number of columns
   */
  virtual uint32_t getNumberOfColumns() const = 0;
};
struct RuntimeStatistics {
  // Number of splits skipped based on statistics.
  int64_t skippedSplits{0};

  // Total bytes in splits skipped based on statistics.
  int64_t skippedSplitBytes{0};

  // Number of strides (row groups) skipped based on statistics.
  int64_t skippedStrides{0};

  std::unordered_map<std::string, RuntimeCounter> toMap() {
    return {
        {"skippedSplits", RuntimeCounter(skippedSplits)},
        {"skippedSplitBytes",
         RuntimeCounter(skippedSplitBytes, RuntimeCounter::Unit::kBytes)},
        {"skippedStrides", RuntimeCounter(skippedStrides)}};
  }
};

} // namespace facebook::velox::dwio::common

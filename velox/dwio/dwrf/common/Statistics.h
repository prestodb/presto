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

#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"

namespace facebook::velox::dwrf {

/**
 * StatsContext contains fields required to compute statistics
 */

struct StatsContext {
  const std::string writerName;
  WriterVersion writerVersion;

  StatsContext(const std::string& name, WriterVersion version)
      : writerName(name), writerVersion{version} {}

  explicit StatsContext(WriterVersion version)
      : writerName(""), writerVersion{version} {}
};

/**
 * Statistics that are available for all types of columns.
 */
class ColumnStatistics {
 public:
  explicit ColumnStatistics(const proto::ColumnStatistics& stats) {
    if (stats.has_numberofvalues()) {
      valueCount_ = stats.numberofvalues();
    }
    if (stats.has_hasnull()) {
      hasNull_ = stats.hasnull();
    }
    if (stats.has_rawsize()) {
      rawSize_ = stats.rawsize();
    }
    if (stats.has_size()) {
      size_ = stats.size();
    }
  }

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

  static std::unique_ptr<ColumnStatistics> fromProto(
      const proto::ColumnStatistics& stats,
      const StatsContext& statsContext);

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
  explicit BinaryColumnStatistics(const proto::ColumnStatistics& stats)
      : ColumnStatistics(stats) {
    DWIO_ENSURE(stats.has_binarystatistics());
    const auto& binStats = stats.binarystatistics();
    // In proto, length(sum) is defined as sint. We need to make sure length
    // is not negative
    if (binStats.has_sum() && binStats.sum() >= 0) {
      length_ = static_cast<uint64_t>(binStats.sum());
    }
  }

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
  explicit BooleanColumnStatistics(const proto::ColumnStatistics& stats)
      : ColumnStatistics{stats} {
    DWIO_ENSURE(stats.has_bucketstatistics());
    const auto& bucketStats = stats.bucketstatistics();
    // Need to make sure there is at least one bucket. True count is saved in
    // bucket 0
    if (bucketStats.count_size() > 0) {
      trueCount_ = bucketStats.count(0);
    }
  }

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
  explicit DoubleColumnStatistics(const proto::ColumnStatistics& stats)
      : ColumnStatistics{stats} {
    DWIO_ENSURE(stats.has_doublestatistics());
    // It's possible min/max/sum is NaN. The factory method that creates this
    // stats object should check if any of them is NaN, and fall back to basic
    // column stats when possible
    const auto& doubleStats = stats.doublestatistics();
    if (doubleStats.has_minimum() && !std::isnan(doubleStats.minimum())) {
      min_ = doubleStats.minimum();
    }
    if (doubleStats.has_maximum() && !std::isnan(doubleStats.maximum())) {
      max_ = doubleStats.maximum();
    }
    if (doubleStats.has_sum() && !std::isnan(doubleStats.sum())) {
      sum_ = doubleStats.sum();
    }
  }

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
  explicit IntegerColumnStatistics(const proto::ColumnStatistics& stats)
      : ColumnStatistics{stats} {
    DWIO_ENSURE(stats.has_intstatistics());
    const auto& intStats = stats.intstatistics();
    if (intStats.has_minimum()) {
      min_ = intStats.minimum();
    }
    if (intStats.has_maximum()) {
      max_ = intStats.maximum();
    }
    if (intStats.has_sum()) {
      sum_ = intStats.sum();
    }
  }

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
  explicit StringColumnStatistics(const proto::ColumnStatistics& stats)
      : ColumnStatistics{stats} {
    DWIO_ENSURE(stats.has_stringstatistics());
    const auto& strStats = stats.stringstatistics();
    if (strStats.has_minimum()) {
      min_ = strStats.minimum();
    }
    if (strStats.has_maximum()) {
      max_ = strStats.maximum();
    }
    // In proto, length(sum) is defined as sint. We need to make sure length
    // is not negative
    if (strStats.has_sum() && strStats.sum() >= 0) {
      length_ = strStats.sum();
    }
  }

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

} // namespace facebook::velox::dwrf

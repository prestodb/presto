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

#include <velox/common/base/Exceptions.h>
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/common/Statistics.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/type/Type.h"

namespace facebook::velox::dwrf {

namespace {
inline bool isEmpty(const dwio::common::ColumnStatistics& stats) {
  auto valueCount = stats.getNumberOfValues();
  return valueCount.has_value() && valueCount.value() == 0;
}

template <typename T>
static void
addWithOverflowCheck(std::optional<T>& to, T value, uint64_t count) {
  if (to.has_value()) {
    // check overflow. Value is only valid when not overflow
    T result;
    auto overflow = __builtin_mul_overflow(value, count, &result);
    if (!overflow) {
      overflow = __builtin_add_overflow(to.value(), result, &to.value());
    }
    if (overflow) {
      to.reset();
    }
  }
}

template <typename T>
static void mergeWithOverflowCheck(
    std::optional<T>& to,
    const std::optional<T>& from) {
  if (to.has_value()) {
    if (from.has_value()) {
      auto overflow =
          __builtin_add_overflow(to.value(), from.value(), &to.value());
      if (overflow) {
        to.reset();
      }
    } else {
      to.reset();
    }
  }
}

inline dwio::common::KeyInfo constructKey(const dwrf::proto::KeyInfo& keyInfo) {
  if (keyInfo.has_intkey()) {
    return dwio::common::KeyInfo{keyInfo.intkey()};
  } else if (keyInfo.has_byteskey()) {
    return dwio::common::KeyInfo{keyInfo.byteskey()};
  }
  VELOX_UNREACHABLE("Illegal null key info");
}
} // namespace

struct StatisticsBuilderOptions {
  explicit StatisticsBuilderOptions(
      uint32_t stringLengthLimit,
      std::optional<uint64_t> initialSize = std::nullopt)
      : stringLengthLimit{stringLengthLimit}, initialSize{initialSize} {}

  uint32_t stringLengthLimit;
  std::optional<uint64_t> initialSize;

  static StatisticsBuilderOptions fromConfig(const Config& config) {
    return StatisticsBuilderOptions{config.get(Config::STRING_STATS_LIMIT)};
  }
};

/*
 * Base class for stats builder. Stats builder is used in writer and file merge
 * to collect and merge stats.
 */
class StatisticsBuilder : public virtual dwio::common::ColumnStatistics {
 public:
  explicit StatisticsBuilder(const StatisticsBuilderOptions& options)
      : options_{options} {
    init();
  }

  ~StatisticsBuilder() override = default;

  void setHasNull() {
    hasNull_ = true;
  }

  void increaseValueCount(uint64_t count = 1) {
    if (valueCount_.has_value()) {
      valueCount_.value() += count;
    }
  }

  void increaseRawSize(uint64_t rawSize) {
    if (rawSize_.has_value()) {
      rawSize_.value() += rawSize;
    }
  }

  void clearRawSize() {
    rawSize_.reset();
  }

  /*
   * Merge stats of same type. This is used in writer to aggregate file level
   * stats.
   */
  virtual void merge(const dwio::common::ColumnStatistics& other);

  /*
   * Reset. Used in the place where row index entry level stats in captured.
   */
  virtual void reset() {
    init();
  }

  /*
   * Write stats to proto
   */
  virtual void toProto(proto::ColumnStatistics& stats) const;

  std::unique_ptr<dwio::common::ColumnStatistics> build() const;

  static std::unique_ptr<StatisticsBuilder> create(
      const Type& type,
      const StatisticsBuilderOptions& options);

  // for the given type tree, create the a list of stat builders
  static void createTree(
      std::vector<std::unique_ptr<StatisticsBuilder>>& statBuilders,
      const Type& type,
      const StatisticsBuilderOptions& options);

 private:
  void init() {
    valueCount_ = 0;
    hasNull_ = false;
    rawSize_ = 0;
    size_ = options_.initialSize;
  }

 protected:
  StatisticsBuilderOptions options_;
};

class BooleanStatisticsBuilder : public StatisticsBuilder,
                                 public dwio::common::BooleanColumnStatistics {
 public:
  explicit BooleanStatisticsBuilder(const StatisticsBuilderOptions& options)
      : StatisticsBuilder{options} {
    init();
  }

  ~BooleanStatisticsBuilder() override = default;

  void addValues(bool value, uint64_t count = 1) {
    increaseValueCount(count);
    if (trueCount_.has_value() && value) {
      trueCount_.value() += count;
    }
  }

  void merge(const dwio::common::ColumnStatistics& other) override;

  void reset() override {
    StatisticsBuilder::reset();
    init();
  }

  void toProto(proto::ColumnStatistics& stats) const override;

 private:
  void init() {
    trueCount_ = 0;
  }
};

class IntegerStatisticsBuilder : public StatisticsBuilder,
                                 public dwio::common::IntegerColumnStatistics {
 public:
  explicit IntegerStatisticsBuilder(const StatisticsBuilderOptions& options)
      : StatisticsBuilder{options} {
    init();
  }

  ~IntegerStatisticsBuilder() override = default;

  void addValues(int64_t value, uint64_t count = 1) {
    increaseValueCount(count);
    if (min_.has_value() && value < min_.value()) {
      min_ = value;
    }
    if (max_.has_value() && value > max_.value()) {
      max_ = value;
    }
    addWithOverflowCheck(sum_, value, count);
  }

  void merge(const dwio::common::ColumnStatistics& other) override;

  void reset() override {
    StatisticsBuilder::reset();
    init();
  }

  void toProto(proto::ColumnStatistics& stats) const override;

 private:
  void init() {
    min_ = std::numeric_limits<int64_t>::max();
    max_ = std::numeric_limits<int64_t>::min();
    sum_ = 0;
  }
};

static_assert(
    std::numeric_limits<double>::has_infinity,
    "infinity not defined");

class DoubleStatisticsBuilder : public StatisticsBuilder,
                                public dwio::common::DoubleColumnStatistics {
 public:
  explicit DoubleStatisticsBuilder(const StatisticsBuilderOptions& options)
      : StatisticsBuilder{options} {
    init();
  }

  ~DoubleStatisticsBuilder() override = default;

  void addValues(double value, uint64_t count = 1) {
    increaseValueCount(count);
    // min/max/sum is defined only when none of the values added is NaN
    if (std::isnan(value)) {
      clear();
      return;
    }

    if (min_.has_value() && value < min_.value()) {
      min_ = value;
    }
    if (max_.has_value() && value > max_.value()) {
      max_ = value;
    }
    // value * count sometimes is not same as adding values (count) times. So
    // add in a loop
    if (sum_.has_value()) {
      for (uint64_t i = 0; i < count; ++i) {
        sum_.value() += value;
      }
      if (std::isnan(sum_.value())) {
        sum_.reset();
      }
    }
  }

  void merge(const dwio::common::ColumnStatistics& other) override;

  void reset() override {
    StatisticsBuilder::reset();
    init();
  }

  void toProto(proto::ColumnStatistics& stats) const override;

 private:
  void init() {
    min_ = std::numeric_limits<double>::infinity();
    max_ = -std::numeric_limits<double>::infinity();
    sum_ = 0;
  }

  void clear() {
    min_.reset();
    max_.reset();
    sum_.reset();
  }
};

class StringStatisticsBuilder : public StatisticsBuilder,
                                public dwio::common::StringColumnStatistics {
 public:
  explicit StringStatisticsBuilder(const StatisticsBuilderOptions& options)
      : StatisticsBuilder{options}, lengthLimit_{options.stringLengthLimit} {
    init();
  }

  ~StringStatisticsBuilder() override = default;

  void addValues(folly::StringPiece value, uint64_t count = 1) {
    // min_/max_ is not initialized with default that can be compared against
    // easily. So we need to capture whether self is empty and handle
    // differently.
    auto isSelfEmpty = isEmpty(*this);
    increaseValueCount(count);
    if (isSelfEmpty) {
      min_ = value;
      max_ = value;
    } else {
      if (min_.has_value() && value < folly::StringPiece{min_.value()}) {
        min_ = value;
      }
      if (max_.has_value() && value > folly::StringPiece{max_.value()}) {
        max_ = value;
      }
    }

    addWithOverflowCheck<uint64_t>(length_, value.size(), count);
  }

  void merge(const dwio::common::ColumnStatistics& other) override;

  void reset() override {
    StatisticsBuilder::reset();
    init();
  }

  void toProto(proto::ColumnStatistics& stats) const override;

 private:
  uint32_t lengthLimit_;

  void init() {
    min_.reset();
    max_.reset();
    length_ = 0;
  }

  bool shouldKeep(const std::optional<std::string>& val) const {
    return val.has_value() && val.value().size() <= lengthLimit_;
  }
};

class BinaryStatisticsBuilder : public StatisticsBuilder,
                                public dwio::common::BinaryColumnStatistics {
 public:
  explicit BinaryStatisticsBuilder(const StatisticsBuilderOptions& options)
      : StatisticsBuilder{options} {
    init();
  }

  ~BinaryStatisticsBuilder() override = default;

  void addValues(uint64_t length, uint64_t count = 1) {
    increaseValueCount(count);
    addWithOverflowCheck(length_, length, count);
  }

  void merge(const dwio::common::ColumnStatistics& other) override;

  void reset() override {
    StatisticsBuilder::reset();
    init();
  }

  void toProto(proto::ColumnStatistics& stats) const override;

 private:
  void init() {
    length_ = 0;
  }
};

class MapStatisticsBuilder : public StatisticsBuilder,
                             public dwio::common::MapColumnStatistics {
 public:
  MapStatisticsBuilder(
      const Type& type,
      const StatisticsBuilderOptions& options)
      : StatisticsBuilder{options},
        valueType_{type.as<velox::TypeKind::MAP>().valueType()} {
    init();
  }

  ~MapStatisticsBuilder() override = default;

  void addValues(
      const dwrf::proto::KeyInfo& keyInfo,
      const StatisticsBuilder& stats) {
    // Since addValues is called once per key info per stride,
    // it's ok to just construct the key struct per call.
    auto& keyStats = getKeyStats(constructKey(keyInfo));
    keyStats.merge(stats);
  }

  void merge(const dwio::common::ColumnStatistics& other) override;

  void reset() override {
    StatisticsBuilder::reset();
    init();
  }

  void toProto(proto::ColumnStatistics& stats) const override;

 private:
  void init() {
    entryStatistics_.clear();
  }

  StatisticsBuilder& getKeyStats(const dwio::common::KeyInfo& keyInfo) {
    auto result = entryStatistics_.try_emplace(
        keyInfo, StatisticsBuilder::create(*valueType_, options_));
    return dynamic_cast<StatisticsBuilder&>(*result.first->second);
  }

  const TypePtr valueType_;
};
} // namespace facebook::velox::dwrf

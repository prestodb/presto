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

#include "velox/common/base/Fs.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"

/// Base classes for schema elements used in execution. A Schema is a collection
/// of Tables. A Table is a collection of Columns. Tables and Columns have
/// statistics and Tables can be sampled. Derived classes connect to different
/// metadata stores and provide different metadata, e.g. order, partitioning,
/// bucketing etc.
namespace facebook::velox::runner {

/// Base class for column. The column's name and type are immutable but the
/// stats may be set multiple times.
class Column {
 public:
  virtual ~Column() = default;

  Column(const std::string& name, TypePtr type) : name_(name), type_(type) {}

  dwio::common::ColumnStatistics* stats() const {
    return latestStats_;
  }

  /// Sets statistics. May be called multipl times if table contents change.
  void setStats(std::unique_ptr<dwio::common::ColumnStatistics> stats) {
    std::lock_guard<std::mutex> l(mutex_);
    allStats_.push_back(std::move(stats));
    latestStats_ = allStats_.back().get();
  }

  const std::string& name() const {
    return name_;
  }

  const TypePtr& type() const {
    return type_;
  }

  void setNumDistinct(int64_t numDistinct) {
    approxNumDistinct_ = numDistinct;
  }

 protected:
  const std::string name_;
  const TypePtr type_;

  // The latest element added to 'allStats_'.
  tsan_atomic<dwio::common::ColumnStatistics*> latestStats_{nullptr};

  // All statistics recorded for this column. Old values can be purged when the
  // containing Schema is not in use.
  std::vector<std::unique_ptr<dwio::common::ColumnStatistics>> allStats_;

  // Latest approximate count of distinct values.
  std::optional<int64_t> approxNumDistinct_;

 private:
  // Serializes changes to statistics.
  std::mutex mutex_;
};

class Schema;

/// Base class for table. This is used to identify a table  for purposes of
/// Split generation, statistics, sampling etc.
class Table {
 public:
  virtual ~Table() = default;

  Table(
      const std::string& name,
      dwio::common::FileFormat format,
      Schema* schema)
      : schema_(schema), name_(name), format_(format) {}

  const std::string& name() const {
    return name_;
  }

  const RowTypePtr& rowType() const {
    return type_;
  }

  dwio::common::FileFormat format() const {
    return format_;
  }

  /// Samples 'pct' percent of rows for 'fields'. Applies 'filters'
  /// before sampling. Returns {count of sampled, count matching filters}.
  /// Returns statistics for the post-filtering values in 'stats' for each of
  /// 'fields'. If 'fields' is empty, simply returns the number of
  /// rows matching 'filter' in a sample of 'pct'% of the table.
  ///
  /// TODO: Introduce generic statistics builder in dwio/common.
  virtual std::pair<int64_t, int64_t> sample(
      float pct,
      const std::vector<common::Subfield>& columns,
      connector::hive::SubfieldFilters filters,
      const core::TypedExprPtr& remainingFilter,
      HashStringAllocator* allocator = nullptr,
      std::vector<std::unique_ptr<dwrf::StatisticsBuilder>>* statsBuilders =
          nullptr) {
    VELOX_UNSUPPORTED("Table class does not support sampling.");
  }

 protected:
  Schema* const schema_;
  const std::string name_;

  const dwio::common::FileFormat format_;

  // Discovered from data. In the event of different types, we take the
  // latest (i.e. widest) table type.
  RowTypePtr type_;
};

/// Base class for collection of tables. A query executes against a
/// Schema and its tables and columns are resolved against the
/// Schema. The schema is mutable and may acquire tables and the
/// tables may acquire stats during their lifetime.
class Schema {
 public:
  virtual ~Schema() = default;

  Schema(const std::string& name, memory::MemoryPool* pool)
      : name_(name), pool_(std::move(pool)) {}

  Table* findTable(const std::string& name) {
    auto it = tables_.find(name);
    VELOX_CHECK(it != tables_.end(), "Table {} not found", name);
    return it->second.get();
  }

  virtual connector::Connector* connector() const = 0;

  virtual const std::shared_ptr<connector::ConnectorQueryCtx>&
  connectorQueryCtx() const = 0;

 protected:
  const std::string name_;

  memory::MemoryPool* const pool_;

  std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
};

} // namespace facebook::velox::runner

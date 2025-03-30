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

#include "folly/CancellationToken.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/common/base/PrefixSortConfig.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/common/base/SpillStats.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/core/ExpressionEvaluator.h"
#include "velox/type/Subfield.h"
#include "velox/vector/ComplexVector.h"

#include <folly/Synchronized.h>

namespace facebook::velox {
class Config;
}
namespace facebook::velox::wave {
class WaveDataSource;
}
namespace facebook::velox::common {
class Filter;
}
namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::core {
class ITypedExpr;
}

namespace facebook::velox::core {
struct IndexLookupCondition;
}

namespace facebook::velox::connector {

class DataSource;

/// A split represents a chunk of data that a connector should load and return
/// as a RowVectorPtr, potentially after processing pushdowns.
struct ConnectorSplit : public ISerializable {
  const std::string connectorId;
  const int64_t splitWeight{0};
  const bool cacheable{true};

  std::unique_ptr<AsyncSource<DataSource>> dataSource;

  explicit ConnectorSplit(
      const std::string& _connectorId,
      int64_t _splitWeight = 0,
      bool _cacheable = true)
      : connectorId(_connectorId),
        splitWeight(_splitWeight),
        cacheable(_cacheable) {}

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED();
    return nullptr;
  }

  virtual ~ConnectorSplit() {}

  virtual std::string toString() const {
    return fmt::format(
        "[split: connector id {}, weight {}, cacheable {}]",
        connectorId,
        splitWeight,
        cacheable ? "true" : "false");
  }
};

class ColumnHandle : public ISerializable {
 public:
  virtual ~ColumnHandle() = default;

  virtual const std::string& name() const {
    VELOX_UNSUPPORTED();
  }

  folly::dynamic serialize() const override;

 protected:
  static folly::dynamic serializeBase(std::string_view name);
};

using ColumnHandlePtr = std::shared_ptr<const ColumnHandle>;

class ConnectorTableHandle : public ISerializable {
 public:
  explicit ConnectorTableHandle(std::string connectorId)
      : connectorId_(std::move(connectorId)) {}

  virtual ~ConnectorTableHandle() = default;

  virtual std::string toString() const {
    VELOX_NYI();
  }

  const std::string& connectorId() const {
    return connectorId_;
  }

  /// Returns the connector-dependent table name. Used with
  /// ConnectorMetadata. Implementations need to supply a definition
  /// to work with metadata.
  virtual const std::string& name() const {
    VELOX_UNSUPPORTED();
  }

  /// Returns true if the connector table handle supports index lookup.
  virtual bool supportsIndexLookup() const {
    return false;
  }

  virtual folly::dynamic serialize() const override;

 protected:
  folly::dynamic serializeBase(std::string_view name) const;

 private:
  const std::string connectorId_;
};

using ConnectorTableHandlePtr = std::shared_ptr<const ConnectorTableHandle>;

/// Represents a request for writing to connector
class ConnectorInsertTableHandle : public ISerializable {
 public:
  virtual ~ConnectorInsertTableHandle() {}

  /// Whether multi-threaded write is supported by this connector. Planner uses
  /// this flag to determine number of drivers.
  virtual bool supportsMultiThreading() const {
    return false;
  }

  virtual std::string toString() const = 0;

  folly::dynamic serialize() const override {
    VELOX_NYI();
  }
};

/// Represents the commit strategy for writing to connector.
enum class CommitStrategy {
  /// No more commit actions are needed.
  kNoCommit,
  /// Task level commit is needed.
  kTaskCommit
};

/// Return a string encoding of the given commit strategy.
std::string commitStrategyToString(CommitStrategy commitStrategy);

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    CommitStrategy strategy) {
  os << commitStrategyToString(strategy);
  return os;
}

/// Return a commit strategy of the given string encoding.
CommitStrategy stringToCommitStrategy(const std::string& strategy);

/// Writes data received from table writer operator into different partitions
/// based on the specific table layout. The actual implementation doesn't need
/// to be thread-safe.
class DataSink {
 public:
  struct Stats {
    uint64_t numWrittenBytes{0};
    uint32_t numWrittenFiles{0};
    uint64_t writeIOTimeUs{0};
    uint64_t numCompressedBytes{0};
    uint64_t recodeTimeNs{0};
    uint64_t compressionTimeNs{0};

    common::SpillStats spillStats;

    bool empty() const;

    std::string toString() const;
  };

  virtual ~DataSink() = default;

  /// Add the next data (vector) to be written. This call is blocking.
  /// TODO maybe at some point we want to make it async.
  virtual void appendData(RowVectorPtr input) = 0;

  /// Called after all data has been added via possibly multiple calls to
  /// appendData() This function finishes the data procesing like sort all the
  /// added data and write them to the file writer. The finish might take long
  /// time so it returns false to yield in the middle of processing. The
  /// function returns true if it has processed all data. This call is blocking.
  virtual bool finish() = 0;

  /// Called once after all data has been added via possibly multiple calls to
  /// appendData(). The function returns the metadata of written data in string
  /// form. We don't expect any appendData() calls on a closed data sink object.
  virtual std::vector<std::string> close() = 0;

  /// Called to abort this data sink object and we don't expect any appendData()
  /// calls on an aborted data sink object.
  virtual void abort() = 0;

  /// Returns the stats of this data sink.
  virtual Stats stats() const = 0;
};

class DataSource {
 public:
  static constexpr int64_t kUnknownRowSize = -1;
  virtual ~DataSource() = default;

  /// Add split to process, then call next multiple times to process the split.
  /// A split must be fully processed by next before another split can be
  /// added. Next returns nullptr to indicate that current split is fully
  /// processed.
  virtual void addSplit(std::shared_ptr<ConnectorSplit> split) = 0;

  /// Process a split added via addSplit. Returns nullptr if split has been
  /// fully processed. Returns std::nullopt and sets the 'future' if started
  /// asynchronous work and needs to wait for it to complete to continue
  /// processing. The caller will wait for the 'future' to complete before
  /// calling 'next' again.
  virtual std::optional<RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) = 0;

  /// Add dynamically generated filter.
  /// @param outputChannel index into outputType specified in
  /// Connector::createDataSource() that identifies the column this filter
  /// applies to.
  virtual void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) = 0;

  /// Returns the number of input bytes processed so far.
  virtual uint64_t getCompletedBytes() = 0;

  /// Returns the number of input rows processed so far.
  virtual uint64_t getCompletedRows() = 0;

  virtual std::unordered_map<std::string, RuntimeCounter> runtimeStats() = 0;

  /// Returns true if 'this' has initiated all the prefetch this will initiate.
  /// This means that the caller should schedule next splits to prefetch in the
  /// background. false if the source does not prefetch.
  virtual bool allPrefetchIssued() const {
    return false;
  }

  /// Initializes this from 'source'. 'source' is effectively moved into 'this'
  /// Adaptation like dynamic filters stay in effect but the parts dealing with
  /// open files, prefetched data etc. are moved. 'source' is freed after the
  /// move.
  virtual void setFromDataSource(std::unique_ptr<DataSource> /*source*/) {
    VELOX_UNSUPPORTED("setFromDataSource");
  }

  /// Returns a connector dependent row size if available. This can be
  /// called after addSplit().  This estimates uncompressed data
  /// sizes. This is better than getCompletedBytes()/getCompletedRows()
  /// since these track sizes before decompression and may include
  /// read-ahead and extra IO from coalescing reads and  will not
  /// fully account for size of sparsely accessed columns.
  virtual int64_t estimatedRowSize() {
    return kUnknownRowSize;
  }

  /// Returns a Wave delegate that implements the Wave Operator
  /// interface for a GPU table scan. This should be called after
  /// construction and no other methods should be called on 'this'
  /// after creating the delegate. Splits, dynamic filters etc.  will
  /// be added to the WaveDataSource instead of 'this'. 'this' should
  /// stay live until after the destruction of the delegate.
  virtual std::shared_ptr<wave::WaveDataSource> toWaveDataSource() {
    VELOX_UNSUPPORTED();
  }

  /// Invoked by table scan close to cancel any inflight async operations
  /// running inside the data source. This is the best effort and the actual
  /// connector implementation decides how to support the cancellation if
  /// needed.
  virtual void cancel() {}
};

class IndexSource {
 public:
  virtual ~IndexSource() = default;

  /// Represents a lookup request for a given input.
  struct LookupRequest {
    /// Contains the input column vectors used by lookup join and range
    /// conditions.
    RowVectorPtr input;

    explicit LookupRequest(RowVectorPtr input) : input(std::move(input)) {}
  };

  /// Represents the lookup result for a subset of input produced by the
  /// 'LookupResultIterator'.
  struct LookupResult {
    /// Specifies the indices of input row in the lookup request that have
    /// matches in 'output'. It contains the input indices in the order
    /// of the input rows in the lookup request. Any gap in the indices means
    /// the input rows that has no matches in output.
    ///
    /// Example:
    ///   LookupRequest: input = [0, 1, 2, 3, 4]
    ///   LookupResult:  inputHits = [0, 0, 2, 2, 3, 4, 4, 4]
    ///                  output    = [0, 1, 2, 3, 4, 5, 6, 7]
    ///
    ///   Here is match results for each input row:
    ///   input row #0: match with output rows #0 and #1.
    ///   input row #1: no matches
    ///   input row #2: match with output rows #2 and #3.
    ///   input row #3: match with output row #4.
    ///   input row #4: match with output rows #5, #6 and #7.
    ///
    /// 'LookupResultIterator' must also produce the output result in order of
    /// input rows.
    BufferPtr inputHits;

    /// Contains the lookup result rows.
    RowVectorPtr output;

    size_t size() const {
      return output->size();
    }

    LookupResult(BufferPtr _inputHits, RowVectorPtr _output)
        : inputHits(std::move(_inputHits)), output(std::move(_output)) {
      VELOX_CHECK_EQ(inputHits->size() / sizeof(vector_size_t), output->size());
    }
  };

  /// The lookup result iterator used to fetch the lookup result in batch for a
  /// given lookup request.
  class LookupResultIterator {
   public:
    virtual ~LookupResultIterator() = default;

    /// Invoked to fetch up to 'size' number of output rows. Returns nullptr if
    /// all the lookup results have been fetched. Returns std::nullopt and sets
    /// the 'future' if started asynchronous work and needs to wait for it to
    /// complete to continue processing. The caller will wait for the 'future'
    /// to complete before calling 'next' again.
    virtual std::optional<std::unique_ptr<LookupResult>> next(
        vector_size_t size,
        velox::ContinueFuture& future) {
      VELOX_UNSUPPORTED();
    }
  };

  virtual std::shared_ptr<LookupResultIterator> lookup(
      const LookupRequest& request) = 0;

  virtual std::unordered_map<std::string, RuntimeMetric> runtimeStats() = 0;
};

/// Collection of context data for use in a DataSource, IndexSource or DataSink.
/// One instance of this per DataSource and DataSink. This may be passed between
/// threads but methods must be invoked sequentially. Serializing use is the
/// responsibility of the caller.
class ConnectorQueryCtx {
 public:
  ConnectorQueryCtx(
      memory::MemoryPool* operatorPool,
      memory::MemoryPool* connectorPool,
      const config::ConfigBase* sessionProperties,
      const common::SpillConfig* spillConfig,
      common::PrefixSortConfig prefixSortConfig,
      std::unique_ptr<core::ExpressionEvaluator> expressionEvaluator,
      cache::AsyncDataCache* cache,
      const std::string& queryId,
      const std::string& taskId,
      const std::string& planNodeId,
      int driverId,
      const std::string& sessionTimezone,
      bool adjustTimestampToTimezone = false,
      folly::CancellationToken cancellationToken = {})
      : operatorPool_(operatorPool),
        connectorPool_(connectorPool),
        sessionProperties_(sessionProperties),
        spillConfig_(spillConfig),
        prefixSortConfig_(prefixSortConfig),
        expressionEvaluator_(std::move(expressionEvaluator)),
        cache_(cache),
        scanId_(fmt::format("{}.{}", taskId, planNodeId)),
        queryId_(queryId),
        taskId_(taskId),
        driverId_(driverId),
        planNodeId_(planNodeId),
        sessionTimezone_(sessionTimezone),
        adjustTimestampToTimezone_(adjustTimestampToTimezone),
        cancellationToken_(std::move(cancellationToken)) {
    VELOX_CHECK_NOT_NULL(sessionProperties);
  }

  /// Returns the associated operator's memory pool which is a leaf kind of
  /// memory pool, used for direct memory allocation use.
  memory::MemoryPool* memoryPool() const {
    return operatorPool_;
  }

  /// Returns the connector's memory pool which is an aggregate kind of
  /// memory pool, used for the data sink for table write that needs the
  /// hierarchical memory pool management, such as HiveDataSink.
  memory::MemoryPool* connectorMemoryPool() const {
    return connectorPool_;
  }

  const config::ConfigBase* sessionProperties() const {
    return sessionProperties_;
  }

  const common::SpillConfig* spillConfig() const {
    return spillConfig_;
  }

  const common::PrefixSortConfig& prefixSortConfig() const {
    return prefixSortConfig_;
  }

  core::ExpressionEvaluator* expressionEvaluator() const {
    return expressionEvaluator_.get();
  }

  cache::AsyncDataCache* cache() const {
    return cache_;
  }

  /// This is a combination of task id and the scan's PlanNodeId. This is an
  /// id that allows sharing state between different threads of the same
  /// scan. This is used for locating a scanTracker, which tracks the read
  /// density of columns for prefetch and other memory hierarchy purposes.
  const std::string& scanId() const {
    return scanId_;
  }

  const std::string queryId() const {
    return queryId_;
  }

  const std::string& taskId() const {
    return taskId_;
  }

  int driverId() const {
    return driverId_;
  }

  const std::string& planNodeId() const {
    return planNodeId_;
  }

  /// Session timezone used for reading Timestamp. Stores a string with the
  /// actual timezone name. If the session timezone is not set in the
  /// QueryConfig, it will return an empty string.
  const std::string& sessionTimezone() const {
    return sessionTimezone_;
  }

  /// Whether to adjust Timestamp to the timeZone obtained through
  /// sessionTimezone(). This is used to be compatible with the
  /// old logic of Presto.
  bool adjustTimestampToTimezone() const {
    return adjustTimestampToTimezone_;
  }

  /// Returns the cancellation token associated with this task.
  const folly::CancellationToken& cancellationToken() const {
    return cancellationToken_;
  }

  bool selectiveNimbleReaderEnabled() const {
    return selectiveNimbleReaderEnabled_;
  }

  void setSelectiveNimbleReaderEnabled(bool value) {
    selectiveNimbleReaderEnabled_ = value;
  }

 private:
  memory::MemoryPool* const operatorPool_;
  memory::MemoryPool* const connectorPool_;
  const config::ConfigBase* const sessionProperties_;
  const common::SpillConfig* const spillConfig_;
  const common::PrefixSortConfig prefixSortConfig_;
  const std::unique_ptr<core::ExpressionEvaluator> expressionEvaluator_;
  cache::AsyncDataCache* cache_;
  const std::string scanId_;
  const std::string queryId_;
  const std::string taskId_;
  const int driverId_;
  const std::string planNodeId_;
  const std::string sessionTimezone_;
  const bool adjustTimestampToTimezone_;
  const folly::CancellationToken cancellationToken_;
  bool selectiveNimbleReaderEnabled_{false};
};

class ConnectorMetadata;

class Connector {
 public:
  explicit Connector(const std::string& id) : id_(id) {}

  virtual ~Connector() = default;

  const std::string& connectorId() const {
    return id_;
  }

  virtual const std::shared_ptr<const config::ConfigBase>& connectorConfig()
      const {
    VELOX_NYI("connectorConfig is not supported yet");
  }

  /// Returns true if this connector would accept a filter dynamically
  /// generated during query execution.
  virtual bool canAddDynamicFilter() const {
    return false;
  }

  /// Returns a ConnectorMetadata for accessing table
  /// information.
  virtual ConnectorMetadata* metadata() const {
    VELOX_UNSUPPORTED();
  }

  virtual std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) = 0;

  /// Returns true if addSplit of DataSource can use 'dataSource' from
  /// ConnectorSplit in addSplit(). If so, TableScan can preload splits
  /// so that file opening and metadata operations are off the Driver'
  /// thread.
  virtual bool supportsSplitPreload() {
    return false;
  }

  /// Returns true if the connector supports index lookup, otherwise false.
  virtual bool supportsIndexLookup() const {
    return false;
  }

  /// Creates index source for index join lookup.
  /// @param inputType The list of probe-side columns that either used in
  /// equi-clauses or join conditions.
  /// @param numJoinKeys The number of key columns used in join equi-clauses.
  /// The first 'numJoinKeys' columns in 'inputType' form a prefix of the
  /// index, and the rest of the columns in inputType are expected to be used in
  /// 'joinConditions'.
  /// @param joinConditions The join conditions. It expects inputs columns from
  /// the 'tail' of 'inputType' and from 'columnHandles'.
  /// @param outputType The lookup output type from index source.
  /// @param tableHandle The index table handle.
  /// @param columnHandles The column handles which maps from column name
  /// used in 'outputType' and 'joinConditions' to the corresponding column
  /// handles in the index table.
  /// @param connectorQueryCtx The query context.
  ///
  /// Here is an example that how the lookup join operator uses index source:
  ///
  /// SELECT t.sid, t.day_ts, u.event_value
  /// FROM t LEFT JOIN u
  /// ON t.sid = u.sid
  ///  AND contains(t.event_list, u.event_type)
  ///  AND t.ds BETWEEN '2024-01-01' AND '2024-01-07'
  ///
  /// Here,
  /// - 'inputType' is ROW{t.sid, t.event_list}
  /// - 'numJoinKeys' is 1 since only t.sid is used in join equi-clauses.
  /// - 'joinConditions' specifies the join condition: contains(t.event_list,
  ///   u.event_type)
  /// - 'outputType' is ROW{u.event_value}
  /// - 'tableHandle' specifies the metadata of the index table.
  /// - 'columnHandles' is a map from 'u.event_type' (in 'joinConditions') and
  ///   'u.event_value' (in 'outputType') to the actual column names in the
  ///   index table.
  /// - 'connectorQueryCtx' provide the connector query execution context.
  ///
  virtual std::shared_ptr<IndexSource> createIndexSource(
      const RowTypePtr& inputType,
      size_t numJoinKeys,
      const std::vector<std::shared_ptr<core::IndexLookupCondition>>&
          joinConditions,
      const RowTypePtr& outputType,
      const std::shared_ptr<ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) {
    VELOX_UNSUPPORTED(
        "Connector {} does not support index source", connectorId());
  }

  virtual std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) = 0;

  /// Returns a ScanTracker for 'id'. 'id' uniquely identifies the
  /// tracker and different threads will share the same
  /// instance. 'loadQuantum' is the largest single IO for the query
  /// being tracked.
  static std::shared_ptr<cache::ScanTracker> getTracker(
      const std::string& scanId,
      int32_t loadQuantum);

  virtual folly::Executor* executor() const {
    return nullptr;
  }

 private:
  static void unregisterTracker(cache::ScanTracker* tracker);

  const std::string id_;

  static folly::Synchronized<
      std::unordered_map<std::string_view, std::weak_ptr<cache::ScanTracker>>>
      trackers_;
};

class ConnectorFactory {
 public:
  explicit ConnectorFactory(const char* name) : name_(name) {}

  virtual ~ConnectorFactory() = default;

  const std::string& connectorName() const {
    return name_;
  }

  virtual std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) = 0;

 private:
  const std::string name_;
};

/// Adds a factory for creating connectors to the registry using connector
/// name as the key. Throws if factor with the same name is already present.
/// Always returns true. The return value makes it easy to use with
/// FB_ANONYMOUS_VARIABLE.
bool registerConnectorFactory(std::shared_ptr<ConnectorFactory> factory);

/// Returns true if a connector with the specified name has been registered,
/// false otherwise.
bool hasConnectorFactory(const std::string& connectorName);

/// Unregister a connector factory by name.
/// Returns true if a connector with the specified name has been
/// unregistered, false otherwise.
bool unregisterConnectorFactory(const std::string& connectorName);

/// Returns a factory for creating connectors with the specified name.
/// Throws if factory doesn't exist.
std::shared_ptr<ConnectorFactory> getConnectorFactory(
    const std::string& connectorName);

/// Adds connector instance to the registry using connector ID as the key.
/// Throws if connector with the same ID is already present. Always returns
/// true. The return value makes it easy to use with FB_ANONYMOUS_VARIABLE.
bool registerConnector(std::shared_ptr<Connector> connector);

/// Removes the connector with specified ID from the registry. Returns true
/// if connector was removed and false if connector didn't exist.
bool unregisterConnector(const std::string& connectorId);

/// Returns a connector with specified ID. Throws if connector doesn't
/// exist.
std::shared_ptr<Connector> getConnector(const std::string& connectorId);

/// Returns a map of all (connectorId -> connector) pairs currently
/// registered.
const std::unordered_map<std::string, std::shared_ptr<Connector>>&
getAllConnectors();

} // namespace facebook::velox::connector

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
#include "velox/common/caching/ScanTracker.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/core/Context.h"
#include "velox/vector/ComplexVector.h"

#include <folly/Synchronized.h>

namespace facebook::velox::common {
class Filter;
}
namespace facebook::velox::core {
class ITypedExpr;
}
namespace facebook::velox::exec {
class ExprSet;
}
namespace facebook::velox::connector {

// A split represents a chunk of data that a connector should load and return
// as a RowVectorPtr, potentially after processing pushdowns.
struct ConnectorSplit {
  const std::string connectorId;

  // true if the Task processing this has aborted. Allows aborting
  // async prefetch for the split.
  bool cancelled{false};

  explicit ConnectorSplit(const std::string& _connectorId)
      : connectorId(_connectorId) {}

  virtual ~ConnectorSplit() {}

  virtual std::string toString() const {
    return fmt::format("[split: {}]", connectorId);
  }
};

class ColumnHandle {
 public:
  virtual ~ColumnHandle() = default;
};

class ConnectorTableHandle {
 public:
  explicit ConnectorTableHandle(std::string connectorId)
      : connectorId_(std::move(connectorId)) {}

  virtual ~ConnectorTableHandle() = default;

  virtual std::string toString() const = 0;

  const std::string& connectorId() const {
    return connectorId_;
  }

 private:
  const std::string connectorId_;
};

/**
 * Represents a request for writing to connector
 */
class ConnectorInsertTableHandle {
 public:
  virtual ~ConnectorInsertTableHandle() {}

  // Whether multi-threaded write is supported by this connector. Planner uses
  // this flag to determine number of drivers.
  virtual bool supportsMultiThreading() const {
    return false;
  }
};

class DataSink {
 public:
  virtual ~DataSink() = default;

  // Add the next data (vector) to be written. This call is blocking
  // TODO maybe at some point we want to make it async
  virtual void appendData(VectorPtr input) = 0;

  virtual void close() = 0;
};

class DataSource {
 public:
  static constexpr int64_t kUnknownRowSize = -1;
  virtual ~DataSource() = default;

  // Add split to process, then call next multiple times to process the split.
  // A split must be fully processed by next before another split can be
  // added. Next returns nullptr to indicate that current split is fully
  // processed.
  virtual void addSplit(std::shared_ptr<ConnectorSplit> split) = 0;

  // Process a split added via addSplit. Returns nullptr if split has been fully
  // processed. Returns std::nullopt and sets the 'future' if started
  // asynchronous work and needs to wait for it to complete to continue
  // processing. The caller will wait for the 'future' to complete before
  // calling 'next' again.
  virtual std::optional<RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) = 0;

  // Add dynamically generated filter.
  // @param outputChannel index into outputType specified in
  // Connector::createDataSource() that identifies the column this filter
  // applies to.
  virtual void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) = 0;

  // Returns the number of input bytes processed so far.
  virtual uint64_t getCompletedBytes() = 0;

  // Returns the number of input rows processed so far.
  virtual uint64_t getCompletedRows() = 0;

  virtual std::unordered_map<std::string, RuntimeCounter> runtimeStats() = 0;

  // Returns a connector dependent row size if available. This can be
  // called after addSplit().  This estimates uncompressed data
  // sizes. This is better than getCompletedBytes()/getCompletedRows()
  // since these track sizes before decompression and may include
  // read-ahead and extra IO from coalescing reads and  will not
  // fully account for size of sparsely accessed columns.
  virtual int64_t estimatedRowSize() {
    return kUnknownRowSize;
  }
};

// Exposes expression evaluation functionality of the engine to the connector.
// Connector may use it, for example, to evaluate pushed down filters.
class ExpressionEvaluator {
 public:
  virtual ~ExpressionEvaluator() = default;

  // Compiles an expression. Returns an instance of exec::ExprSet that can be
  // used to evaluate that expression on multiple vectors using evaluate method.
  virtual std::unique_ptr<exec::ExprSet> compile(
      const std::shared_ptr<const core::ITypedExpr>& expression) const = 0;

  // Evaluates previously compiled expression on the specified rows.
  // Re-uses result vector if it is not null.
  virtual void evaluate(
      exec::ExprSet* exprSet,
      const SelectivityVector& rows,
      RowVectorPtr& input,
      VectorPtr* result) const = 0;
};

class ConnectorQueryCtx {
 public:
  ConnectorQueryCtx(
      memory::MemoryPool* pool,
      Config* config,
      ExpressionEvaluator* expressionEvaluator,
      memory::MappedMemory* mappedMemory,
      const std::string& scanId)
      : pool_(pool),
        config_(config),
        expressionEvaluator_(expressionEvaluator),
        mappedMemory_(mappedMemory),
        scanId_(scanId) {}

  memory::MemoryPool* memoryPool() const {
    return pool_;
  }

  Config* config() const {
    return config_;
  }

  ExpressionEvaluator* expressionEvaluator() const {
    return expressionEvaluator_;
  }

  // MappedMemory for large allocations. Used for caching with
  // CachedBufferedImput if this implements cache::AsyncDataCache.
  memory::MappedMemory* mappedMemory() const {
    return mappedMemory_;
  }

  // Returns an id that allows sharing state between different threads
  // of the same scan. This is typically a query id plus the scan's
  // PlanNodeId. This is used for locating a scanTracker, which tracks
  // the read density of columns for prefetch and other memory
  // hierarchy purposes.
  const std::string& scanId() const {
    return scanId_;
  }

 private:
  memory::MemoryPool* pool_;
  Config* config_;
  ExpressionEvaluator* expressionEvaluator_;
  memory::MappedMemory* mappedMemory_;
  std::string scanId_;
};

class Connector {
 public:
  explicit Connector(
      const std::string& id,
      std::shared_ptr<const Config> properties)
      : id_(id), properties_(std::move(properties)) {}

  virtual ~Connector() = default;

  const std::string& connectorId() const {
    return id_;
  }

  const std::shared_ptr<const Config>& connectorProperties() const {
    return properties_;
  }

  // Returns true if this connector would accept a filter dynamically generated
  // during query execution.
  virtual bool canAddDynamicFilter() const {
    return false;
  }

  virtual std::shared_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) = 0;

  virtual std::shared_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx) = 0;

  // Returns a ScanTracker for 'id'. 'id' uniquely identifies the
  // tracker and different threads will share the same
  // instance. 'loadQuantum' is the largest single IO for the query
  // being tracked.
  static std::shared_ptr<cache::ScanTracker> getTracker(
      const std::string& scanId,
      int32_t loadQuantum);

 private:
  static void unregisterTracker(cache::ScanTracker* tracker);

  const std::string id_;

  static folly::Synchronized<
      std::unordered_map<std::string_view, std::weak_ptr<cache::ScanTracker>>>
      trackers_;

  const std::shared_ptr<const Config> properties_;
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
      std::shared_ptr<const Config> properties,
      folly::Executor* executor = nullptr) = 0;

 private:
  const std::string name_;
};

/// Adds a factory for creating connectors to the registry using connector name
/// as the key. Throws if factor with the same name is already present. Always
/// returns true. The return value makes it easy to use with
/// FB_ANONYMOUS_VARIABLE.
bool registerConnectorFactory(std::shared_ptr<ConnectorFactory> factory);

/// Returns a factory for creating connectors with the specified name. Throws if
/// factory doesn't exist.
std::shared_ptr<ConnectorFactory> getConnectorFactory(
    const std::string& connectorName);

/// Adds connector instance to the registry using connector ID as the key.
/// Throws if connector with the same ID is already present. Always returns
/// true. The return value makes it easy to use with FB_ANONYMOUS_VARIABLE.
bool registerConnector(std::shared_ptr<Connector> connector);

/// Removes the connector with specified ID from the registry. Returns true if
/// connector was removed and false if connector didn't exist.
bool unregisterConnector(const std::string& connectorId);

/// Returns a connector with specified ID. Throws if connector doesn't exist.
std::shared_ptr<Connector> getConnector(const std::string& connectorId);

#define VELOX_REGISTER_CONNECTOR_FACTORY(theFactory)                      \
  namespace {                                                             \
  static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) =                 \
      facebook::velox::connector::registerConnectorFactory((theFactory)); \
  }
} // namespace facebook::velox::connector

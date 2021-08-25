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

#include "velox/common/caching/DataCache.h"
#include "velox/core/Context.h"
#include "velox/vector/ComplexVector.h"

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
  virtual ~ConnectorTableHandle() = default;
};

/**
 * Represents a request for writing to connector
 */
class ConnectorInsertTableHandle {
 public:
  virtual ~ConnectorInsertTableHandle() {}
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
  virtual ~DataSource() = default;

  // Add split to process, then call next multiple times to process the split.
  // A split must be fully processed by next before another split can be
  // added. Next returns nullptr to indicate that current split is fully
  // processed.
  virtual void addSplit(std::shared_ptr<ConnectorSplit> split) = 0;

  // Process a split added via addSplit. Returns nullptr if split has been fully
  // processed.
  virtual RowVectorPtr next(uint64_t size) = 0;

  // Returns the number of input bytes processed so far.
  virtual uint64_t getCompletedBytes() = 0;

  // Returns the number of input rows processed so far.
  virtual uint64_t getCompletedRows() = 0;

  virtual std::unordered_map<std::string, int64_t> runtimeStats() = 0;

  // TODO Allow DataSource to indicate that it is blocked (say waiting for IO)
  // to avoid holding up the thread.
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
      ExpressionEvaluator* expressionEvaluator)
      : pool_(pool),
        config_(config),
        expressionEvaluator_(expressionEvaluator) {}

  memory::MemoryPool* memoryPool() const {
    return pool_;
  }

  Config* config() const {
    return config_;
  }

  ExpressionEvaluator* expressionEvaluator() const {
    return expressionEvaluator_;
  }

 private:
  memory::MemoryPool* pool_;
  Config* config_;
  ExpressionEvaluator* expressionEvaluator_;
};

class Connector {
 public:
  explicit Connector(const std::string& id) : id_(id) {}

  virtual ~Connector() = default;

  const std::string& connectorId() const {
    return id_;
  }

  // TODO Generalize to specify TableHandle/Layout and ColumnHandles.
  // We should basically aim to match the interface defined at
  // https://github.com/prestodb/presto/blob/master/presto-main/src/main/
  //      java/com/facebook/presto/split/PageSourceProvider.java
  // except that the split should NOT be part of the signature.
  virtual std::shared_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) = 0;

  virtual std::shared_ptr<DataSink> createDataSink(
      std::shared_ptr<const RowType> inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx) = 0;

 private:
  const std::string id_;
};

class ConnectorFactory {
 public:
  explicit ConnectorFactory(const std::string& name) : name_(name) {}

  virtual ~ConnectorFactory() = default;

  const std::string& connectorName() const {
    return name_;
  }

  virtual std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::unique_ptr<DataCache> dataCache = nullptr) = 0;

 private:
  const std::string name_;
};

bool registerConnectorFactory(std::shared_ptr<ConnectorFactory> factory);

std::shared_ptr<ConnectorFactory> getConnectorFactory(
    const std::string& connectorName);

bool registerConnector(std::shared_ptr<Connector> connector);

bool unregisterConnector(const std::string& connectorId);

std::shared_ptr<Connector> getConnector(const std::string& connectorId);

#define VELOX_REGISTER_CONNECTOR_FACTORY(theFactory)                      \
  namespace {                                                             \
  static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) =                 \
      facebook::velox::connector::registerConnectorFactory((theFactory)); \
  }
} // namespace facebook::velox::connector

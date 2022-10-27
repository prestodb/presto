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

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector {

class ConnectorInsertTableHandle;
class ConnectorQueryCtx;

/// Interface to provide key parameters for writers. Ex., write and commit
/// locations, append or overwrite, etc.
class WriterParameters {
 public:
  virtual ~WriterParameters() = default;
};

/// Interface that includes all info from write. It will be passed to commit()
/// of WriteProtocol.
class WriteInfo {
 public:
  virtual ~WriteInfo() = default;

  virtual vector_size_t numWrittenRows() const = 0;
};

/// Abstraction for write behaviors. Systems register WriteProtocols
/// by CommitStrategy. Writers call getWriteProtocol() to get the registered
/// instance of the WriteProtocol when needed.
class WriteProtocol {
 public:
  /// Represents the commit strategy of a write protocol.
  enum class CommitStrategy {
    kNoCommit, // No more commit actions are needed.
    kTaskCommit // Task level commit is needed.
  };

  virtual ~WriteProtocol() {}

  /// Return the commit strategy of the write protocol. It will be the commit
  /// strategy that the write protocol registers for.
  virtual CommitStrategy commitStrategy() const = 0;

  /// Return a string encoding of the given commit strategy.
  static std::string commitStrategyToString(CommitStrategy commitStrategy) {
    switch (commitStrategy) {
      case CommitStrategy::kNoCommit:
        return "NO_COMMIT";
      case CommitStrategy::kTaskCommit:
        return "TASK_COMMIT";
      default:
        VELOX_UNREACHABLE();
    }
  }

  /// Perform actions of commit. It would be called by the writers and could
  /// return outputs that would be included in writer outputs. Return nullptr if
  /// the commit action does not need to add output to the table writer output.
  virtual RowVectorPtr commit(
      const WriteInfo& writeInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) {
    return nullptr;
  }

  /// Return parameters for writers. Ex., write and commit locations. Return
  /// nullptr if the writer does not need parameters from the write protocol.
  virtual std::shared_ptr<const WriterParameters> getWriterParameters(
      const std::shared_ptr<const velox::connector::ConnectorInsertTableHandle>&
          tableHandle,
      const velox::connector::ConnectorQueryCtx* FOLLY_NONNULL
          connectorQueryCtx) const = 0;

  /// Register a WriteProtocol implementation for the given CommitStrategy. If
  /// the CommitStrategy has already been registered, it will replace the old
  /// WriteProtocol implementation with the new one and return false; otherwise
  /// return true.
  static bool registerWriteProtocol(
      CommitStrategy commitStrategy,
      std::shared_ptr<WriteProtocol> writeProtocol);

  /// Return the instance of the WriteProtocol registered for
  /// the given CommitStrategy.
  static std::shared_ptr<WriteProtocol> getWriteProtocol(
      CommitStrategy commitStrategy);
};

class DefaultWriteProtocol : public WriteProtocol {
 public:
  ~DefaultWriteProtocol() override {}

  CommitStrategy commitStrategy() const override {
    return CommitStrategy::kNoCommit;
  }

  RowVectorPtr commit(
      const WriteInfo& writeInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override;

  std::shared_ptr<const WriterParameters> getWriterParameters(
      const std::shared_ptr<const velox::connector::ConnectorInsertTableHandle>&
          tableHandle,
      const velox::connector::ConnectorQueryCtx* FOLLY_NONNULL
          connectorQueryCtx) const override {
    return std::make_shared<WriterParameters>();
  }
};

} // namespace facebook::velox::connector

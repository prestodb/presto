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

#include "velox/connectors/hive/HiveWriteProtocol.h"

namespace facebook::velox::exec {
class TableWriterWriteInfo;
}
namespace facebook::presto::protocol {

class PrestoNoCommitWriteProtocol
    : public velox::connector::hive::HiveNoCommitWriteProtocol {
 public:
  ~PrestoNoCommitWriteProtocol() {}

  velox::RowVectorPtr commit(
      const velox::connector::WriteInfo& writeInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override;

  static void registerProtocol() {
    registerWriteProtocol(
        velox::connector::WriteProtocol::CommitStrategy::kNoCommit,
        []() { return std::make_shared<PrestoNoCommitWriteProtocol>(); });
  }

 private:
  velox::RowVectorPtr commit(
      const velox::exec::TableWriterWriteInfo& tableWriterOperator,
      velox::memory::MemoryPool* FOLLY_NONNULL pool);
};

class PrestoTaskCommitWriteProtocol
    : public velox::connector::hive::HiveTaskCommitWriteProtocol {
 public:
  ~PrestoTaskCommitWriteProtocol() {}

  velox::RowVectorPtr commit(
      const velox::connector::WriteInfo& writeInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override;

  static void registerProtocol() {
    registerWriteProtocol(
        velox::connector::WriteProtocol::CommitStrategy::kTaskCommit,
        []() { return std::make_shared<PrestoTaskCommitWriteProtocol>(); });
  }

 private:
  velox::RowVectorPtr commit(
      const velox::exec::TableWriterWriteInfo& tableWriterOperator,
      velox::memory::MemoryPool* FOLLY_NONNULL pool);
};

} // namespace facebook::presto::protocol
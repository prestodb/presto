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

namespace facebook::velox::connector {
class CommitInfo;
}
namespace facebook::presto::protocol {

class HiveNoCommitWriteProtocol
    : public velox::connector::hive::HiveNoCommitWriteProtocol {
 public:
  ~HiveNoCommitWriteProtocol() override = default;

  velox::RowVectorPtr commit(
      const velox::connector::CommitInfo& commitInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override;

  static void registerProtocol() {
    registerWriteProtocol(
        CommitStrategy::kNoCommit,
        std::make_shared<HiveNoCommitWriteProtocol>());
  }
};

class HiveTaskCommitWriteProtocol
    : public velox::connector::hive::HiveTaskCommitWriteProtocol {
 public:
  ~HiveTaskCommitWriteProtocol() override = default;

  velox::RowVectorPtr commit(
      const velox::connector::CommitInfo& commitInfo,
      velox::memory::MemoryPool* FOLLY_NONNULL pool) override;

  static void registerProtocol() {
    registerWriteProtocol(
        CommitStrategy::kTaskCommit,
        std::make_shared<HiveTaskCommitWriteProtocol>());
  }
};

} // namespace facebook::presto::protocol
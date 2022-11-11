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

#include "velox/connectors/WriteProtocol.h"
#include "velox/connectors/Connector.h"
#include "velox/vector/ConstantVector.h"

#include <unordered_map>

namespace facebook::velox::connector {

namespace {

using RegisteredWriteProtocols = std::unordered_map<
    WriteProtocol::CommitStrategy,
    std::shared_ptr<WriteProtocol>>;

RegisteredWriteProtocols& registeredWriteProtocols() {
  // Default write protocol registered
  static RegisteredWriteProtocols protocols{
      {WriteProtocol::CommitStrategy::kNoCommit,
       std::make_shared<DefaultWriteProtocol>()}};
  return protocols;
}

} // namespace

// static
bool WriteProtocol::registerWriteProtocol(
    WriteProtocol::CommitStrategy commitStrategy,
    std::shared_ptr<WriteProtocol> writeProtocol) {
  return registeredWriteProtocols()
      .insert_or_assign(commitStrategy, writeProtocol)
      .second;
}

// static
std::shared_ptr<WriteProtocol> WriteProtocol::getWriteProtocol(
    CommitStrategy commitStrategy) {
  const auto iter = registeredWriteProtocols().find(commitStrategy);
  // Fail if no WriteProtocol has been registered for the given CommitStrategy.
  VELOX_CHECK(
      iter != registeredWriteProtocols().end(),
      "No write protocol found for commit strategy {}",
      commitStrategyToString(commitStrategy));
  return iter->second;
}

RowVectorPtr DefaultWriteProtocol::commit(
    const CommitInfo& commitInfo,
    velox::memory::MemoryPool* FOLLY_NONNULL pool) {
  return std::make_shared<RowVector>(
      pool,
      ROW({"rowCount"}, {BIGINT()}),
      BufferPtr(nullptr),
      1,
      std::vector<VectorPtr>{std::make_shared<ConstantVector<int64_t>>(
          pool, 1, false, BIGINT(), commitInfo.numWrittenRows())});
}

} // namespace facebook::velox::connector

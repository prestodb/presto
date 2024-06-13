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

#include "velox/connectors/Connector.h"

namespace facebook::velox::exec {

struct Split {
  std::shared_ptr<velox::connector::ConnectorSplit> connectorSplit;
  int32_t groupId{-1}; // Bucketed group id (-1 means 'none').

  Split() = default;

  explicit Split(
      std::shared_ptr<velox::connector::ConnectorSplit>&& connectorSplit,
      int32_t groupId = -1)
      : connectorSplit(std::move(connectorSplit)), groupId(groupId) {}

  Split(Split&& other) = default;
  Split(const Split& other) = default;
  Split& operator=(Split&& other) = default;
  Split& operator=(const Split& other) = default;

  inline bool hasConnectorSplit() const {
    return connectorSplit != nullptr;
  }

  inline bool hasGroup() const {
    return groupId != -1;
  }

  std::string toString() const {
    return fmt::format(
        "Split: [{}] {}",
        hasConnectorSplit() ? connectorSplit->toString() : "NULL",
        groupId);
  }
};

} // namespace facebook::velox::exec

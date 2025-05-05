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
  std::shared_ptr<velox::connector::ConnectorSplit> connectorSplit{nullptr};
  int32_t groupId{-1}; // Bucketed group id (-1 means 'none').

  /// Indicates if this is a barrier split. A barrier split is used by task
  /// barrier processing which adds one barrier split to each leaf source node
  /// to signal the output drain processing.
  bool barrier{false};

  Split() = default;

  explicit Split(
      std::shared_ptr<velox::connector::ConnectorSplit>&& connectorSplit,
      int32_t groupId = -1)
      : connectorSplit(std::move(connectorSplit)), groupId(groupId) {}

  /// Called by the task barrier to create a special barrier split.
  static Split createBarrier() {
    static Split barrierSplit;
    barrierSplit.barrier = true;
    return barrierSplit;
  }

  Split(Split&& other) = default;
  Split(const Split& other) = default;
  Split& operator=(Split&& other) = default;
  Split& operator=(const Split& other) = default;

  bool isBarrier() const {
    return barrier;
  }

  inline bool hasConnectorSplit() const {
    return connectorSplit != nullptr;
  }

  inline bool hasGroup() const {
    return groupId != -1;
  }

  std::string toString() const {
    if (barrier) {
      return "BarrierSplit";
    } else {
      return fmt::format(
          "Split: [{}] {}",
          hasConnectorSplit() ? connectorSplit->toString() : "NULL",
          groupId);
    }
  }
};

} // namespace facebook::velox::exec

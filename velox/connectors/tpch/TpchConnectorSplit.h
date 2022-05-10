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

namespace facebook::velox::connector::tpch {

struct TpchConnectorSplit : public connector::ConnectorSplit {
  // TODO: Placeholder for now. Need to add totalParts and partNum to follow
  // Presto's logic.
  explicit TpchConnectorSplit(const std::string& connectorId)
      : ConnectorSplit(connectorId) {}
};

} // namespace facebook::velox::connector::tpch

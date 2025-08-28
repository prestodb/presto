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

#include <fmt/format.h>
#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::tpcds {

struct TpcdsConnectorSplit : public velox::connector::ConnectorSplit {
  explicit TpcdsConnectorSplit(
      const std::string& connectorId,
      size_t totalParts,
      size_t partNumber)
      : TpcdsConnectorSplit(connectorId, true, totalParts, partNumber) {}

  TpcdsConnectorSplit(
      const std::string& connectorId,
      bool cacheable,
      size_t totalParts,
      size_t partNumber)
      : ConnectorSplit(connectorId, /*splitWeight=*/0, cacheable),
        totalParts_(totalParts),
        partNumber_(partNumber) {
    VELOX_CHECK_GE(totalParts, 1, "totalParts must be >= 1");
    VELOX_CHECK_GT(totalParts, partNumber, "totalParts must be > partNumber");
  }

  // In how many parts the generated TPC-DS table will be segmented, roughly
  // `rowCount / totalParts`
  const vector_size_t totalParts_{1};

  // Which of these parts will be read by this split.
  const vector_size_t partNumber_{0};
};

} // namespace facebook::velox::connector::tpcds

template <>
struct fmt::formatter<facebook::velox::connector::tpcds::TpcdsConnectorSplit>
    : formatter<std::string> {
  auto format(
      facebook::velox::connector::tpcds::TpcdsConnectorSplit s,
      format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<
    std::shared_ptr<facebook::velox::connector::tpcds::TpcdsConnectorSplit>>
    : formatter<std::string> {
  auto format(
      std::shared_ptr<facebook::velox::connector::tpcds::TpcdsConnectorSplit> s,
      format_context& ctx) {
    return formatter<std::string>::format(s->toString(), ctx);
  }
};

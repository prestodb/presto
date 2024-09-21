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

namespace facebook::velox::connector::fuzzer {

struct FuzzerConnectorSplit : public connector::ConnectorSplit {
  explicit FuzzerConnectorSplit(const std::string& connectorId, size_t numRows)
      : ConnectorSplit(connectorId), numRows(numRows) {}

  // Row many rows to generate.
  size_t numRows;
};

} // namespace facebook::velox::connector::fuzzer

template <>
struct fmt::formatter<facebook::velox::connector::fuzzer::FuzzerConnectorSplit>
    : formatter<std::string> {
  auto format(
      facebook::velox::connector::fuzzer::FuzzerConnectorSplit s,
      format_context& ctx) const {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

template <>
struct fmt::formatter<
    std::shared_ptr<facebook::velox::connector::fuzzer::FuzzerConnectorSplit>>
    : formatter<std::string> {
  auto format(
      std::shared_ptr<facebook::velox::connector::fuzzer::FuzzerConnectorSplit>
          s,
      format_context& ctx) const {
    return formatter<std::string>::format(s->toString(), ctx);
  }
};

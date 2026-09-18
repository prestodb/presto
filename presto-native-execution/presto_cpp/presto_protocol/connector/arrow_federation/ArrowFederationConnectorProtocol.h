/*
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
#include <folly/base64.h>
#include "presto_cpp/main/thrift/JdbcThriftToJson.h"
#include "presto_cpp/presto_protocol/connector/arrow_federation/presto_protocol_arrow_federation.h"
#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"

namespace facebook::presto::protocol::arrow_federation {
template <typename Protocol, json (*Decode)(const std::string&)>
struct JdbcFederationHandle : Protocol {
  static std::shared_ptr<JdbcFederationHandle> deserialize(
      const std::string& data,
      std::shared_ptr<JdbcFederationHandle>) {
    auto result = std::make_shared<JdbcFederationHandle>();
    from_json(Decode(data), static_cast<Protocol&>(*result));
    return result;
  }
};

using JdbcArrowFederationConnectorProtocol = ConnectorProtocolTemplate<
    JdbcFederationHandle<ArrowFederationTableHandle, jdbcTableHandleFromThrift>,
    JdbcFederationHandle<
        ArrowFederationTableLayoutHandle,
        jdbcTableLayoutHandleFromThrift>,
    JdbcFederationHandle<
        ArrowFederationColumnHandle,
        jdbcColumnHandleFromThrift>,
    UnsupportedOperation,
    UnsupportedOperation,
    JdbcFederationHandle<ArrowFederationSplit, jdbcSplitFromThrift>,
    NotImplemented,
    JdbcFederationHandle<
        ArrowFederationTransactionHandle,
        jdbcTransactionHandleFromThrift>,
    NotImplemented,
    UnsupportedOperation,
    NotImplemented,
    UnsupportedOperation>;

using ArrowFederationConnectorProtocol = ConnectorProtocolTemplate<
    ArrowFederationTableHandle,
    ArrowFederationTableLayoutHandle,
    ArrowFederationColumnHandle,
    UnsupportedOperation, // ConnectorInsertTableHandle
    UnsupportedOperation, // ConnectorOutputTableHandle (CTAS)
    ArrowFederationSplit,
    NotImplemented, // ConnectorPartitioningHandle
    ArrowFederationTransactionHandle,
    NotImplemented, // ConnectorDistributedProcedureHandle
    UnsupportedOperation, // ConnectorDeleteTableHandle
    NotImplemented, // ConnectorIndexHandle
    UnsupportedOperation>; // ConnectorMergeTableHandle
} // namespace facebook::presto::protocol::arrow_federation

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

#include "presto_cpp/presto_protocol/connector/tpch/presto_protocol_tpch.h"
#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"

namespace facebook::presto::protocol::tpch {

using TpchConnectorProtocol = ConnectorProtocolTemplate<
    TpchTableHandle,
    TpchTableLayoutHandle,
    TpchColumnHandle,
    NotImplemented,
    NotImplemented,
    TpchSplit,
    TpchPartitioningHandle,
    TpchTransactionHandle,
    NotImplemented>;

} // namespace facebook::presto::protocol::tpch

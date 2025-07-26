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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class TableFunctionHandle
{
    private final ConnectorId connectorId;
    private final ConnectorTableFunctionHandle functionHandle;
    private final ConnectorTransactionHandle transactionHandle;

    @JsonCreator
    public TableFunctionHandle(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("functionHandle") ConnectorTableFunctionHandle functionHandle,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
    }

    @JsonProperty
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorTableFunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    @JsonProperty
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }
}

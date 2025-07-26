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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class AnalyzeTableHandle
{
    private final ConnectorId connectorId;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorTableHandle connectorHandle;

    @JsonCreator
    @ThriftConstructor
    public AnalyzeTableHandle(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle,
            @JsonProperty("connectorHandle") ConnectorTableHandle connectorHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
    }

    @JsonProperty
    @ThriftField(1)
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @ThriftField(3)
    public ConnectorTableHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonProperty
    @ThriftField(2)
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnalyzeTableHandle that = (AnalyzeTableHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(transactionHandle, that.transactionHandle) &&
                Objects.equals(connectorHandle, that.connectorHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, transactionHandle, connectorHandle);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + connectorHandle + ":" + transactionHandle;
    }
}

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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.transaction.TransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class TableLayoutHandle
{
    private final String connectorId;
    private final TransactionHandle transactionHandle;
    private final ConnectorTableLayoutHandle layout;

    @JsonCreator
    public TableLayoutHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("transactionHandle") TransactionHandle transactionHandle,
            @JsonProperty("connectorHandle") ConnectorTableLayoutHandle layout)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(transactionHandle, "transactionHandle is null");
        requireNonNull(layout, "layout is null");

        this.connectorId = connectorId;
        this.transactionHandle = transactionHandle;
        this.layout = layout;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public TransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }

    @JsonProperty
    public ConnectorTableLayoutHandle getConnectorHandle()
    {
        return layout;
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
        TableLayoutHandle that = (TableLayoutHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(transactionHandle, that.transactionHandle) &&
                Objects.equals(layout, that.layout);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, transactionHandle, layout);
    }
}

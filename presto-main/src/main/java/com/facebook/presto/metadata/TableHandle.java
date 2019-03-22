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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class TableHandle
{
    private final ConnectorId connectorId;
    private final ConnectorTableHandle connectorHandle;
    private final ConnectorTransactionHandle transaction;

    private final Optional<ConnectorTableLayoutHandle> layout;

    @JsonCreator
    public TableHandle(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("connectorHandle") ConnectorTableHandle connectorHandle,
            @JsonProperty("transaction") ConnectorTransactionHandle transaction,
            @JsonProperty("connectorTableLayout") Optional<ConnectorTableLayoutHandle> layout)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.layout = requireNonNull(layout, "layout is null");
    }

    @JsonProperty
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorTableHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonProperty
    public ConnectorTransactionHandle getTransaction()
    {
        return transaction;
    }

    @JsonProperty
    public Optional<ConnectorTableLayoutHandle> getLayout()
    {
        return layout;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableHandle)) {
            return false;
        }
        TableHandle that = (TableHandle) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(connectorHandle, that.connectorHandle) &&
                Objects.equals(transaction, that.transaction) &&
                Objects.equals(layout, that.layout);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, connectorHandle, transaction, layout);
    }

    @Override
    public String toString()
    {
        return "TableHandle{" +
                "connectorId=" + connectorId +
                ", connectorHandle=" + connectorHandle +
                ", transaction=" + transaction +
                ", layout=" + layout +
                '}';
    }
}

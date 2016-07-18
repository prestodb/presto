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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitioningHandle
{
    private final Optional<String> connectorId;
    private final Optional<ConnectorTransactionHandle> transactionHandle;
    private final ConnectorPartitioningHandle connectorHandle;

    @JsonCreator
    public PartitioningHandle(
            @JsonProperty("connectorId") Optional<String> connectorId,
            @JsonProperty("transactionHandle") Optional<ConnectorTransactionHandle> transactionHandle,
            @JsonProperty("connectorHandle") ConnectorPartitioningHandle connectorHandle)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        checkArgument(!connectorId.isPresent() || transactionHandle.isPresent(), "transactionHandle is required when connectorId is present");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
    }

    @JsonProperty
    public Optional<String> getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Optional<ConnectorTransactionHandle> getTransactionHandle()
    {
        return transactionHandle;
    }

    @JsonProperty
    public ConnectorPartitioningHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    public boolean isSingleNode()
    {
        return connectorHandle.isSingleNode();
    }

    public boolean isCoordinatorOnly()
    {
        return connectorHandle.isCoordinatorOnly();
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
        PartitioningHandle that = (PartitioningHandle) o;

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
        if (connectorId.isPresent()) {
            return connectorId.get() + ":" + connectorHandle;
        }
        return connectorHandle.toString();
    }
}

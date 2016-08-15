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

import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorNewTablePartitioning;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class NewTableLayout
{
    private final String connectorId;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorNewTableLayout layout;

    @JsonCreator
    public NewTableLayout(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle,
            @JsonProperty("layout") ConnectorNewTableLayout layout)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.layout = requireNonNull(layout, "layout is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorNewTableLayout getLayout()
    {
        return layout;
    }

    public Optional<Partitioning> getNodePartitioning(Function<String, Symbol> symbolLookup)
    {
        return layout.getNodePartitioning().map(newTablePartitioning -> toPartitioning(newTablePartitioning, symbolLookup));
    }

    public Optional<Partitioning> getStreamPartitioning(Function<String, Symbol> symbolLookup)
    {
        return layout.getStreamPartitioning().map(newTablePartitioning -> toPartitioning(newTablePartitioning, symbolLookup));
    }

    private Partitioning toPartitioning(ConnectorNewTablePartitioning newTablePartitioning, Function<String, Symbol> symbolLookup)
    {
        PartitioningHandle partitioningHandle = new PartitioningHandle(Optional.of(connectorId), Optional.of(transactionHandle), newTablePartitioning.getPartitioningHandle());

        List<Symbol> partitionFunctionArguments = newTablePartitioning.getPartitioningColumns().stream()
                .map(column -> {
                    Symbol symbol = symbolLookup.apply(column);
                    // todo this should be checked in analysis
                    if (symbol == null) {
                        throw new PrestoException(NOT_SUPPORTED, "INSERT must write all partitioning columns: " + column);
                    }
                    return symbol;
                })
                .collect(Collectors.toList());

        return Partitioning.create(partitioningHandle, partitionFunctionArguments);
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

        NewTableLayout that = (NewTableLayout) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(transactionHandle, that.transactionHandle) &&
                Objects.equals(layout, that.layout);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, transactionHandle, layout);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("transactionHandle", transactionHandle)
                .add("layout", layout)
                .toString();
    }
}

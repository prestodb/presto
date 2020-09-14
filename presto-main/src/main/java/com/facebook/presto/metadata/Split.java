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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Split
{
    private final ConnectorId connectorId;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorSplit connectorSplit;
    private final Lifespan lifespan;
    private final SplitContext splitContext;

    // TODO: inline
    public Split(ConnectorId connectorId, ConnectorTransactionHandle transactionHandle, ConnectorSplit connectorSplit)
    {
        this(connectorId, transactionHandle, connectorSplit, Lifespan.taskWide(), NON_CACHEABLE);
    }

    @JsonCreator
    public Split(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit,
            @JsonProperty("lifespan") Lifespan lifespan,
            @JsonProperty("splitContext") SplitContext splitContext)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
        this.lifespan = requireNonNull(lifespan, "lifespan is null");
        this.splitContext = requireNonNull(splitContext, "splitContext is null");
    }

    @JsonProperty
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }

    @JsonProperty
    public ConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }

    @JsonProperty
    public Lifespan getLifespan()
    {
        return lifespan;
    }

    @JsonProperty
    public SplitContext getSplitContext()
    {
        return splitContext;
    }

    public Object getInfo()
    {
        return connectorSplit.getInfo();
    }

    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return connectorSplit.getPreferredNodes(sortedCandidates);
    }

    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return connectorSplit.getNodeSelectionStrategy();
    }

    public SplitIdentifier getSplitIdentifier()
    {
        return new SplitIdentifier(connectorId, connectorSplit.getSplitIdentifier());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("transactionHandle", transactionHandle)
                .add("connectorSplit", connectorSplit)
                .add("lifespan", lifespan)
                .add("splitContext", splitContext)
                .toString();
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

        Split split = (Split) o;
        return connectorId.equals(split.connectorId) &&
                transactionHandle.equals(split.transactionHandle) &&
                connectorSplit.equals(split.connectorSplit) &&
                lifespan.equals(split.lifespan);
    }

    @Override
    public int hashCode()
    {
        // Requires connectorSplit's hash function to be set up correctly
        return Objects.hash(connectorId, transactionHandle, connectorSplit, lifespan);
    }

    public static class SplitIdentifier
    {
        public final ConnectorId connectorId;
        public final Object splitIdentifier;

        public SplitIdentifier(ConnectorId connectorId, Object splitIdentifier)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.splitIdentifier = requireNonNull(splitIdentifier, "splitIdentifier is null");
        }

        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public Object getSplitIdentifier()
        {
            return splitIdentifier;
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
            SplitIdentifier that = (SplitIdentifier) o;
            return Objects.equals(connectorId, that.connectorId) &&
                    Objects.equals(splitIdentifier, that.splitIdentifier);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(connectorId, splitIdentifier);
        }
    }
}


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
package com.facebook.presto.split;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class NativeEngineSplitWrapper
        implements ConnectorSplit
{
    private final PlanNodeId sourceNodeId;
    private final ConnectorSplit connectorSplit;
    public static final String NATIVE_ENGINE_SPLIT_WRAPPER_NAME = "nativeEngineSplitWrapper";

    @JsonCreator
    public NativeEngineSplitWrapper(
            @JsonProperty("sourceNodeId") PlanNodeId sourceNodeId,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit)
    {
        this.sourceNodeId = requireNonNull(sourceNodeId, "sourceNodeId cannot be null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit cannot be null");
    }

    @JsonProperty
    public PlanNodeId getSourceNodeId()
    {
        return sourceNodeId;
    }

    @JsonProperty
    public ConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return connectorSplit.getNodeSelectionStrategy();
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return connectorSplit.getPreferredNodes(nodeProvider);
    }

    @Override
    public Object getInfo()
    {
        return this;
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
        NativeEngineSplitWrapper that = (NativeEngineSplitWrapper) o;
        return Objects.equals(sourceNodeId, that.sourceNodeId) &&
                Objects.equals(connectorSplit, that.connectorSplit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sourceNodeId, connectorSplit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sourceNodeId", sourceNodeId)
                .add("connectorSplit", connectorSplit)
                .toString();
    }

    public static Class<NativeEngineSplitWrapper> getResolverClass(String className)
    {
        return NativeEngineSplitWrapper.class;
    }

    public String getResolverName()
    {
        return NATIVE_ENGINE_SPLIT_WRAPPER_NAME;
    }
}

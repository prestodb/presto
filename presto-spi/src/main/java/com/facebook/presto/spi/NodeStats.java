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
package com.facebook.presto.spi;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Load Statistics for a node in the Presto cluster along with the state.
 */
@ThriftStruct
public class NodeStats
{
    private final NodeState nodeState;
    private final Optional<NodeLoadMetrics> loadMetrics;

    @JsonCreator
    @ThriftConstructor
    public NodeStats(
            @JsonProperty("nodeState") NodeState nodeState,
            @JsonProperty("loadMetrics") Optional<NodeLoadMetrics> loadMetrics)
    {
        this.nodeState = requireNonNull(nodeState, "nodeState is null");
        this.loadMetrics = loadMetrics;
    }

    @JsonProperty
    @ThriftField(1)
    public NodeState getNodeState()
    {
        return nodeState;
    }

    @JsonProperty
    @ThriftField(2)
    public Optional<NodeLoadMetrics> getLoadMetrics()
    {
        return loadMetrics;
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
        NodeStats nodeStats = (NodeStats) o;
        return Objects.equals(nodeState, nodeStats.nodeState) &&
                Objects.equals(loadMetrics, nodeStats.loadMetrics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeState, loadMetrics);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("NodeStats{");
        sb.append("nodeState=").append(nodeState);
        sb.append(", loadMetrics=").append(loadMetrics);
        sb.append('}');
        return sb.toString();
    }
}

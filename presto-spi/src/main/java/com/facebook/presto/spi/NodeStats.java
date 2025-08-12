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

import javax.annotation.concurrent.Immutable;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Load Statistics for a node in the Presto cluster along with the state.
 */
@Immutable
@ThriftStruct
public class NodeStats
{
    private final NodeState nodeState;
    private final Map<String, Double> metrics;

    @JsonCreator
    @ThriftConstructor
    public NodeStats(
            @JsonProperty("nodeState") NodeState nodeState,
            @JsonProperty("metrics") Map<String, Double> metrics)
    {
        this.nodeState = requireNonNull(nodeState, "nodeState is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
    }

    @JsonProperty
    @ThriftField(1)
    public NodeState getNodeState()
    {
        return nodeState;
    }

    @JsonProperty
    @ThriftField(2)
    public Map<String, Double> getMetrics()
    {
        return metrics;
    }
}

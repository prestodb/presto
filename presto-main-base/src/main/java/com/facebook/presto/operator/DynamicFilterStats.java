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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Records the dynamic filter execution stats for an operator on native worker.
 */
@ThriftStruct
public class DynamicFilterStats
{
    private final Set<PlanNodeId> producerNodeIds;

    /**
     * Creates a DynamicFilterStats.
     *
     * @param producerNodeIds The set of plan node ids that produce the dynamic filters.
     */
    @JsonCreator
    @ThriftConstructor
    public DynamicFilterStats(
            @JsonProperty("producerNodeIds") Set<PlanNodeId> producerNodeIds)
    {
        this.producerNodeIds = requireNonNull(producerNodeIds, "producerNodeIds is null");
    }

    public static DynamicFilterStats copyOf(DynamicFilterStats dynamicFilterStats)
    {
        requireNonNull(dynamicFilterStats, "dynamicFilterStats is null");
        return new DynamicFilterStats(dynamicFilterStats.getProducerNodeIds());
    }

    public void mergeWith(DynamicFilterStats other)
    {
        if (other == null) {
            return;
        }
        producerNodeIds.addAll(other.getProducerNodeIds());
    }

    public boolean empty()
    {
        return producerNodeIds.isEmpty();
    }

    @JsonProperty
    @ThriftField(1)
    public Set<PlanNodeId> getProducerNodeIds()
    {
        return producerNodeIds;
    }
}

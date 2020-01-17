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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.TypeProvider;
import io.airlift.units.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class PlanRepresentation
{
    private final PlanNode root;
    private final Optional<Duration> totalCpuTime;
    private final Optional<Duration> totalScheduledTime;
    private final TypeProvider types;

    private final Map<PlanNodeId, NodeRepresentation> nodeInfo = new HashMap<>();

    public PlanRepresentation(PlanNode root, TypeProvider types, Optional<Duration> totalCpuTime, Optional<Duration> totalScheduledTime)
    {
        this.root = requireNonNull(root, "root is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.types = requireNonNull(types, "types is null");
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
    }

    public NodeRepresentation getRoot()
    {
        return nodeInfo.get(root.getId());
    }

    public TypeProvider getTypes()
    {
        return types;
    }

    public Optional<Duration> getTotalCpuTime()
    {
        return totalCpuTime;
    }

    public Optional<Duration> getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    public Optional<NodeRepresentation> getNode(PlanNodeId id)
    {
        return Optional.ofNullable(nodeInfo.get(id));
    }

    public void addNode(NodeRepresentation node)
    {
        nodeInfo.put(node.getId(), node);
    }
}

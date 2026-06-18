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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.OptimizerResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PlanRepresentation
{
    private final PlanNode root;
    private final Optional<Duration> totalCpuTime;
    private final Optional<Duration> totalScheduledTime;
    private final TypeProvider types;

    private final Map<PlanNodeId, NodeRepresentation> nodeInfo = new HashMap<>();
    private final List<PlanOptimizerInformation> planOptimizerInfo;
    private final List<CTEInformation> cteInformationList;
    private final List<OptimizerResult> planOptimizerResults;

    public PlanRepresentation(
            PlanNode root,
            TypeProvider types,
            Optional<Duration> totalCpuTime,
            Optional<Duration> totalScheduledTime,
            List<PlanOptimizerInformation> planOptimizerInfo,
            List<CTEInformation> cteInformationList,
            List<OptimizerResult> planOptimizerResults)
    {
        this.root = requireNonNull(root, "root is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.types = requireNonNull(types, "types is null");
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.planOptimizerInfo = requireNonNull(planOptimizerInfo, "planOptimizerInfo is null");
        this.cteInformationList = requireNonNull(cteInformationList, "cteInformationList is null");
        this.planOptimizerResults = requireNonNull(planOptimizerResults, "planOptimizerResults is null");
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

    public PlanNode getPlanNodeRoot()
    {
        return root;
    }

    public void addNode(NodeRepresentation node)
    {
        NodeRepresentation previous = nodeInfo.put(node.getId(), node);
        if (previous != null) {
            throw new IllegalStateException(String.format("Duplicate node ID %s: %s vs. %s", node.getId(), previous.getName(), node.getName()));
        }
    }

    public List<PlanOptimizerInformation> getPlanOptimizerInfo()
    {
        return planOptimizerInfo;
    }

    public List<CTEInformation> getCteInformationList()
    {
        return cteInformationList;
    }
    public List<OptimizerResult> getPlanOptimizerResults()
    {
        return planOptimizerResults;
    }
}

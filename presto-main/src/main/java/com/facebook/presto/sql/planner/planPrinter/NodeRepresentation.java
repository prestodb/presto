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

import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodeRepresentation
{
    private final PlanNodeId id;
    private final String name;
    private final String type;
    private final String identifier;
    private final List<VariableReferenceExpression> outputs;
    private final List<PlanNodeId> children;
    private final List<PlanFragmentId> remoteSources;
    private final Optional<PlanNodeStats> stats;
    private final List<PlanNodeStatsEstimate> estimatedStats;
    private final List<PlanCostEstimate> estimatedCost;

    private final StringBuilder details = new StringBuilder();

    public NodeRepresentation(
            PlanNodeId id,
            String name,
            String type,
            String identifier,
            List<VariableReferenceExpression> outputs,
            Optional<PlanNodeStats> stats,
            List<PlanNodeStatsEstimate> estimatedStats,
            List<PlanCostEstimate> estimatedCost,
            List<PlanNodeId> children,
            List<PlanFragmentId> remoteSources)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.estimatedStats = requireNonNull(estimatedStats, "estimatedStats is null");
        this.estimatedCost = requireNonNull(estimatedCost, "estimatedCost is null");
        this.children = requireNonNull(children, "children is null");
        this.remoteSources = requireNonNull(remoteSources, "remoteSources is null");

        checkArgument(estimatedCost.size() == estimatedStats.size(), "size of cost and stats list does not match");
    }

    public void appendDetails(String string, Object... args)
    {
        if (args.length == 0) {
            details.append(string);
        }
        else {
            details.append(format(string, args));
        }
    }

    public void appendDetailsLine(String string, Object... args)
    {
        appendDetails(string, args);
        details.append('\n');
    }

    public PlanNodeId getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public List<VariableReferenceExpression> getOutputs()
    {
        return outputs;
    }

    public List<PlanNodeId> getChildren()
    {
        return children;
    }

    public List<PlanFragmentId> getRemoteSources()
    {
        return remoteSources;
    }

    public String getDetails()
    {
        return details.toString();
    }

    public Optional<PlanNodeStats> getStats()
    {
        return stats;
    }

    public List<PlanNodeStatsEstimate> getEstimatedStats()
    {
        return estimatedStats;
    }

    public List<PlanCostEstimate> getEstimatedCost()
    {
        return estimatedCost;
    }
}

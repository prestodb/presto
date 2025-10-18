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
package com.facebook.presto.spi.plan;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class MaterializedViewScanNode
        extends PlanNode
{
    private final PlanNode dataTablePlan;
    private final PlanNode viewQueryPlan;
    private final QualifiedObjectName materializedViewName;
    private final Map<VariableReferenceExpression, VariableReferenceExpression> dataTableMappings;
    private final Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMappings;
    private final List<VariableReferenceExpression> outputVariables;

    @JsonCreator
    public MaterializedViewScanNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("dataTablePlan") PlanNode dataTablePlan,
            @JsonProperty("viewQueryPlan") PlanNode viewQueryPlan,
            @JsonProperty("materializedViewName") QualifiedObjectName materializedViewName,
            @JsonProperty("dataTableMappings") Map<VariableReferenceExpression, VariableReferenceExpression> dataTableMappings,
            @JsonProperty("viewQueryMappings") Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMappings,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables)
    {
        this(sourceLocation, id, Optional.empty(), dataTablePlan, viewQueryPlan, materializedViewName, dataTableMappings, viewQueryMappings, outputVariables);
    }

    public MaterializedViewScanNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode dataTablePlan,
            PlanNode viewQueryPlan,
            QualifiedObjectName materializedViewName,
            Map<VariableReferenceExpression, VariableReferenceExpression> dataTableMappings,
            Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMappings,
            List<VariableReferenceExpression> outputVariables)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.dataTablePlan = requireNonNull(dataTablePlan, "dataTablePlan is null");
        this.viewQueryPlan = requireNonNull(viewQueryPlan, "viewQueryPlan is null");
        this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
        this.dataTableMappings = unmodifiableMap(new HashMap<>(requireNonNull(dataTableMappings, "dataTableMappings is null")));
        this.viewQueryMappings = unmodifiableMap(new HashMap<>(requireNonNull(viewQueryMappings, "viewQueryMappings is null")));
        this.outputVariables = unmodifiableList(new ArrayList<>(requireNonNull(outputVariables, "outputVariables is null")));
    }

    @JsonProperty("dataTablePlan")
    public PlanNode getDataTablePlan()
    {
        return dataTablePlan;
    }

    @JsonProperty("viewQueryPlan")
    public PlanNode getViewQueryPlan()
    {
        return viewQueryPlan;
    }

    @JsonProperty("materializedViewName")
    public QualifiedObjectName getMaterializedViewName()
    {
        return materializedViewName;
    }

    @JsonProperty("dataTableMappings")
    public Map<VariableReferenceExpression, VariableReferenceExpression> getDataTableMappings()
    {
        return dataTableMappings;
    }

    @JsonProperty("viewQueryMappings")
    public Map<VariableReferenceExpression, VariableReferenceExpression> getViewQueryMappings()
    {
        return viewQueryMappings;
    }

    @Override
    @JsonProperty("outputVariables")
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public List<PlanNode> getSources()
    {
        // MaterializedViewScanNode exposes both alternative plans as children
        // so they can be properly optimized by the plan framework.
        // The MaterializedViewOptimizer will later choose which one to use.
        List<PlanNode> sources = new ArrayList<>(2);
        sources.add(dataTablePlan);
        sources.add(viewQueryPlan);
        return unmodifiableList(sources);
    }

    /**
     * Returns the output layout for a specific source (similar to SetOperationNode.sourceOutputLayout)
     * @param sourceIndex 0 for dataTablePlan, 1 for viewQueryPlan
     * @return List of variables from the source that map to the output variables
     */
    public List<VariableReferenceExpression> sourceOutputLayout(int sourceIndex)
    {
        if (sourceIndex == 0) {
            // Data table plan - use dataTableMappings
            return outputVariables.stream()
                    .map(dataTableMappings::get)
                    .collect(java.util.stream.Collectors.toList());
        }
        else if (sourceIndex == 1) {
            // View query plan - use viewQueryMappings
            return outputVariables.stream()
                    .map(viewQueryMappings::get)
                    .collect(java.util.stream.Collectors.toList());
        }
        else {
            throw new IllegalArgumentException("MaterializedViewScanNode only has 2 sources (index 0 and 1), but got index " + sourceIndex);
        }
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("Expected exactly 2 children but got " + newChildren.size());
        }
        return new MaterializedViewScanNode(
                getSourceLocation(),
                getId(),
                getStatsEquivalentPlanNode(),
                newChildren.get(0),
                newChildren.get(1),
                materializedViewName,
                dataTableMappings,
                viewQueryMappings,
                outputVariables);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMaterializedViewScan(this, context);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new MaterializedViewScanNode(
                getSourceLocation(),
                getId(),
                statsEquivalentPlanNode,
                dataTablePlan,
                viewQueryPlan,
                materializedViewName,
                dataTableMappings,
                viewQueryMappings,
                outputVariables);
    }
}

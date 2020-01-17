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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class TableWriterMergeNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final VariableReferenceExpression rowCountVariable;
    private final VariableReferenceExpression fragmentVariable;
    private final VariableReferenceExpression tableCommitContextVariable;
    private final Optional<StatisticAggregations> statisticsAggregation;
    private final List<VariableReferenceExpression> outputs;

    @JsonCreator
    public TableWriterMergeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("rowCountVariable") VariableReferenceExpression rowCountVariable,
            @JsonProperty("fragmentVariable") VariableReferenceExpression fragmentVariable,
            @JsonProperty("tableCommitContextVariable") VariableReferenceExpression tableCommitContextVariable,
            @JsonProperty("statisticsAggregation") Optional<StatisticAggregations> statisticsAggregation)
    {
        super(requireNonNull(id, "id is null"));
        this.source = requireNonNull(source, "source is null");
        this.rowCountVariable = requireNonNull(rowCountVariable, "rowCountVariable is null");
        this.fragmentVariable = requireNonNull(fragmentVariable, "fragmentVariable is null");
        this.tableCommitContextVariable = requireNonNull(tableCommitContextVariable, "tableCommitContextVariable is null");
        this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");

        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.<VariableReferenceExpression>builder()
                .add(rowCountVariable)
                .add(fragmentVariable)
                .add(tableCommitContextVariable);
        statisticsAggregation.ifPresent(aggregation -> {
            outputs.addAll(aggregation.getGroupingVariables());
            outputs.addAll(aggregation.getAggregations().keySet());
        });
        this.outputs = outputs.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public VariableReferenceExpression getRowCountVariable()
    {
        return rowCountVariable;
    }

    @JsonProperty
    public VariableReferenceExpression getFragmentVariable()
    {
        return fragmentVariable;
    }

    @JsonProperty
    public VariableReferenceExpression getTableCommitContextVariable()
    {
        return tableCommitContextVariable;
    }

    @JsonProperty
    public Optional<StatisticAggregations> getStatisticsAggregation()
    {
        return statisticsAggregation;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TableWriterMergeNode(getId(), getOnlyElement(newChildren), rowCountVariable, fragmentVariable, tableCommitContextVariable, statisticsAggregation);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableWriteMerge(this, context);
    }
}

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

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TableFinishNode
        extends PlanNode
{
    private final PlanNode source;
    private final Optional<TableWriterNode.WriterTarget> target;
    private final VariableReferenceExpression rowCountVariable;
    private final Optional<StatisticAggregations> statisticsAggregation;
    private final Optional<StatisticAggregationsDescriptor<VariableReferenceExpression>> statisticsAggregationDescriptor;

    @JsonCreator
    public TableFinishNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") Optional<TableWriterNode.WriterTarget> target,
            @JsonProperty("rowCountVariable") VariableReferenceExpression rowCountVariable,
            @JsonProperty("statisticsAggregation") Optional<StatisticAggregations> statisticsAggregation,
            @JsonProperty("statisticsAggregationDescriptor") Optional<StatisticAggregationsDescriptor<VariableReferenceExpression>> statisticsAggregationDescriptor)
    {
        this(sourceLocation, id, Optional.empty(), source, target, rowCountVariable, statisticsAggregation, statisticsAggregationDescriptor);
    }

    public TableFinishNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            Optional<TableWriterNode.WriterTarget> target,
            VariableReferenceExpression rowCountVariable,
            Optional<StatisticAggregations> statisticsAggregation,
            Optional<StatisticAggregationsDescriptor<VariableReferenceExpression>> statisticsAggregationDescriptor)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        checkArgument(target != null || source instanceof TableWriterNode);
        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowCountVariable = requireNonNull(rowCountVariable, "rowCountVariable is null");
        this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");
        this.statisticsAggregationDescriptor = requireNonNull(statisticsAggregationDescriptor, "statisticsAggregationDescriptor is null");
        checkArgument(statisticsAggregation.isPresent() == statisticsAggregationDescriptor.isPresent(), "statisticsAggregation and statisticsAggregationDescriptor must both be either present or absent");
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonIgnore
    public Optional<TableWriterNode.WriterTarget> getTarget()
    {
        return target;
    }

    @JsonProperty
    public VariableReferenceExpression getRowCountVariable()
    {
        return rowCountVariable;
    }

    @JsonProperty
    public Optional<StatisticAggregations> getStatisticsAggregation()
    {
        return statisticsAggregation;
    }

    @JsonProperty
    public Optional<StatisticAggregationsDescriptor<VariableReferenceExpression>> getStatisticsAggregationDescriptor()
    {
        return statisticsAggregationDescriptor;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.singletonList(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return Collections.singletonList(rowCountVariable);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableFinish(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1);
        return new TableFinishNode(
                getSourceLocation(),
                getId(),
                getStatsEquivalentPlanNode(),
                newChildren.get(0),
                target,
                rowCountVariable,
                statisticsAggregation,
                statisticsAggregationDescriptor);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new TableFinishNode(
                getSourceLocation(),
                getId(),
                statsEquivalentPlanNode,
                source,
                target,
                rowCountVariable,
                statisticsAggregation,
                statisticsAggregationDescriptor);
    }
}

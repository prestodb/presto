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

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * For every row from {@link #input} a {@link #subquery} relation is calculated.
 * Then input row is cross joined with subquery relation and returned as a result.
 * <p>
 * INNER - does not return any row for input row when subquery relation is empty
 * LEFT - does return input completed with NULL values when subquery relation is empty
 */
@Immutable
public class LateralJoinNode
        extends InternalPlanNode
{
    public enum Type
    {
        INNER(JoinNode.Type.INNER),
        LEFT(JoinNode.Type.LEFT);

        Type(JoinNode.Type joinNodeType)
        {
            this.joinNodeType = joinNodeType;
        }

        private final JoinNode.Type joinNodeType;

        public JoinNode.Type toJoinNodeType()
        {
            return joinNodeType;
        }
    }

    private final PlanNode input;
    private final PlanNode subquery;

    /**
     * Correlation variables, returned from input (outer plan) used in subquery (inner plan)
     */
    private final List<VariableReferenceExpression> correlation;
    private final Type type;

    /**
     * This information is only used for sanity check.
     */
    private final String originSubqueryError;

    @JsonCreator
    public LateralJoinNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("input") PlanNode input,
            @JsonProperty("subquery") PlanNode subquery,
            @JsonProperty("correlation") List<VariableReferenceExpression> correlation,
            @JsonProperty("type") Type type,
            @JsonProperty("originSubqueryError") String originSubqueryError)
    {
        this(sourceLocation, id, Optional.empty(), input, subquery, correlation, type, originSubqueryError);
    }

    public LateralJoinNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode input,
            PlanNode subquery,
            List<VariableReferenceExpression> correlation,
            Type type,
            String originSubqueryError)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        requireNonNull(input, "input is null");
        requireNonNull(subquery, "right is null");
        requireNonNull(correlation, "correlation is null");
        requireNonNull(originSubqueryError, "originSubqueryError is null");

        checkArgument(input.getOutputVariables().containsAll(correlation), "Input does not contain symbols from correlation");

        this.input = input;
        this.subquery = subquery;
        this.correlation = ImmutableList.copyOf(correlation);
        this.type = type;
        this.originSubqueryError = originSubqueryError;
    }

    @JsonProperty
    public PlanNode getInput()
    {
        return input;
    }

    @JsonProperty
    public PlanNode getSubquery()
    {
        return subquery;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getCorrelation()
    {
        return correlation;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getOriginSubqueryError()
    {
        return originSubqueryError;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(input, subquery);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(input.getOutputVariables())
                .addAll(subquery.getOutputVariables())
                .build();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new LateralJoinNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), newChildren.get(1), correlation, type, originSubqueryError);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new LateralJoinNode(getSourceLocation(), getId(), statsEquivalentPlanNode, input, subquery, correlation, type, originSubqueryError);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitLateralJoin(this, context);
    }
}

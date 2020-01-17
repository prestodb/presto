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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.presto.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ApplyNode
        extends InternalPlanNode
{
    private final PlanNode input;
    private final PlanNode subquery;

    /**
     * Correlation variables, returned from input (outer plan) used in subquery (inner plan)
     */
    private final List<VariableReferenceExpression> correlation;

    /**
     * Expressions that use subquery symbols.
     * <p>
     * Subquery expressions are different than other expressions
     * in a sense that they might use an entire subquery result
     * as an input (e.g: "x IN (subquery)", "x < ALL (subquery)").
     * Such expressions are invalid in linear operator context
     * (e.g: ProjectNode) in logical plan, but are correct in
     * ApplyNode context.
     * <p>
     * Example 1:
     * - expression: input_symbol_X IN (subquery_symbol_Y)
     * - meaning: if set consisting of all values for subquery_symbol_Y contains value represented by input_symbol_X
     * <p>
     * Example 2:
     * - expression: input_symbol_X < ALL (subquery_symbol_Y)
     * - meaning: if input_symbol_X is smaller than all subquery values represented by subquery_symbol_Y
     * <p>
     */
    private final Assignments subqueryAssignments;

    /**
     * This information is only used for sanity check.
     */
    private final String originSubqueryError;

    @JsonCreator
    public ApplyNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("input") PlanNode input,
            @JsonProperty("subquery") PlanNode subquery,
            @JsonProperty("subqueryAssignments") Assignments subqueryAssignments,
            @JsonProperty("correlation") List<VariableReferenceExpression> correlation,
            @JsonProperty("originSubqueryError") String originSubqueryError)
    {
        super(id);
        checkArgument(input.getOutputVariables().containsAll(correlation), "Input does not contain symbols from correlation");
        verifySubquerySupported(subqueryAssignments);

        this.input = requireNonNull(input, "input is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
        this.subqueryAssignments = requireNonNull(subqueryAssignments, "assignments is null");
        this.correlation = ImmutableList.copyOf(requireNonNull(correlation, "correlation is null"));
        this.originSubqueryError = requireNonNull(originSubqueryError, "originSubqueryError is null");
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
    public Assignments getSubqueryAssignments()
    {
        return subqueryAssignments;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getCorrelation()
    {
        return correlation;
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
                .addAll(subqueryAssignments.getOutputs())
                .build();
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitApply(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new ApplyNode(getId(), newChildren.get(0), newChildren.get(1), subqueryAssignments, correlation, originSubqueryError);
    }
}

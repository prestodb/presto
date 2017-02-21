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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.SymbolReference;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isScalar;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ApplyNode
        extends PlanNode
{
    private final PlanNode input;
    private final PlanNode subquery;

    /**
     * Correlation symbols, returned from input (outer plan) used in subquery (inner plan)
     */
    private final List<Symbol> correlation;

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
     * Example 3:
     * - expression: subquery_symbol_Y
     * - meaning: subquery is scalar (might be enforced), therefore subquery_symbol_Y can be used directly in the rest of the plan
     */
    private final Assignments subqueryAssignments;

    @JsonCreator
    public ApplyNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("input") PlanNode input,
            @JsonProperty("subquery") PlanNode subquery,
            @JsonProperty("subqueryAssignments") Assignments subqueryAssignments,
            @JsonProperty("correlation") List<Symbol> correlation)
    {
        super(id);
        requireNonNull(input, "input is null");
        requireNonNull(subquery, "right is null");
        requireNonNull(subqueryAssignments, "assignments is null");
        requireNonNull(correlation, "correlation is null");

        checkArgument(input.getOutputSymbols().containsAll(correlation), "Input does not contain symbols from correlation");

        this.input = input;
        this.subquery = subquery;
        this.subqueryAssignments = subqueryAssignments;
        this.correlation = ImmutableList.copyOf(correlation);
    }

    public boolean isResolvedScalarSubquery()
    {
        return isScalar(subquery) && isSubqueryResolved();
    }

    /**
     * @return true if output symbols of subquery are directly mapped to ApplyNode output symbols
     */
    public boolean isSubqueryResolved()
    {
        return subqueryAssignments.getExpressions().stream()
                .allMatch(expression -> expression instanceof SymbolReference);
    }

    @JsonProperty("input")
    public PlanNode getInput()
    {
        return input;
    }

    @JsonProperty("subquery")
    public PlanNode getSubquery()
    {
        return subquery;
    }

    @JsonProperty("subqueryAssignments")
    public Assignments getSubqueryAssignments()
    {
        return subqueryAssignments;
    }

    @JsonProperty("correlation")
    public List<Symbol> getCorrelation()
    {
        return correlation;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(input, subquery);
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(input.getOutputSymbols())
                .addAll(subqueryAssignments.getOutputs())
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitApply(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new ApplyNode(getId(), newChildren.get(0), newChildren.get(1), subqueryAssignments, correlation);
    }
}

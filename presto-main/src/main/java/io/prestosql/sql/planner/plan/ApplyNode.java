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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;

import javax.annotation.concurrent.Immutable;

import java.util.List;

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
     */
    private final Assignments subqueryAssignments;

    /**
     * HACK!
     * Used for error reporting in case this ApplyNode is not supported
     */
    private final Node originSubquery;

    @JsonCreator
    public ApplyNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("input") PlanNode input,
            @JsonProperty("subquery") PlanNode subquery,
            @JsonProperty("subqueryAssignments") Assignments subqueryAssignments,
            @JsonProperty("correlation") List<Symbol> correlation,
            @JsonProperty("originSubquery") Node originSubquery)
    {
        super(id);
        requireNonNull(input, "input is null");
        requireNonNull(subquery, "right is null");
        requireNonNull(subqueryAssignments, "assignments is null");
        requireNonNull(correlation, "correlation is null");
        requireNonNull(originSubquery, "originSubquery is null");

        checkArgument(input.getOutputSymbols().containsAll(correlation), "Input does not contain symbols from correlation");
        checkArgument(
                subqueryAssignments.getExpressions().stream().allMatch(ApplyNode::isSupportedSubqueryExpression),
                "Unexpected expression used for subquery expression");

        this.input = input;
        this.subquery = subquery;
        this.subqueryAssignments = subqueryAssignments;
        this.correlation = ImmutableList.copyOf(correlation);
        this.originSubquery = originSubquery;
    }

    private static boolean isSupportedSubqueryExpression(Expression expression)
    {
        return expression instanceof InPredicate ||
                expression instanceof ExistsPredicate ||
                expression instanceof QuantifiedComparisonExpression;
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

    @JsonProperty("originSubquery")
    public Node getOriginSubquery()
    {
        return originSubquery;
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
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitApply(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new ApplyNode(getId(), newChildren.get(0), newChildren.get(1), subqueryAssignments, correlation, originSubquery);
    }
}

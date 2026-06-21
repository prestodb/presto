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
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * A FilterNode variant that opts out of merging, decorrelation, and predicate-pushdown rewrites.
 * Used for guard predicates whose evaluation must not be combined with other predicates — for
 * example, the scalar-subquery cardinality check whose predicate calls {@code fail()} when the
 * subquery returns more than one row. Merging or pushing such a guard with sibling predicates
 * lets the engine short-circuit the error away over selectivity vectors and produce wrong results.
 *
 * Optimizer rules pattern-match on {@code FilterNode} via {@link Patterns#filter()} and therefore
 * do not see this node. A terminal optimizer pass rewrites this node back to a regular FilterNode
 * before plan fragmentation, so execution planners and the wire protocol never observe it.
 */
@Immutable
public final class UnmergeableFilterNode
        extends PlanNode
{
    private final PlanNode source;
    private final RowExpression predicate;

    @JsonCreator
    public UnmergeableFilterNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("predicate") RowExpression predicate)
    {
        this(sourceLocation, id, Optional.empty(), source, predicate);
    }

    public UnmergeableFilterNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            RowExpression predicate)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.source = requireNonNull(source, "source is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public RowExpression getPredicate()
    {
        return predicate;
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return singletonList(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnmergeableFilter(this, context);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new UnmergeableFilterNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, predicate);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        if (newChildren == null || newChildren.size() != 1) {
            throw new IllegalArgumentException("Expect exactly one child to replace");
        }
        return new UnmergeableFilterNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), predicate);
    }

    @Override
    public LogicalProperties computeLogicalProperties(LogicalPropertiesProvider logicalPropertiesProvider)
    {
        requireNonNull(logicalPropertiesProvider, "logicalPropertiesProvider cannot be null.");
        // Logical properties of an unmergeable filter are identical to those of the equivalent FilterNode.
        return logicalPropertiesProvider.getFilterProperties(
                new FilterNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), source, predicate));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnmergeableFilterNode that = (UnmergeableFilterNode) o;
        return Objects.equals(source, that.source) && Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, predicate);
    }
}

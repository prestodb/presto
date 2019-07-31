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

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TopNNode
        extends PlanNode
{
    /**
     * Stages of `TopNNode`:
     *
     * SINGLE:    `TopNNode` is in the logical plan.
     * PARTIAL:   `TopNNode` is in the distributed plan, and generates partial results of `TopN` on local workers.
     * FINAL:     `TopNNode` is in the distributed plan, and finalizes the partial results from `PARTIAL` nodes.
     */
    public enum Step
    {
        SINGLE,
        PARTIAL,
        FINAL
    }

    private final PlanNode source;
    private final long count;
    private final OrderingScheme orderingScheme;
    private final Step step;

    @JsonCreator
    public TopNNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("orderingScheme") OrderingScheme orderingScheme,
            @JsonProperty("step") Step step)
    {
        super(id);

        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be positive");
        checkCondition(count <= Integer.MAX_VALUE, NOT_SUPPORTED, "ORDER BY LIMIT > %s is not supported", Integer.MAX_VALUE);
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.count = count;
        this.orderingScheme = orderingScheme;
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return singletonList(source);
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

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @JsonProperty("orderingScheme")
    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkCondition(newChildren != null && newChildren.size() == 1, GENERIC_INTERNAL_ERROR, "Expect exactly 1 child PlanNode");
        return new TopNNode(getId(), newChildren.get(0), count, orderingScheme, step);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(errorCode, format(formatString, args));
        }
    }
}

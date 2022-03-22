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
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class LimitNode
        extends PlanNode
{
    /**
     * Stages of `LimitNode`:
     *
     * PARTIAL:   `LimitNode` is in the distributed plan and generates partial results on local workers.
     * FINAL:     `LimitNode` is in the distributed plan and finalizes the partial results from `PARTIAL` nodes.
     */
    public enum Step
    {
        PARTIAL,
        FINAL
    }

    private final PlanNode source;
    private final long count;
    private final Step step;

    @JsonCreator
    public LimitNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("count") long count,
            @JsonProperty("step") Step step)
    {
        super(id);
        checkCondition(count >= 0, INVALID_FUNCTION_ARGUMENT, "count must be greater than or equal to zero");

        this.source = requireNonNull(source, "source is null");
        this.count = count;
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return singletonList(source);
    }

    /**
     * LimitNode only expects a single upstream PlanNode.
     */
    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    /**
     * Get the limit `N` number of results to return.
     */
    @JsonProperty
    public long getCount()
    {
        return count;
    }

    @JsonProperty
    public Step getStep()
    {
        return step;
    }

    public boolean isPartial()
    {
        return step == Step.PARTIAL;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkCondition(newChildren != null && newChildren.size() == 1, GENERIC_INTERNAL_ERROR, "Expect exactly 1 child PlanNode");
        return new LimitNode(getId(), newChildren.get(0), count, getStep());
    }

    private static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String message)
    {
        if (!condition) {
            throw new PrestoException(errorCode, message);
        }
    }
}

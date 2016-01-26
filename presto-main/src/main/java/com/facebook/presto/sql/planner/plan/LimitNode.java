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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class LimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;

    @JsonCreator
    public LimitNode(@JsonProperty("id") PlanNodeId id, @JsonProperty("source") PlanNode source, @JsonProperty("count") long count)
    {
        super(id);

        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be greater than or equal to zero");
        checkCondition(count <= Integer.MAX_VALUE, NOT_SUPPORTED, "LIMIT > %s is not supported", Integer.MAX_VALUE);

        this.source = source;
        this.count = count;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("count")
    public long getCount()
    {
        return count;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }
}

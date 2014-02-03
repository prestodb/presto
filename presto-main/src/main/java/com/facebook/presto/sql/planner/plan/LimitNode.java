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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

@Immutable
public class LimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long count;
    private final Optional<Symbol> sampleWeight;

    @JsonCreator
    public LimitNode(@JsonProperty("id") PlanNodeId id, @JsonProperty("source") PlanNode source, @JsonProperty("count") long count, @JsonProperty("sampleWeight") Optional<Symbol> sampleWeight)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkArgument(count >= 0, "count must be greater than or equal to zero");
        Preconditions.checkNotNull(sampleWeight, "sampleWeight is null");
        if (sampleWeight.isPresent()) {
            Preconditions.checkArgument(source.getOutputSymbols().contains(sampleWeight.get()), "source does not output sample weight");
        }

        this.source = source;
        this.count = count;
        this.sampleWeight = sampleWeight;
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

    @JsonProperty("sampleWeight")
    public Optional<Symbol> getSampleWeight()
    {
        return sampleWeight;
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

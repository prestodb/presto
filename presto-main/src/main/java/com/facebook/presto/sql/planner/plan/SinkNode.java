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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SinkNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> outputSymbols; // Expected output symbol layout

    @JsonCreator
    public SinkNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(outputSymbols, "outputSymbols is null");

        this.source = source;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);

        Preconditions.checkArgument(source.getOutputSymbols().containsAll(this.outputSymbols), "Source output needs to be able to produce all of the required outputSymbols");
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSink(this, context);
    }
}

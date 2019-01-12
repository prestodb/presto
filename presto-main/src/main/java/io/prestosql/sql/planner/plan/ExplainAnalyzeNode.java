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
import com.google.common.collect.Iterables;
import io.prestosql.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class ExplainAnalyzeNode
        extends PlanNode
{
    private final PlanNode source;
    private final Symbol outputSymbol;
    private final boolean verbose;

    @JsonCreator
    public ExplainAnalyzeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("outputSymbol") Symbol outputSymbol,
            @JsonProperty("verbose") boolean verbose)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.outputSymbol = requireNonNull(outputSymbol, "outputSymbol is null");
        this.verbose = verbose;
    }

    @JsonProperty("outputSymbol")
    public Symbol getOutputSymbol()
    {
        return outputSymbol;
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("verbose")
    public boolean isVerbose()
    {
        return verbose;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.of(outputSymbol);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplainAnalyze(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ExplainAnalyzeNode(getId(), Iterables.getOnlyElement(newChildren), outputSymbol, isVerbose());
    }
}

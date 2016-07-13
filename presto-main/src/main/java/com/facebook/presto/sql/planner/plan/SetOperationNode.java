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
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public abstract class SetOperationNode
        extends PlanNode
{
    private final List<PlanNode> sources;
    private final ListMultimap<Symbol, Symbol> outputToInputs;
    private final List<Symbol> outputs;

    @JsonCreator
    protected SetOperationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("outputToInputs") ListMultimap<Symbol, Symbol> outputToInputs,
            @JsonProperty("outputs") List<Symbol> outputs)
    {
        super(id);

        requireNonNull(sources, "sources is null");
        checkArgument(!sources.isEmpty(), "Must have at least one source");
        requireNonNull(outputToInputs, "outputToInputs is null");
        requireNonNull(outputs, "outputs is null");

        this.sources = ImmutableList.copyOf(sources);
        this.outputToInputs = ImmutableListMultimap.copyOf(outputToInputs);
        this.outputs = ImmutableList.copyOf(outputs);

        for (Collection<Symbol> inputs : this.outputToInputs.asMap().values()) {
            checkArgument(inputs.size() == this.sources.size(), "Every source needs to map its symbols to an output %s operation symbol", this.getClass().getSimpleName());
        }

        // Make sure each source positionally corresponds to their Symbol values in the Multimap
        for (int i = 0; i < sources.size(); i++) {
            for (Collection<Symbol> expectedInputs : this.outputToInputs.asMap().values()) {
                checkArgument(sources.get(i).getOutputSymbols().contains(Iterables.get(expectedInputs, i)), "Source does not provide required symbols");
            }
        }
    }

    @Override
    @JsonProperty("sources")
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @Override
    @JsonProperty("outputs")
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty("outputToInputs")
    public ListMultimap<Symbol, Symbol> getSymbolMapping()
    {
        return outputToInputs;
    }

    public List<Symbol> sourceOutputLayout(int sourceIndex)
    {
        // Make sure the sourceOutputLayout symbols are listed in the same order as the corresponding output symbols
        return getOutputSymbols().stream()
                .map(symbol -> outputToInputs.get(symbol).get(sourceIndex))
                .collect(toImmutableList());
    }

    /**
     * Returns the output to input symbol mapping for the given source channel
     */
    public Map<Symbol, SymbolReference> sourceSymbolMap(int sourceIndex)
    {
        ImmutableMap.Builder<Symbol, SymbolReference> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Collection<Symbol>> entry : outputToInputs.asMap().entrySet()) {
            builder.put(entry.getKey(), Iterables.get(entry.getValue(), sourceIndex).toSymbolReference());
        }

        return builder.build();
    }

    /**
     * Returns the input to output symbol mapping for the given source channel.
     * A single input symbol can map to multiple output symbols, thus requiring a Multimap.
     */
    public Multimap<Symbol, SymbolReference> outputSymbolMap(int sourceIndex)
    {
        return Multimaps.transformValues(FluentIterable.from(getOutputSymbols())
                .toMap(outputToSourceSymbolFunction(sourceIndex))
                .asMultimap()
                .inverse(), Symbol::toSymbolReference);
    }

    private Function<Symbol, Symbol> outputToSourceSymbolFunction(final int sourceIndex)
    {
        return outputSymbol -> outputToInputs.get(outputSymbol).get(sourceIndex);
    }
}

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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@Immutable
public class MultiSourceSymbolMapping
{
    private final ListMultimap<Symbol, Symbol> outputToInputs;
    private final List<Symbol> outputs;
    private final List<PlanNode> sources;

    public MultiSourceSymbolMapping(ListMultimap<Symbol, Symbol> outputToInputs, List<PlanNode> sources)
    {
        this(outputToInputs, ImmutableList.copyOf(requireNonNull(outputToInputs, "outputToInputs is null").keySet()), sources);
    }

    public MultiSourceSymbolMapping(
            @JsonProperty("outputToInputs") ListMultimap<Symbol, Symbol> outputToInputs,
            @JsonProperty("outputs") List<Symbol> outputs,
            @JsonProperty("sources") List<PlanNode> sources)
    {
        this.outputToInputs = ImmutableListMultimap.copyOf(requireNonNull(outputToInputs, "outputToInputs is null"));
        this.outputs = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));

        checkArgument(!sources.isEmpty(), "Must have at least one source");

        // Make sure each source positionally corresponds to their Symbol values in the Multimap
        for (int i = 0; i < sources.size(); i++) {
            checkArgument(sources.get(i).getOutputSymbols().containsAll(sourceOutputLayout(i)), "Source does not provide required symbols");
        }
    }

    @JsonProperty("outputToInputs")
    public ListMultimap<Symbol, Symbol> getOutputToInputs()
    {
        return outputToInputs;
    }

    @JsonProperty("outputs")
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty("sources")
    public List<PlanNode> getSources()
    {
        return sources;
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
        return outputs.stream()
                .collect(toImmutableMap(identity(), symbol -> outputToInputs.get(symbol).get(sourceIndex).toSymbolReference()));
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

    public Symbol getInput(Symbol output, int index)
    {
        return outputToInputs.get(output).get(index);
    }

    public List<Symbol> getInput(Symbol output)
    {
        return outputToInputs.get(output);
    }

    public MultiSourceSymbolMapping replaceSources(List<PlanNode> newChildren)
    {
        return new MultiSourceSymbolMapping(outputToInputs, outputs, newChildren);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputs = ImmutableListMultimap.builder();
        private final ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();

        public Builder putAll(ListMultimap<Symbol, Symbol> outputToInputs)
        {
            this.outputsToInputs.putAll(outputToInputs);
            return this;
        }

        public Builder put(Symbol value, Symbol requiredHashSymbol)
        {
            this.outputsToInputs.put(value, requiredHashSymbol);
            return this;
        }

        public Builder putAll(Symbol symbol, List<Symbol> input)
        {
            this.outputsToInputs.putAll(symbol, input);
            return this;
        }

        public Builder addSource(PlanNode source)
        {
            this.sources.add(source);
            return this;
        }

        public MultiSourceSymbolMapping build()
        {
            return new MultiSourceSymbolMapping(outputsToInputs.build(), sources.build());
        }
    }
}

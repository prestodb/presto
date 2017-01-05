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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class AggregationNode
        extends PlanNode
{
    private final PlanNode source;
    private final Map<Symbol, FunctionCall> aggregations;
    // Map from function symbol, to the mask symbol
    private final Map<Symbol, Symbol> masks;
    private final List<List<Symbol>> groupingSets;
    private final Map<Symbol, Signature> functions;
    private final Step step;
    private final Optional<Symbol> hashSymbol;
    private final Optional<Symbol> groupIdSymbol;

    public enum Step
    {
        PARTIAL(true, true),
        FINAL(false, false),
        INTERMEDIATE(false, true),
        SINGLE(true, false);

        private final boolean inputRaw;
        private final boolean outputPartial;

        Step(boolean inputRaw, boolean outputPartial)
        {
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        public boolean isInputRaw()
        {
            return inputRaw;
        }

        public boolean isOutputPartial()
        {
            return outputPartial;
        }

        public static Step partialOutput(Step step)
        {
            if (step.isInputRaw()) {
                return Step.PARTIAL;
            }
            else {
                return Step.INTERMEDIATE;
            }
        }

        public static Step partialInput(Step step)
        {
            if (step.isOutputPartial()) {
                return Step.INTERMEDIATE;
            }
            else {
                return Step.FINAL;
            }
        }
    }

    @JsonCreator
    public AggregationNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("aggregations") Map<Symbol, FunctionCall> aggregations,
            @JsonProperty("functions") Map<Symbol, Signature> functions,
            @JsonProperty("masks") Map<Symbol, Symbol> masks,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("step") Step step,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("groupIdSymbol") Optional<Symbol> groupIdSymbol)
    {
        super(id);

        this.source = source;
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        this.functions = ImmutableMap.copyOf(requireNonNull(functions, "functions is null"));
        this.masks = ImmutableMap.copyOf(requireNonNull(masks, "masks is null"));
        for (Symbol mask : masks.keySet()) {
            checkArgument(aggregations.containsKey(mask), "mask does not match any aggregations");
        }
        requireNonNull(groupingSets, "groupingSets is null");
        checkArgument(!groupingSets.isEmpty(), "grouping sets list cannot be empty");
        this.groupingSets = ImmutableList.copyOf(groupingSets);
        this.step = step;
        this.hashSymbol = hashSymbol;
        this.groupIdSymbol = requireNonNull(groupIdSymbol);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();

        symbols.addAll(getGroupingKeys());
        hashSymbol.ifPresent(symbols::add);
        symbols.addAll(aggregations.keySet());

        return symbols.build();
    }

    @JsonProperty("aggregations")
    public Map<Symbol, FunctionCall> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty("functions")
    public Map<Symbol, Signature> getFunctions()
    {
        return functions;
    }

    @JsonProperty("masks")
    public Map<Symbol, Symbol> getMasks()
    {
        return masks;
    }

    public List<Symbol> getGroupingKeys()
    {
        List<Symbol> symbols = new ArrayList<>(groupingSets.stream()
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList()));

        groupIdSymbol.ifPresent(symbols::add);
        return symbols;
    }

    @JsonProperty("groupingSets")
    public List<List<Symbol>> getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    @JsonProperty("hashSymbol")
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty("groupIdSymbol")
    public Optional<Symbol> getGroupIdSymbol()
    {
        return groupIdSymbol;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }
}

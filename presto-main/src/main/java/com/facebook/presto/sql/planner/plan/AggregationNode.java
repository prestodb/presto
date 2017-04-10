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
import com.google.common.collect.Iterables;

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
    private final Map<Symbol, Aggregation> assignments;
    private final List<List<Symbol>> groupingSets;
    private final Step step;
    private final Optional<Symbol> hashSymbol;
    private final Optional<Symbol> groupIdSymbol;
    private final List<Symbol> outputs;

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
    public AggregationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("assignments") Map<Symbol, Aggregation> assignments,
            @JsonProperty("groupingSets") List<List<Symbol>> groupingSets,
            @JsonProperty("step") Step step,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("groupIdSymbol") Optional<Symbol> groupIdSymbol)
    {
        super(id);

        this.source = source;
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "aggregations is null"));
        requireNonNull(groupingSets, "groupingSets is null");
        checkArgument(!groupingSets.isEmpty(), "grouping sets list cannot be empty");
        this.groupingSets = ImmutableList.copyOf(groupingSets);
        this.step = step;
        this.hashSymbol = hashSymbol;
        this.groupIdSymbol = requireNonNull(groupIdSymbol);

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        outputs.addAll(getGroupingKeys());
        hashSymbol.ifPresent(outputs::add);
        outputs.addAll(assignments.keySet());

        this.outputs = outputs.build();
    }

    /**
     * @deprecated pass Assignments object instead
     */
    @Deprecated
    public AggregationNode(
            PlanNodeId id,
            PlanNode source,
            Map<Symbol, FunctionCall> assignments,
            Map<Symbol, Signature> functions,
            Map<Symbol, Symbol> masks,
            List<List<Symbol>> groupingSets,
            Step step,
            Optional<Symbol> hashSymbol,
            Optional<Symbol> groupIdSymbol)
    {
        this(id, source, makeAssignments(assignments, functions, masks), groupingSets, step, hashSymbol, groupIdSymbol);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @JsonProperty
    public Map<Symbol, Aggregation> getAssignments()
    {
        return assignments;
    }

    /**
     * @deprecated Use getAssignments
     */
    @Deprecated
    public Map<Symbol, FunctionCall> getAggregations()
    {
        // use an ImmutableMap.Builder because the output has to preserve
        // the iteration order of the original map.
        ImmutableMap.Builder<Symbol, FunctionCall> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : assignments.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getCall());
        }
        return builder.build();
    }

    /**
     * @deprecated Use getAssignments
     */
    @Deprecated
    public Map<Symbol, Signature> getFunctions()
    {
        // use an ImmutableMap.Builder because the output has to preserve
        // the iteration order of the original map.
        ImmutableMap.Builder<Symbol, Signature> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : assignments.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getSignature());
        }
        return builder.build();
    }

    /**
     * @deprecated Use getAssignments
     */
    @Deprecated
    public Map<Symbol, Symbol> getMasks()
    {
        // use an ImmutableMap.Builder because the output has to preserve
        // the iteration order of the original map.
        ImmutableMap.Builder<Symbol, Symbol> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : assignments.entrySet()) {
            entry.getValue()
                    .getMask()
                    .ifPresent(symbol -> builder.put(entry.getKey(), symbol));
        }
        return builder.build();
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

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new AggregationNode(getId(), Iterables.getOnlyElement(newChildren), assignments, groupingSets, step, hashSymbol, groupIdSymbol);
    }

    private static Map<Symbol, Aggregation> makeAssignments(
            Map<Symbol, FunctionCall> aggregations,
            Map<Symbol, Signature> functions,
            Map<Symbol, Symbol> masks)
    {
        ImmutableMap.Builder<Symbol, Aggregation> builder = ImmutableMap.builder();

        for (Map.Entry<Symbol, FunctionCall> entry : aggregations.entrySet()) {
            Symbol output = entry.getKey();
            builder.put(output, new Aggregation(
                    entry.getValue(),
                    functions.get(output),
                    Optional.ofNullable(masks.get(output))));
        }

        return builder.build();
    }

    public static class Aggregation
    {
        private final FunctionCall call;
        private final Signature signature;
        private final Optional<Symbol> mask;

        @JsonCreator
        public Aggregation(
                @JsonProperty("call") FunctionCall call,
                @JsonProperty("signature") Signature signature,
                @JsonProperty("mask") Optional<Symbol> mask)
        {
            this.call = call;
            this.signature = signature;
            this.mask = mask;
        }

        @JsonProperty
        public FunctionCall getCall()
        {
            return call;
        }

        @JsonProperty
        public Signature getSignature()
        {
            return signature;
        }

        @JsonProperty
        public Optional<Symbol> getMask()
        {
            return mask;
        }
    }
}

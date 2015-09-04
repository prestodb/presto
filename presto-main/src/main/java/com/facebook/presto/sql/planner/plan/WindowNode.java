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
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.WindowFrame;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;

@Immutable
public class WindowNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> partitionBy;
    private final Set<Symbol> prePartitionedInputs;
    private final List<Symbol> orderBy;
    private final Map<Symbol, SortOrder> orderings;
    private final int preSortedOrderPrefix;
    private final Frame frame;
    private final Map<Symbol, FunctionCall> windowFunctions;
    private final Map<Symbol, Signature> functionHandles;
    private final Optional<Symbol> hashSymbol;

    @JsonCreator
    public WindowNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("partitionBy") List<Symbol> partitionBy,
            @JsonProperty("orderBy") List<Symbol> orderBy,
            @JsonProperty("orderings") Map<Symbol, SortOrder> orderings,
            @JsonProperty("frame") Frame frame,
            @JsonProperty("windowFunctions") Map<Symbol, FunctionCall> windowFunctions,
            @JsonProperty("signatures") Map<Symbol, Signature> signatures,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("prePartitionedInputs") Set<Symbol> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(partitionBy, "partitionBy is null");
        requireNonNull(orderBy, "orderBy is null");
        checkArgument(orderings.size() == orderBy.size(), "orderBy and orderings sizes don't match");
        checkArgument(orderings.keySet().containsAll(orderBy), "Every orderBy symbol must have an ordering direction");
        requireNonNull(frame, "frame is null");
        requireNonNull(windowFunctions, "windowFunctions is null");
        requireNonNull(signatures, "signatures is null");
        checkArgument(windowFunctions.keySet().equals(signatures.keySet()), "windowFunctions does not match signatures");
        requireNonNull(hashSymbol, "hashSymbol is null");
        checkArgument(partitionBy.containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        checkArgument(preSortedOrderPrefix <= orderBy.size(), "Cannot have sorted more symbols than those requested");
        checkArgument(preSortedOrderPrefix == 0 || ImmutableSet.copyOf(prePartitionedInputs).equals(ImmutableSet.copyOf(partitionBy)), "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");

        this.source = source;
        this.partitionBy = ImmutableList.copyOf(partitionBy);
        this.prePartitionedInputs = ImmutableSet.copyOf(prePartitionedInputs);
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.orderings = ImmutableMap.copyOf(orderings);
        this.frame = frame;
        this.windowFunctions = ImmutableMap.copyOf(windowFunctions);
        this.functionHandles = ImmutableMap.copyOf(signatures);
        this.hashSymbol = hashSymbol;
        this.preSortedOrderPrefix = preSortedOrderPrefix;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(concat(source.getOutputSymbols(), windowFunctions.keySet()));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<Symbol> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public List<Symbol> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public Map<Symbol, SortOrder> getOrderings()
    {
        return orderings;
    }

    @JsonProperty
    public Frame getFrame()
    {
        return frame;
    }

    @JsonProperty
    public Map<Symbol, FunctionCall> getWindowFunctions()
    {
        return windowFunctions;
    }

    @JsonProperty
    public Map<Symbol, Signature> getSignatures()
    {
        return functionHandles;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @JsonProperty
    public Set<Symbol> getPrePartitionedInputs()
    {
        return prePartitionedInputs;
    }

    @JsonProperty
    public int getPreSortedOrderPrefix()
    {
        return preSortedOrderPrefix;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }

    @Immutable
    public static class Frame
    {
        private final WindowFrame.Type type;
        private final FrameBound.Type startType;
        private final Optional<Symbol> startValue;
        private final FrameBound.Type endType;
        private final Optional<Symbol> endValue;

        @JsonCreator
        public Frame(
                @JsonProperty("type") WindowFrame.Type type,
                @JsonProperty("startType") FrameBound.Type startType,
                @JsonProperty("startValue") Optional<Symbol> startValue,
                @JsonProperty("endType") FrameBound.Type endType,
                @JsonProperty("endValue") Optional<Symbol> endValue)
        {
            this.startType = requireNonNull(startType, "startType is null");
            this.startValue = requireNonNull(startValue, "startValue is null");
            this.endType = requireNonNull(endType, "endType is null");
            this.endValue = requireNonNull(endValue, "endValue is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public WindowFrame.Type getType()
        {
            return type;
        }

        @JsonProperty
        public FrameBound.Type getStartType()
        {
            return startType;
        }

        @JsonProperty
        public Optional<Symbol> getStartValue()
        {
            return startValue;
        }

        @JsonProperty
        public FrameBound.Type getEndType()
        {
            return endType;
        }

        @JsonProperty
        public Optional<Symbol> getEndValue()
        {
            return endValue;
        }
    }
}

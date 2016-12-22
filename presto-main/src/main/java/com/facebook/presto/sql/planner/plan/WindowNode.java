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
import com.facebook.presto.util.ImmutableCollectors;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final Set<Symbol> prePartitionedInputs;
    private final Specification specification;
    private final int preSortedOrderPrefix;
    private final Map<Symbol, Function> windowFunctions;
    private final Optional<Symbol> hashSymbol;

    @JsonCreator
    public WindowNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") Specification specification,
            @JsonProperty("windowFunctions") Map<Symbol, Function> windowFunctions,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol,
            @JsonProperty("prePartitionedInputs") Set<Symbol> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        requireNonNull(windowFunctions, "windowFunctions is null");
        requireNonNull(hashSymbol, "hashSymbol is null");
        checkArgument(specification.getPartitionBy().containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        checkArgument(preSortedOrderPrefix <= specification.getOrderBy().size(), "Cannot have sorted more symbols than those requested");
        checkArgument(preSortedOrderPrefix == 0 || ImmutableSet.copyOf(prePartitionedInputs).equals(ImmutableSet.copyOf(specification.getPartitionBy())), "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");

        this.source = source;
        this.prePartitionedInputs = ImmutableSet.copyOf(prePartitionedInputs);
        this.specification = specification;
        this.windowFunctions = ImmutableMap.copyOf(windowFunctions);
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
    public Specification getSpecification()
    {
        return specification;
    }

    public List<Symbol> getPartitionBy()
    {
        return specification.getPartitionBy();
    }

    public List<Symbol> getOrderBy()
    {
        return specification.getOrderBy();
    }

    public Map<Symbol, SortOrder> getOrderings()
    {
        return specification.getOrderings();
    }

    @JsonProperty
    public Map<Symbol, Function> getWindowFunctions()
    {
        return windowFunctions;
    }

    public List<Frame> getFrames()
    {
        return windowFunctions.values().stream()
                .map(WindowNode.Function::getFrame)
                .collect(ImmutableCollectors.toImmutableList());
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

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new WindowNode(getId(), Iterables.getOnlyElement(newChildren), specification, windowFunctions, hashSymbol, prePartitionedInputs, preSortedOrderPrefix);
    }

    @Immutable
    public static class Specification
    {
        private final List<Symbol> partitionBy;
        private final List<Symbol> orderBy;
        private final Map<Symbol, SortOrder> orderings;

        @JsonCreator
        public Specification(
                @JsonProperty("partitionBy") List<Symbol> partitionBy,
                @JsonProperty("orderBy") List<Symbol> orderBy,
                @JsonProperty("orderings") Map<Symbol, SortOrder> orderings)
        {
            requireNonNull(partitionBy, "partitionBy is null");
            requireNonNull(orderBy, "orderBy is null");
            checkArgument(orderings.size() == orderBy.size(), "orderBy and orderings sizes don't match");
            checkArgument(orderings.keySet().containsAll(orderBy), "Every orderBy symbol must have an ordering direction");

            this.partitionBy = ImmutableList.copyOf(partitionBy);
            this.orderBy = ImmutableList.copyOf(orderBy);
            this.orderings = ImmutableMap.copyOf(orderings);
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

        @Override
        public int hashCode()
        {
            return Objects.hash(partitionBy, orderBy, orderings);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Specification other = (Specification) obj;

            return Objects.equals(this.partitionBy, other.partitionBy) &&
                    Objects.equals(this.orderBy, other.orderBy) &&
                    Objects.equals(this.orderings, other.orderings);
        }
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

        @Override
        public int hashCode()
        {
            return Objects.hash(type, startType, startValue, endType, endValue);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Frame other = (Frame) obj;
            return Objects.equals(this.type, other.type) &&
                    Objects.equals(this.startType, other.startType) &&
                    Objects.equals(this.startValue, other.startValue) &&
                    Objects.equals(this.endType, other.endType) &&
                    Objects.equals(this.endValue, other.endValue);
        }
    }

    @Immutable
    public static final class Function
    {
        private final FunctionCall functionCall;
        private final Signature signature;
        private final Frame frame;

        @JsonCreator
        public Function(
                @JsonProperty("functionCall") FunctionCall functionCall,
                @JsonProperty("signature") Signature signature,
                @JsonProperty("frame") Frame frame)
        {
            this.functionCall = requireNonNull(functionCall, "functionCall is null");
            this.signature = requireNonNull(signature, "Signature is null");
            this.frame = requireNonNull(frame, "Frame is null");
        }

        @JsonProperty
        public FunctionCall getFunctionCall()
        {
            return functionCall;
        }

        @JsonProperty
        public Signature getSignature()
        {
            return signature;
        }

        @JsonProperty
        public Frame getFrame()
        {
            return frame;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionCall, signature, frame);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Function other = (Function) obj;
            return Objects.equals(this.functionCall, other.functionCall) &&
                    Objects.equals(this.signature, other.signature) &&
                    Objects.equals(this.frame, other.frame);
        }
    }
}

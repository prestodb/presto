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

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class WindowNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final Set<VariableReferenceExpression> prePartitionedInputs;
    private final Specification specification;
    private final int preSortedOrderPrefix;
    private final Map<VariableReferenceExpression, Function> windowFunctions;
    private final Optional<VariableReferenceExpression> hashVariable;

    @JsonCreator
    public WindowNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") Specification specification,
            @JsonProperty("windowFunctions") Map<VariableReferenceExpression, Function> windowFunctions,
            @JsonProperty("hashVariable") Optional<VariableReferenceExpression> hashVariable,
            @JsonProperty("prePartitionedInputs") Set<VariableReferenceExpression> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        requireNonNull(windowFunctions, "windowFunctions is null");
        requireNonNull(hashVariable, "hashVariable is null");
        checkArgument(specification.getPartitionBy().containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        Optional<OrderingScheme> orderingScheme = specification.getOrderingScheme();
        checkArgument(preSortedOrderPrefix == 0 || (orderingScheme.isPresent() && preSortedOrderPrefix <= orderingScheme.get().getOrderByVariables().size()), "Cannot have sorted more symbols than those requested");
        checkArgument(preSortedOrderPrefix == 0 || ImmutableSet.copyOf(prePartitionedInputs).equals(ImmutableSet.copyOf(specification.getPartitionBy())), "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");

        this.source = source;
        this.prePartitionedInputs = ImmutableSet.copyOf(prePartitionedInputs);
        this.specification = specification;
        this.windowFunctions = ImmutableMap.copyOf(windowFunctions);
        this.hashVariable = hashVariable;
        this.preSortedOrderPrefix = preSortedOrderPrefix;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(source.getOutputVariables())
                .addAll(windowFunctions.keySet())
                .build();
    }

    public Set<VariableReferenceExpression> getCreatedVariable()
    {
        return windowFunctions.keySet();
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

    public List<VariableReferenceExpression> getPartitionBy()
    {
        return specification.getPartitionBy();
    }

    public Optional<OrderingScheme> getOrderingScheme()
    {
        return specification.orderingScheme;
    }

    @JsonProperty
    public Map<VariableReferenceExpression, Function> getWindowFunctions()
    {
        return windowFunctions;
    }

    public List<Frame> getFrames()
    {
        return windowFunctions.values().stream()
                .map(WindowNode.Function::getFrame)
                .collect(toImmutableList());
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @JsonProperty
    public Set<VariableReferenceExpression> getPrePartitionedInputs()
    {
        return prePartitionedInputs;
    }

    @JsonProperty
    public int getPreSortedOrderPrefix()
    {
        return preSortedOrderPrefix;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new WindowNode(getId(), Iterables.getOnlyElement(newChildren), specification, windowFunctions, hashVariable, prePartitionedInputs, preSortedOrderPrefix);
    }

    @Immutable
    public static class Specification
    {
        private final List<VariableReferenceExpression> partitionBy;
        private final Optional<OrderingScheme> orderingScheme;

        @JsonCreator
        public Specification(
                @JsonProperty("partitionBy") List<VariableReferenceExpression> partitionBy,
                @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme)
        {
            requireNonNull(partitionBy, "partitionBy is null");
            requireNonNull(orderingScheme, "orderingScheme is null");

            this.partitionBy = ImmutableList.copyOf(partitionBy);
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
        }

        @JsonProperty
        public List<VariableReferenceExpression> getPartitionBy()
        {
            return partitionBy;
        }

        @JsonProperty
        public Optional<OrderingScheme> getOrderingScheme()
        {
            return orderingScheme;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitionBy, orderingScheme);
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
                    Objects.equals(this.orderingScheme, other.orderingScheme);
        }
    }

    @Immutable
    public static class Frame
    {
        private final WindowType type;
        private final BoundType startType;
        private final Optional<VariableReferenceExpression> startValue;
        private final BoundType endType;
        private final Optional<VariableReferenceExpression> endValue;

        // This information is only used for printing the plan.
        private final Optional<String> originalStartValue;
        private final Optional<String> originalEndValue;

        @JsonCreator
        public Frame(
                @JsonProperty("type") WindowType type,
                @JsonProperty("startType") BoundType startType,
                @JsonProperty("startValue") Optional<VariableReferenceExpression> startValue,
                @JsonProperty("endType") BoundType endType,
                @JsonProperty("endValue") Optional<VariableReferenceExpression> endValue,
                @JsonProperty("originalStartValue") Optional<String> originalStartValue,
                @JsonProperty("originalEndValue") Optional<String> originalEndValue)
        {
            this.startType = requireNonNull(startType, "startType is null");
            this.startValue = requireNonNull(startValue, "startValue is null");
            this.endType = requireNonNull(endType, "endType is null");
            this.endValue = requireNonNull(endValue, "endValue is null");
            this.type = requireNonNull(type, "type is null");
            this.originalStartValue = requireNonNull(originalStartValue, "originalStartValue is null");
            this.originalEndValue = requireNonNull(originalEndValue, "originalEndValue is null");

            if (startValue.isPresent()) {
                checkArgument(originalStartValue.isPresent(), "originalStartValue must be present if startValue is present");
            }

            if (endValue.isPresent()) {
                checkArgument(originalEndValue.isPresent(), "originalEndValue must be present if endValue is present");
            }
        }

        @JsonProperty
        public WindowType getType()
        {
            return type;
        }

        @JsonProperty
        public BoundType getStartType()
        {
            return startType;
        }

        @JsonProperty
        public Optional<VariableReferenceExpression> getStartValue()
        {
            return startValue;
        }

        @JsonProperty
        public BoundType getEndType()
        {
            return endType;
        }

        @JsonProperty
        public Optional<VariableReferenceExpression> getEndValue()
        {
            return endValue;
        }

        @JsonProperty
        public Optional<String> getOriginalStartValue()
        {
            return originalStartValue;
        }

        @JsonProperty
        public Optional<String> getOriginalEndValue()
        {
            return originalEndValue;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Frame frame = (Frame) o;
            return type == frame.type &&
                    startType == frame.startType &&
                    Objects.equals(startValue, frame.startValue) &&
                    endType == frame.endType &&
                    Objects.equals(endValue, frame.endValue);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, startType, startValue, endType, endValue, originalStartValue, originalEndValue);
        }

        public enum WindowType
        {
            RANGE, ROWS
        }

        public enum BoundType
        {
            UNBOUNDED_PRECEDING,
            PRECEDING,
            CURRENT_ROW,
            FOLLOWING,
            UNBOUNDED_FOLLOWING
        }
    }

    @Immutable
    public static final class Function
    {
        private final CallExpression functionCall;
        private final Frame frame;
        private final boolean ignoreNulls;

        @JsonCreator
        public Function(
                @JsonProperty("functionCall") CallExpression functionCall,
                @JsonProperty("frame") Frame frame,
                @JsonProperty("ignoreNulls") boolean ignoreNulls)
        {
            this.functionCall = requireNonNull(functionCall, "functionCall is null");
            this.frame = requireNonNull(frame, "Frame is null");
            this.ignoreNulls = ignoreNulls;
        }

        @JsonProperty
        public CallExpression getFunctionCall()
        {
            return functionCall;
        }

        @JsonProperty
        public FunctionHandle getFunctionHandle()
        {
            return functionCall.getFunctionHandle();
        }

        @JsonProperty
        public Frame getFrame()
        {
            return frame;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionCall, frame, ignoreNulls);
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
                    Objects.equals(this.frame, other.frame) &&
                    Objects.equals(this.ignoreNulls, other.ignoreNulls);
        }

        @JsonProperty
        public boolean isIgnoreNulls()
        {
            return ignoreNulls;
        }
    }
}

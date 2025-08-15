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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.spi.plan.WindowNode.Frame.WindowType.RANGE;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public class WindowNode
        extends PlanNode
{
    private final PlanNode source;
    private final Set<VariableReferenceExpression> prePartitionedInputs;
    private final DataOrganizationSpecification specification;
    private final int preSortedOrderPrefix;
    private final Map<VariableReferenceExpression, Function> windowFunctions;
    private final Optional<VariableReferenceExpression> hashVariable;

    @JsonCreator
    public WindowNode(
            @JsonProperty("sourceLocation") Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("specification") DataOrganizationSpecification specification,
            @JsonProperty("windowFunctions") Map<VariableReferenceExpression, Function> windowFunctions,
            @JsonProperty("hashVariable") Optional<VariableReferenceExpression> hashVariable,
            @JsonProperty("prePartitionedInputs") Set<VariableReferenceExpression> prePartitionedInputs,
            @JsonProperty("preSortedOrderPrefix") int preSortedOrderPrefix)
    {
        this(sourceLocation, id, Optional.empty(), source, specification, windowFunctions, hashVariable, prePartitionedInputs, preSortedOrderPrefix);
    }

    public WindowNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            DataOrganizationSpecification specification,
            Map<VariableReferenceExpression, Function> windowFunctions,
            Optional<VariableReferenceExpression> hashVariable,
            Set<VariableReferenceExpression> prePartitionedInputs,
            int preSortedOrderPrefix)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        requireNonNull(source, "source is null");
        requireNonNull(specification, "specification is null");
        requireNonNull(windowFunctions, "windowFunctions is null");
        requireNonNull(hashVariable, "hashVariable is null");
        checkArgument(specification.getPartitionBy().containsAll(prePartitionedInputs), "prePartitionedInputs must be contained in partitionBy");
        Optional<OrderingScheme> orderingScheme = specification.getOrderingScheme();
        checkArgument(preSortedOrderPrefix == 0 || (orderingScheme.isPresent() && preSortedOrderPrefix <= orderingScheme.get().getOrderByVariables().size()), "Cannot have sorted more symbols than those requested");
        checkArgument(preSortedOrderPrefix == 0 || prePartitionedInputs.equals(new HashSet<>(specification.getPartitionBy())), "preSortedOrderPrefix can only be greater than zero if all partition symbols are pre-partitioned");

        this.source = source;
        this.prePartitionedInputs = unmodifiableSet(new LinkedHashSet<>(prePartitionedInputs));
        this.specification = specification;
        this.windowFunctions = unmodifiableMap(new LinkedHashMap<>(windowFunctions));
        this.hashVariable = hashVariable;
        this.preSortedOrderPrefix = preSortedOrderPrefix;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return singletonList(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        List<VariableReferenceExpression> outputVariables = new ArrayList<>(source.getOutputVariables());
        outputVariables.addAll(windowFunctions.keySet());
        return unmodifiableList(outputVariables);
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
    public DataOrganizationSpecification getSpecification()
    {
        return specification;
    }

    public List<VariableReferenceExpression> getPartitionBy()
    {
        return specification.getPartitionBy();
    }

    public Optional<OrderingScheme> getOrderingScheme()
    {
        return specification.getOrderingScheme();
    }

    @JsonProperty
    public Map<VariableReferenceExpression, Function> getWindowFunctions()
    {
        return windowFunctions;
    }

    public List<Frame> getFrames()
    {
        List<Frame> frames = windowFunctions.values().stream()
                .map(WindowNode.Function::getFrame)
                .collect(Collectors.toList());
        return unmodifiableList(frames);
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
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1);
        return new WindowNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), specification, windowFunctions, hashVariable, prePartitionedInputs, preSortedOrderPrefix);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new WindowNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, specification, windowFunctions, hashVariable, prePartitionedInputs, preSortedOrderPrefix);
    }

    @Immutable
    public static class Frame
    {
        private final WindowType type;
        private final BoundType startType;
        private final Optional<VariableReferenceExpression> startValue;
        // Sort key coerced to the same type of range start expression, for comparing and deciding frame start for range expression
        private final Optional<VariableReferenceExpression> sortKeyCoercedForFrameStartComparison;
        private final BoundType endType;
        private final Optional<VariableReferenceExpression> endValue;
        // Sort key coerced to the same type of range end expression, for comparing and deciding frame end for range expression
        private final Optional<VariableReferenceExpression> sortKeyCoercedForFrameEndComparison;

        // This information is only used for printing the plan.
        private final Optional<String> originalStartValue;
        private final Optional<String> originalEndValue;

        @JsonCreator
        public Frame(
                @JsonProperty("type") WindowType type,
                @JsonProperty("startType") BoundType startType,
                @JsonProperty("startValue") Optional<VariableReferenceExpression> startValue,
                @JsonProperty("sortKeyCoercedForFrameStartComparison") Optional<VariableReferenceExpression> sortKeyCoercedForFrameStartComparison,
                @JsonProperty("endType") BoundType endType,
                @JsonProperty("endValue") Optional<VariableReferenceExpression> endValue,
                @JsonProperty("sortKeyCoercedForFrameEndComparison") Optional<VariableReferenceExpression> sortKeyCoercedForFrameEndComparison,
                @JsonProperty("originalStartValue") Optional<String> originalStartValue,
                @JsonProperty("originalEndValue") Optional<String> originalEndValue)
        {
            this.startType = requireNonNull(startType, "startType is null");
            this.startValue = requireNonNull(startValue, "startValue is null");
            this.sortKeyCoercedForFrameStartComparison = requireNonNull(sortKeyCoercedForFrameStartComparison, "sortKeyCoercedForFrameStartComparison is null");
            this.endType = requireNonNull(endType, "endType is null");
            this.endValue = requireNonNull(endValue, "endValue is null");
            this.sortKeyCoercedForFrameEndComparison = requireNonNull(sortKeyCoercedForFrameEndComparison, "sortKeyCoercedForFrameEndComparison is null");
            this.type = requireNonNull(type, "type is null");
            this.originalStartValue = requireNonNull(originalStartValue, "originalStartValue is null");
            this.originalEndValue = requireNonNull(originalEndValue, "originalEndValue is null");

            if (startValue.isPresent()) {
                checkArgument(originalStartValue.isPresent(), "originalStartValue must be present if startValue is present");
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameStartComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameStartComparison must be present if startValue is present");
                }
            }

            if (endValue.isPresent()) {
                checkArgument(originalEndValue.isPresent(), "originalEndValue must be present if endValue is present");
                if (type == RANGE) {
                    checkArgument(sortKeyCoercedForFrameEndComparison.isPresent(), "for frame of type RANGE, sortKeyCoercedForFrameEndComparison must be present if endValue is present");
                }
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
        public Optional<VariableReferenceExpression> getSortKeyCoercedForFrameStartComparison()
        {
            return sortKeyCoercedForFrameStartComparison;
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
        public Optional<VariableReferenceExpression> getSortKeyCoercedForFrameEndComparison()
        {
            return sortKeyCoercedForFrameEndComparison;
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
                    Objects.equals(sortKeyCoercedForFrameStartComparison, frame.sortKeyCoercedForFrameStartComparison) &&
                    endType == frame.endType &&
                    Objects.equals(endValue, frame.endValue) &&
                    Objects.equals(sortKeyCoercedForFrameEndComparison, frame.sortKeyCoercedForFrameEndComparison);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, startType, startValue, sortKeyCoercedForFrameStartComparison, endType, endValue, originalStartValue, originalEndValue, sortKeyCoercedForFrameEndComparison);
        }

        public enum WindowType
        {
            RANGE, ROWS, GROUPS
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

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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
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

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class AggregationNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final Map<VariableReferenceExpression, Aggregation> aggregations;
    private final GroupingSetDescriptor groupingSets;
    private final List<VariableReferenceExpression> preGroupedVariables;
    private final Step step;
    private final Optional<VariableReferenceExpression> hashVariable;
    private final Optional<VariableReferenceExpression> groupIdVariable;
    private final List<VariableReferenceExpression> outputs;

    @JsonCreator
    public AggregationNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("aggregations") Map<VariableReferenceExpression, Aggregation> aggregations,
            @JsonProperty("groupingSets") GroupingSetDescriptor groupingSets,
            @JsonProperty("preGroupedVariables") List<VariableReferenceExpression> preGroupedVariables,
            @JsonProperty("step") Step step,
            @JsonProperty("hashVariable") Optional<VariableReferenceExpression> hashVariable,
            @JsonProperty("groupIdVariable") Optional<VariableReferenceExpression> groupIdVariable)
    {
        super(id);

        this.source = source;
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));

        requireNonNull(groupingSets, "groupingSets is null");
        groupIdVariable.ifPresent(variable -> checkArgument(groupingSets.getGroupingKeys().contains(variable), "Grouping columns does not contain groupId column"));
        this.groupingSets = groupingSets;

        this.groupIdVariable = requireNonNull(groupIdVariable);

        boolean noOrderBy = aggregations.values().stream()
                .map(Aggregation::getOrderBy)
                .noneMatch(Optional::isPresent);
        checkArgument(noOrderBy || step == SINGLE, "ORDER BY does not support distributed aggregation");

        this.step = step;
        this.hashVariable = hashVariable;

        requireNonNull(preGroupedVariables, "preGroupedVariables is null");
        checkArgument(preGroupedVariables.isEmpty() || groupingSets.getGroupingKeys().containsAll(preGroupedVariables), "Pre-grouped variables must be a subset of the grouping keys");
        this.preGroupedVariables = ImmutableList.copyOf(preGroupedVariables);

        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        outputs.addAll(groupingSets.getGroupingKeys());
        hashVariable.ifPresent(outputs::add);
        outputs.addAll(aggregations.keySet());

        this.outputs = outputs.build();
    }

    public List<VariableReferenceExpression> getGroupingKeys()
    {
        return groupingSets.getGroupingKeys();
    }

    @JsonProperty
    public GroupingSetDescriptor getGroupingSets()
    {
        return groupingSets;
    }

    /**
     * @return whether this node should produce default output in case of no input pages.
     * For example for query:
     * <p>
     * SELECT count(*) FROM nation WHERE nationkey < 0
     * <p>
     * A default output of "0" is expected to be produced by FINAL aggregation operator.
     */
    public boolean hasDefaultOutput()
    {
        return hasEmptyGroupingSet() && (step.isOutputPartial() || step.equals(SINGLE));
    }

    public boolean hasEmptyGroupingSet()
    {
        return !groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public boolean hasNonEmptyGroupingSet()
    {
        return groupingSets.getGroupingSetCount() > groupingSets.getGlobalGroupingSets().size();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    @JsonProperty
    public Map<VariableReferenceExpression, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getPreGroupedVariables()
    {
        return preGroupedVariables;
    }

    public int getGroupingSetCount()
    {
        return groupingSets.getGroupingSetCount();
    }

    public Set<Integer> getGlobalGroupingSets()
    {
        return groupingSets.getGlobalGroupingSets();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Step getStep()
    {
        return step;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getGroupIdVariable()
    {
        return groupIdVariable;
    }

    public boolean hasOrderings()
    {
        return aggregations.values().stream()
                .map(Aggregation::getOrderBy)
                .anyMatch(Optional::isPresent);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new AggregationNode(getId(), Iterables.getOnlyElement(newChildren), aggregations, groupingSets, preGroupedVariables, step, hashVariable, groupIdVariable);
    }

    public boolean isDecomposable(FunctionManager functionManager)
    {
        boolean hasOrderBy = getAggregations().values().stream()
                .map(Aggregation::getOrderBy)
                .anyMatch(Optional::isPresent);

        boolean hasDistinct = getAggregations().values().stream()
                .anyMatch(Aggregation::isDistinct);

        boolean decomposableFunctions = getAggregations().values().stream()
                .map(Aggregation::getFunctionHandle)
                .map(functionManager::getAggregateFunctionImplementation)
                .allMatch(InternalAggregationFunction::isDecomposable);

        return !hasOrderBy && !hasDistinct && decomposableFunctions;
    }

    public boolean hasSingleNodeExecutionPreference(FunctionManager functionManager)
    {
        // There are two kinds of aggregations the have single node execution preference:
        //
        // 1. aggregations with only empty grouping sets like
        //
        // SELECT count(*) FROM lineitem;
        //
        // there is no need for distributed aggregation. Single node FINAL aggregation will suffice,
        // since all input have to be aggregated into one line output.
        //
        // 2. aggregations that must produce default output and are not decomposable, we can not distribute them.
        return (hasEmptyGroupingSet() && !hasNonEmptyGroupingSet()) || (hasDefaultOutput() && !isDecomposable(functionManager));
    }

    public boolean isStreamable()
    {
        return !preGroupedVariables.isEmpty() && groupingSets.getGroupingSetCount() == 1 && groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public static GroupingSetDescriptor globalAggregation()
    {
        return singleGroupingSet(ImmutableList.of());
    }

    public static GroupingSetDescriptor singleGroupingSet(List<VariableReferenceExpression> groupingKeys)
    {
        Set<Integer> globalGroupingSets;
        if (groupingKeys.isEmpty()) {
            globalGroupingSets = ImmutableSet.of(0);
        }
        else {
            globalGroupingSets = ImmutableSet.of();
        }

        return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
    }

    public static GroupingSetDescriptor groupingSets(List<VariableReferenceExpression> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
    {
        return new GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
    }

    public static class GroupingSetDescriptor
    {
        private final List<VariableReferenceExpression> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        @JsonCreator
        public GroupingSetDescriptor(
                @JsonProperty("groupingKeys") List<VariableReferenceExpression> groupingKeys,
                @JsonProperty("groupingSetCount") int groupingSetCount,
                @JsonProperty("globalGroupingSets") Set<Integer> globalGroupingSets)
        {
            requireNonNull(globalGroupingSets, "globalGroupingSets is null");
            checkArgument(globalGroupingSets.size() <= groupingSetCount, "list of empty global grouping sets must be no larger than grouping set count");
            requireNonNull(groupingKeys, "groupingKeys is null");
            if (groupingKeys.isEmpty()) {
                checkArgument(!globalGroupingSets.isEmpty(), "no grouping keys implies at least one global grouping set, but none provided");
            }

            this.groupingKeys = ImmutableList.copyOf(groupingKeys);
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = ImmutableSet.copyOf(globalGroupingSets);
        }

        @JsonProperty
        public List<VariableReferenceExpression> getGroupingKeys()
        {
            return groupingKeys;
        }

        @JsonProperty
        public int getGroupingSetCount()
        {
            return groupingSetCount;
        }

        @JsonProperty
        public Set<Integer> getGlobalGroupingSets()
        {
            return globalGroupingSets;
        }
    }

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

    public static class Aggregation
    {
        private final CallExpression call;
        private final Optional<RowExpression> filter;
        private final Optional<OrderingScheme> orderingScheme;
        private final boolean isDistinct;
        private final Optional<VariableReferenceExpression> mask;

        @JsonCreator
        public Aggregation(
                @JsonProperty("call") CallExpression call,
                @JsonProperty("filter") Optional<RowExpression> filter,
                @JsonProperty("orderBy") Optional<OrderingScheme> orderingScheme,
                @JsonProperty("distinct") boolean isDistinct,
                @JsonProperty("mask") Optional<VariableReferenceExpression> mask)
        {
            this.call = requireNonNull(call, "call is null");
            this.filter = requireNonNull(filter, "filter is null");
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
            this.isDistinct = isDistinct;
            this.mask = requireNonNull(mask, "mask is null");
        }

        @JsonProperty
        public CallExpression getCall()
        {
            return call;
        }

        @JsonProperty
        public FunctionHandle getFunctionHandle()
        {
            return call.getFunctionHandle();
        }

        @JsonProperty
        public List<RowExpression> getArguments()
        {
            return call.getArguments();
        }

        @JsonProperty
        public Optional<OrderingScheme> getOrderBy()
        {
            return orderingScheme;
        }

        @JsonProperty
        public Optional<RowExpression> getFilter()
        {
            return filter;
        }

        @JsonProperty
        public boolean isDistinct()
        {
            return isDistinct;
        }

        @JsonProperty
        public Optional<VariableReferenceExpression> getMask()
        {
            return mask;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Aggregation)) {
                return false;
            }
            Aggregation that = (Aggregation) o;
            return isDistinct == that.isDistinct &&
                    Objects.equals(call, that.call) &&
                    Objects.equals(filter, that.filter) &&
                    Objects.equals(orderingScheme, that.orderingScheme) &&
                    Objects.equals(mask, that.mask);
        }

        @Override
        public String toString()
        {
            return "Aggregation{" +
                    "call=" + call +
                    ", filter=" + filter +
                    ", orderingScheme=" + orderingScheme +
                    ", isDistinct=" + isDistinct +
                    ", mask=" + mask +
                    '}';
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(call, filter, orderingScheme, isDistinct, mask);
        }
    }
}

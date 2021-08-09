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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinNode.Type.FULL;
import static com.facebook.presto.spi.plan.JoinNode.Type.INNER;
import static com.facebook.presto.spi.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.spi.plan.JoinNode.Type.RIGHT;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public class JoinNode
        extends AbstractJoinNode
{
    private final Type type;
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;
    private final List<VariableReferenceExpression> outputVariables;
    private final Optional<RowExpression> filter;
    private final Optional<VariableReferenceExpression> leftHashVariable;
    private final Optional<VariableReferenceExpression> rightHashVariable;
    private final Optional<DistributionType> distributionType;
    private final Map<String, VariableReferenceExpression> dynamicFilters;

    @JsonCreator
    public JoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("filter") Optional<RowExpression> filter,
            @JsonProperty("leftHashVariable") Optional<VariableReferenceExpression> leftHashVariable,
            @JsonProperty("rightHashVariable") Optional<VariableReferenceExpression> rightHashVariable,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType,
            @JsonProperty("dynamicFilters") Map<String, VariableReferenceExpression> dynamicFilters)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(outputVariables, "outputVariables is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashVariable, "leftHashVariable is null");
        requireNonNull(rightHashVariable, "rightHashVariable is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(dynamicFilters, "dynamicFilters is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = unmodifiableList(criteria);
        this.outputVariables = unmodifiableList(outputVariables);
        this.filter = filter;
        this.leftHashVariable = leftHashVariable;
        this.rightHashVariable = rightHashVariable;
        this.distributionType = distributionType;
        this.dynamicFilters = unmodifiableMap(dynamicFilters);

        Set<VariableReferenceExpression> inputVariables = immutableSetCopyOf(left.getOutputVariables(), right.getOutputVariables());

        checkArgument(new HashSet<>(inputVariables).containsAll(outputVariables), "Left and right join inputs do not contain all output variables");
        checkArgument(!isCrossJoin() || inputVariables.size() == outputVariables.size(), "Cross join does not support output variables pruning or reordering");

        checkArgument(!(criteria.isEmpty() && leftHashVariable.isPresent()), "Left hash variable is only valid in an equijoin");
        checkArgument(!(criteria.isEmpty() && rightHashVariable.isPresent()), "Right hash variable is only valid in an equijoin");

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && type.mustPartition()),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
            // It does not make sense to PARTITION when there is nothing to partition on
            checkArgument(
                    !(distributionType.get() == PARTITIONED && type.mustReplicate(criteria)),
                    "Equi criteria are empty, so %s join should not have %s distribution type",
                    type,
                    distributionType.get());
        }

        for (VariableReferenceExpression variableReferenceExpression : dynamicFilters.values()) {
            checkArgument(right.getOutputVariables().contains(variableReferenceExpression), format(
                    "Right join input doesn't contain symbol for dynamic filter: %s, rightVariables: %s, dynamicFilters.values(): %s",
                    variableReferenceExpression,
                    Arrays.toString(right.getOutputVariables().toArray()),
                    Arrays.toString(dynamicFilters.values().toArray())));
        }
    }

    public JoinNode flipChildren()
    {
        return new JoinNode(
                getId(),
                flipType(type),
                right,
                left,
                flipJoinCriteria(criteria),
                flipOutputVariables(getOutputVariables(), left, right),
                filter,
                rightHashVariable,
                leftHashVariable,
                distributionType,
                emptyMap()); // dynamicFilters are invalid after flipping children
    }

    private static Type flipType(Type type)
    {
        switch (type) {
            case INNER:
                return INNER;
            case FULL:
                return FULL;
            case LEFT:
                return RIGHT;
            case RIGHT:
                return LEFT;
            default:
                throw new IllegalStateException("No inverse defined for join type: " + type);
        }
    }

    private static List<EquiJoinClause> flipJoinCriteria(List<EquiJoinClause> joinCriteria)
    {
        return joinCriteria.stream()
                .map(EquiJoinClause::flip)
                .collect(toList());
    }

    private static List<VariableReferenceExpression> flipOutputVariables(List<VariableReferenceExpression> outputVariables, PlanNode left, PlanNode right)
    {
        List<VariableReferenceExpression> leftVariables = outputVariables.stream()
                .filter(variable -> left.getOutputVariables().contains(variable))
                .collect(Collectors.toList());
        List<VariableReferenceExpression> rightVariables = outputVariables.stream()
                .filter(variable -> right.getOutputVariables().contains(variable))
                .collect(Collectors.toList());
        return immutableListCopyOf(rightVariables, leftVariables);
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public boolean mustPartition()
        {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            return this == RIGHT || this == FULL;
        }

        public boolean mustReplicate(List<JoinNode.EquiJoinClause> criteria)
        {
            // There is nothing to partition on
            return criteria.isEmpty() && (this == INNER || this == LEFT);
        }
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public PlanNode getLeft()
    {
        return left;
    }

    @Override
    public PlanNode getProbe()
    {
        return left;
    }

    @JsonProperty
    public PlanNode getRight()
    {
        return right;
    }

    @Override
    public PlanNode getBuild()
    {
        return right;
    }

    @JsonProperty
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public Optional<RowExpression> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getLeftHashVariable()
    {
        return leftHashVariable;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getRightHashVariable()
    {
        return rightHashVariable;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return unmodifiableList(Arrays.asList(left, right));
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @Override
    @JsonProperty
    public Map<String, VariableReferenceExpression> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new JoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria, outputVariables, filter, leftHashVariable, rightHashVariable, distributionType, dynamicFilters);
    }

    public JoinNode withDistributionType(DistributionType distributionType)
    {
        return new JoinNode(getId(), type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, Optional.of(distributionType), dynamicFilters);
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && !filter.isPresent() && type == INNER;
    }

    private static void checkArgument(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private static void checkArgument(boolean condition, String messageFormat, Object... args)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(messageFormat, args));
        }
    }

    private static <T> Set<T> immutableSetCopyOf(Collection<T> collection1, Collection<T> collection2)
    {
        TreeSet<T> treeSet = new TreeSet<>(requireNonNull(collection1, "collection is null"));
        treeSet.addAll(requireNonNull(collection2, "collection is null"));
        return unmodifiableSet(treeSet);
    }

    @SafeVarargs
    private static <T> List<T> immutableListCopyOf(Collection<T>... collections)
    {
        List<T> lists = new ArrayList<>();
        for (Collection<T> collection : collections) {
            lists.addAll(requireNonNull(collection, "collection is null"));
        }
        return unmodifiableList(lists);
    }

    public static class EquiJoinClause
    {
        private final VariableReferenceExpression left;
        private final VariableReferenceExpression right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") VariableReferenceExpression left, @JsonProperty("right") VariableReferenceExpression right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty
        public VariableReferenceExpression getLeft()
        {
            return left;
        }

        @JsonProperty
        public VariableReferenceExpression getRight()
        {
            return right;
        }

        public EquiJoinClause flip()
        {
            return new EquiJoinClause(right, left);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            EquiJoinClause other = (EquiJoinClause) obj;

            return Objects.equals(this.left, other.left) &&
                    Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        @Override
        public String toString()
        {
            return format("%s = %s", left, right);
        }
    }
}

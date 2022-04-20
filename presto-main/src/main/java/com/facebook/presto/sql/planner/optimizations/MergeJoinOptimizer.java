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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MergeJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isGroupedExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.preferMergeJoin;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MergeJoinOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser parser;

    public MergeJoinOptimizer(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider type, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (preferMergeJoin(session) && isGroupedExecutionEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new MergeJoinOptimizer.Rewriter(variableAllocator, idAllocator, metadata, session), plan, null);
        }
        return plan;
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;
        private final TypeProvider types;

        private Rewriter(PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.types = variableAllocator.getTypes();
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            // For example: when we have a plan that looks like:
            // JoinNode
            //- TableScanA
            //- TableScanB

            // We check the data properties of TableScanA and TableScanB to see if they meet requirements for merge join:
            // 1. If so, we replace the JoinNode to MergeJoinNode
            // MergeJoinNode
            //- TableScanA
            //- TableScanB

            // 2. If not, we don't optimize

            Optional<SortingPropertyPair> orderPropertyPair = getMergeJoinOrderPropertyPair(node.getLeft(), node.getRight(), node);
            if (orderPropertyPair.isPresent()) {
                return new MergeJoinNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getType(),
                        node.getLeft(),
                        node.getRight(),
                        node.getCriteria(),
                        orderPropertyPair.get().getLeftSortingProperties(),
                        orderPropertyPair.get().getRightSortingProperties(),
                        node.getOutputVariables(),
                        node.getFilter(),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable());
            }
            return node;
        }

        private Optional<SortingPropertyPair> getMergeJoinOrderPropertyPair(PlanNode left, PlanNode right, JoinNode node)
        {
            // Acquire data properties for both left and right side
            StreamPropertyDerivations.StreamProperties leftProperties = StreamPropertyDerivations.derivePropertiesRecursively(left, metadata, session, types, parser);
            StreamPropertyDerivations.StreamProperties rightProperties = StreamPropertyDerivations.derivePropertiesRecursively(right, metadata, session, types, parser);
            List<VariableReferenceExpression> leftJoinColumns = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(toImmutableList());
            List<VariableReferenceExpression> rightJoinColumns = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(toImmutableList());
            List<LocalProperty<VariableReferenceExpression>> leftSortingProperties = leftProperties.getLocalProperties().stream().filter(SortingProperty.class::isInstance).collect(Collectors.toList());
            List<LocalProperty<VariableReferenceExpression>> rightSortingProperties = rightProperties.getLocalProperties().stream().filter(SortingProperty.class::isInstance).collect(Collectors.toList());

            // Check if the left side and right side's partitioning columns (bucketed-by columns) are a subset of join columns
            // B = subset (J)
            if (!verifyStreamProperties(leftProperties, leftJoinColumns) || !verifyStreamProperties(rightProperties, rightJoinColumns)) {
                return Optional.empty();
            }

            // Check if the join columns has the same elements as the prefix of the sorted-by keys for both left and right sides
            // J = Set (prefix(S))
            if (!verifyLocalProperties(leftSortingProperties, leftJoinColumns) || !verifyLocalProperties(rightSortingProperties, rightJoinColumns)) {
                return Optional.empty();
            }

            // Check If left and right join columns pairs match the sorting property pairs (positions match and ASC/DESC match)
            return orderPropertyPairingMatch(leftSortingProperties, leftJoinColumns, rightSortingProperties, rightJoinColumns);
        }

        private boolean verifyStreamProperties(StreamPropertyDerivations.StreamProperties streamProperties, List<VariableReferenceExpression> joinColumns)
        {
            if (streamProperties.getDistribution() == SINGLE) {
                return true;
            }
            else if (!streamProperties.getPartitioningColumns().isPresent()) {
                return false;
            }
            else {
                List<VariableReferenceExpression> partitioningColumns = streamProperties.getPartitioningColumns().get();
                return partitioningColumns.size() <= joinColumns.size() && joinColumns.containsAll(partitioningColumns);
            }
        }

        private boolean verifyLocalProperties(List<LocalProperty<VariableReferenceExpression>> sortingProperties, List<VariableReferenceExpression> joinColumns)
        {
            // Logic in LocalProperties.match(sortingProperties, joinColumns)
            // 1. Extract the longest prefix of sortingProperties to a set that is a subset of joinColumns
            // 2. Iterate join columns and add the elements that's not in the set to the result
            // Result would be a List of one element: Optional<GroupingProperty>, GroupingProperty would contain one/multiple elements from step 2
            // Eg:
            // [A, B] [(B, A)]     ->   List.of(Optional.empty())
            // [A, B] [B]          ->   List.of(Optional.of(GroupingProperty(B)))
            // [A, B] [A]          ->   List.of(Optional.empty())
            // [A, B] [(A, C)]     ->   List.of(Optional.of(GroupingProperty(C)))
            // [A, B] [(D, A, C)]  ->   List.of(Optional.of(GroupingProperty(D, C)))

            // !isPresent() indicates the property was satisfied completely
            return !LocalProperties.match(sortingProperties, LocalProperties.grouped(joinColumns)).get(0).isPresent();
        }

        private Optional<SortingPropertyPair> orderPropertyPairingMatch(List<LocalProperty<VariableReferenceExpression>> leftSortingProperties, List<VariableReferenceExpression> leftJoinColumns, List<LocalProperty<VariableReferenceExpression>> rightSortingProperties, List<VariableReferenceExpression> rightJoinColumns)
        {
            // 1. Check if join columns pairs match the sorting property pairs
            // For example: Table A is sorted by [A, B], Table B is sorted by [C, D]
            // Join criteria:
            //      A = C, B = D would work
            //      A = D, B = C wouldn't work
            //      B = D, A = C would work

            // 2. Check sorting property ASC/DESC matching
            // For example: Table A is sorted by [A DESC, B], Table B is sorted by [C ASC, D]
            // Join criteria:
            //      A = C, B = D wouldn't work
            int joinColumnSize = leftJoinColumns.size();
            for (int i = 0; i < joinColumnSize; i++) {
                VariableReferenceExpression leftJoinColumn = leftJoinColumns.get(i);
                VariableReferenceExpression rightJoinColumn = rightJoinColumns.get(i);
                int leftPosition = 0;
                SortingProperty leftSortingProperty = null;
                for (; leftPosition < leftSortingProperties.size(); leftPosition++) {
                    leftSortingProperty = (SortingProperty) leftSortingProperties.get(leftPosition);
                    if (leftSortingProperty.getColumn().equals(leftJoinColumn)) {
                        break;
                    }
                }
                int rightPosition = 0;
                SortingProperty rightSortingProperty = null;
                for (; rightPosition < rightSortingProperties.size(); rightPosition++) {
                    rightSortingProperty = (SortingProperty) rightSortingProperties.get(rightPosition);
                    if (rightSortingProperty.getColumn().equals(rightJoinColumn)) {
                        break;
                    }
                }

                if (leftPosition != rightPosition
                        || !leftSortingProperty.getOrder().equals(rightSortingProperty.getOrder())
                        || leftSortingProperty.isOrderSensitive() != rightSortingProperty.isOrderSensitive() ) {
                    return Optional.empty();
                }
            }
            return Optional.of(new SortingPropertyPair(leftSortingProperties.subList(0, leftJoinColumns.size()), rightSortingProperties.subList(0, rightJoinColumns.size())));
        }
    }

    private class SortingPropertyPair
    {
        private List<SortingProperty<VariableReferenceExpression>> leftSortingProperties;
        private List<SortingProperty<VariableReferenceExpression>> rightSortingProperties;

        public SortingPropertyPair(List<LocalProperty<VariableReferenceExpression>> leftSortingProperties, List<LocalProperty<VariableReferenceExpression>> rightSortingProperties) {
            this.leftSortingProperties = leftSortingProperties.stream().map(property -> (SortingProperty<VariableReferenceExpression>) property).collect(Collectors.toList());
            this.rightSortingProperties = rightSortingProperties.stream().map(property -> (SortingProperty<VariableReferenceExpression>) property).collect(Collectors.toList());
        }

        public List<SortingProperty<VariableReferenceExpression>> getLeftSortingProperties()
        {
            return leftSortingProperties;
        }

        public void setLeftSortingProperties(List<SortingProperty<VariableReferenceExpression>> leftSortingProperties)
        {
            this.leftSortingProperties = leftSortingProperties;
        }

        public List<SortingProperty<VariableReferenceExpression>> getRightSortingProperties()
        {
            return rightSortingProperties;
        }

        public void setRightSortingProperties(List<SortingProperty<VariableReferenceExpression>> rightSortingProperties)
        {
            this.rightSortingProperties = rightSortingProperties;
        }
    }
}

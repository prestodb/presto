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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TableWriterNode.CallDistributedProcedureTarget;
import com.facebook.presto.spi.plan.TopNRowNumberNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.isPartitionAwareGroupedExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.preferSortMergeJoin;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PLAN_ERROR;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_PAGE_SINK_COMMIT;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_REWINDABLE_SPLIT_SOURCE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class GroupedExecutionTagger
        extends InternalPlanVisitor<GroupedExecutionTagger.GroupedExecutionProperties, Void>
{
    private final Session session;
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final boolean groupedExecutionEnabled;
    private final boolean partitionAwareEnabled;
    private final boolean isPrestoOnSpark;

    public GroupedExecutionTagger(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager, boolean groupedExecutionEnabled, boolean isPrestoOnSpark)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.groupedExecutionEnabled = groupedExecutionEnabled;
        this.partitionAwareEnabled = isPartitionAwareGroupedExecutionEnabled(session);
        this.isPrestoOnSpark = isPrestoOnSpark;
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitPlan(PlanNode node, Void context)
    {
        if (node.getSources().isEmpty()) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        return processChildren(node);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitJoin(JoinNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties left = node.getLeft().accept(this, null);
        GroupedExecutionTagger.GroupedExecutionProperties right = node.getRight().accept(this, null);

        if (!node.getDistributionType().isPresent() || !groupedExecutionEnabled) {
            // This is possible when the optimizers is invoked with `noExchange` set to true.
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }

        if ((node.getType() == JoinType.RIGHT || node.getType() == JoinType.FULL) && !right.currentNodeCapable) {
            // For a plan like this, if the fragment participates in grouped execution,
            // the LookupOuterOperator corresponding to the RJoin will not work execute properly.
            //
            // * The operator has to execute as not-grouped because it can only look at the "used" flags in
            //   join build after all probe has finished.
            // * The operator has to execute as grouped the subsequent LJoin expects that incoming
            //   operators are grouped. Otherwise, the LJoin won't be able to throw out the build side
            //   for each group as soon as the group completes.
            //
            //       LJoin
            //       /   \
            //   RJoin   Scan
            //   /   \
            // Scan Remote
            //
            // TODO:
            // The RJoin can still execute as grouped if there is no subsequent operator that depends
            // on the RJoin being executed in a grouped manner. However, this is not currently implemented.
            // Support for this scenario is already implemented in the execution side.
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }

        switch (node.getDistributionType().get()) {
            case REPLICATED:
                // Broadcast join maintains partitioning for the left side.
                // Right side of a broadcast is not capable of grouped execution because it always comes from a remote exchange.
                checkState(!right.currentNodeCapable);
                return left;
            case PARTITIONED:
                if (left.currentNodeCapable && right.currentNodeCapable) {
                    return mergeJoinSides(left, right, node.getCriteria());
                }
                // right.subTreeUseful && !left.currentNodeCapable:
                //   It's not particularly helpful to do grouped execution on the right side
                //   because the benefit is likely cancelled out due to required buffering for hash build.
                //   In theory, it could still be helpful (e.g. when the underlying aggregation's intermediate group state maybe larger than aggregation output).
                //   However, this is not currently implemented. JoinBridgeManager need to support such a lifecycle.
                // !right.currentNodeCapable:
                //   The build/right side needs to buffer fully for this JOIN, but the probe/left side will still stream through.
                //   As a result, there is no reason to change currentNodeCapable or subTreeUseful to false.
                //
                return left;
            default:
                throw new UnsupportedOperationException("Unknown distribution type: " + node.getDistributionType());
        }
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitMergeJoin(MergeJoinNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties left = node.getLeft().accept(this, null);
        GroupedExecutionTagger.GroupedExecutionProperties right = node.getRight().accept(this, null);

        if (groupedExecutionEnabled && left.currentNodeCapable && right.currentNodeCapable) {
            return mergeJoinSides(left, right, node.getCriteria());
        }
        if (preferSortMergeJoin(session)) {
            // TODO: This will break the other use case for merge join operating on sorted tables, which requires grouped execution for correctness.
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }

        if (isPrestoOnSpark) {
            GroupedExecutionTagger.GroupedExecutionProperties mergeJoinLeft = node.getLeft().accept(new GroupedExecutionTagger(session, metadata, nodePartitioningManager, true, true), null);
            GroupedExecutionTagger.GroupedExecutionProperties mergeJoinRight = node.getRight().accept(new GroupedExecutionTagger(session, metadata, nodePartitioningManager, true, true), null);
            if (mergeJoinLeft.currentNodeCapable || mergeJoinRight.currentNodeCapable) {
                return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
            }
        }

        throw new PrestoException(
                INVALID_PLAN_ERROR,
                format("When grouped execution can't be enabled, merge join plan is not valid." +
                                "%s is currently set to %s; left node grouped execution capable is %s and " +
                                "right node grouped execution capable is %s.",
                        GROUPED_EXECUTION,
                        groupedExecutionEnabled,
                        left.currentNodeCapable,
                        right.currentNodeCapable));
    }

    /**
     * Merge partition column tracking from both sides of a partitioned join (hash or merge).
     * For each equi-join clause where both sides have usable partition columns,
     * union the corresponding TableScanColumns in the union-find and track which vars participated.
     * Drop vars whose TableScanColumn is not in any equivalence class with a joined var.
     */
    private GroupedExecutionTagger.GroupedExecutionProperties mergeJoinSides(
            GroupedExecutionTagger.GroupedExecutionProperties left,
            GroupedExecutionTagger.GroupedExecutionProperties right,
            List<EquiJoinClause> criteria)
    {
        checkState(left.totalLifespans == right.totalLifespans, format("Mismatched number of lifespans on left(%s) and right(%s) side of join", left.totalLifespans, right.totalLifespans));

        if (!partitionAwareEnabled) {
            return new GroupedExecutionTagger.GroupedExecutionProperties(
                    true,
                    true,
                    ImmutableList.<PlanNodeId>builder()
                            .addAll(left.capableTableScanNodes)
                            .addAll(right.capableTableScanNodes)
                            .build(),
                    left.totalLifespans,
                    left.recoveryEligible && right.recoveryEligible);
        }

        // Merge partition column maps from both sides
        Map<VariableReferenceExpression, TableScanColumn> mergedPartitionColumns = new HashMap<>();
        mergedPartitionColumns.putAll(left.partitionColumns);
        mergedPartitionColumns.putAll(right.partitionColumns);

        // Copy union-find from both sides
        UnionFind<TableScanColumn> mergedUnionFind = left.partitionColumnUnionFind.copy();
        mergedUnionFind.addAll(right.partitionColumnUnionFind);

        // For each equi-join clause where both sides have usable vars,
        // union the corresponding TableScanColumns and track joined vars
        Set<VariableReferenceExpression> joinedVars = new HashSet<>();
        for (EquiJoinClause clause : criteria) {
            if (left.getUsablePartitionColumns().contains(clause.getLeft())
                    && right.getUsablePartitionColumns().contains(clause.getRight())) {
                joinedVars.add(clause.getLeft());
                joinedVars.add(clause.getRight());
                mergedUnionFind.union(
                        left.partitionColumns.get(clause.getLeft()),
                        right.partitionColumns.get(clause.getRight()));
            }
        }

        // Find roots of all joined vars' TableScanColumns
        Set<TableScanColumn> joinedRoots = new HashSet<>();
        for (VariableReferenceExpression var : joinedVars) {
            TableScanColumn tsc = mergedPartitionColumns.get(var);
            if (tsc != null) {
                joinedRoots.add(mergedUnionFind.find(tsc));
            }
        }

        // Drop vars whose TableScanColumn root is not in any joined equivalence class
        mergedPartitionColumns.entrySet().removeIf(entry ->
                !joinedRoots.contains(mergedUnionFind.find(entry.getValue())));

        return new GroupedExecutionTagger.GroupedExecutionProperties(
                true,
                true,
                ImmutableList.<PlanNodeId>builder()
                        .addAll(left.capableTableScanNodes)
                        .addAll(right.capableTableScanNodes)
                        .build(),
                left.totalLifespans,
                left.recoveryEligible && right.recoveryEligible,
                mergedPartitionColumns,
                mergedUnionFind);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitAggregation(AggregationNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = node.getSource().accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            switch (node.getStep()) {
                case SINGLE:
                case FINAL:
                    // Keep only partition columns that are in the GROUP BY keys
                    return filterPartitionColumnsByVars(properties, new HashSet<>(node.getGroupingKeys()));
                case PARTIAL:
                case INTERMEDIATE:
                    return properties;
            }
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitWindow(WindowNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            // Keep only vars that appear in the window's PARTITION BY columns
            return filterPartitionColumnsByVars(properties, new HashSet<>(node.getPartitionBy()));
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitRowNumber(RowNumberNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            // Keep only vars that appear in the row number's PARTITION BY columns
            return filterPartitionColumnsByVars(properties, new HashSet<>(node.getPartitionBy()));
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitTopNRowNumber(TopNRowNumberNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            // Keep only vars that appear in the top-N row number's PARTITION BY columns
            return filterPartitionColumnsByVars(properties, new HashSet<>(node.getPartitionBy()));
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    /**
     * Filter partition columns to only those in the given set of allowed variables.
     * The union-find is passed through unchanged since TableScanColumns don't change.
     */
    private GroupedExecutionTagger.GroupedExecutionProperties filterPartitionColumnsByVars(
            GroupedExecutionTagger.GroupedExecutionProperties properties,
            Set<VariableReferenceExpression> allowedVars)
    {
        ImmutableMap.Builder<VariableReferenceExpression, TableScanColumn> filtered = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, TableScanColumn> entry : properties.partitionColumns.entrySet()) {
            if (allowedVars.contains(entry.getKey())) {
                filtered.put(entry.getKey(), entry.getValue());
            }
        }
        return new GroupedExecutionTagger.GroupedExecutionProperties(
                true, true, properties.capableTableScanNodes, properties.totalLifespans, properties.recoveryEligible,
                filtered.build(), properties.partitionColumnUnionFind.copy());
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitMarkDistinct(MarkDistinctNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
        if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
            return new GroupedExecutionTagger.GroupedExecutionProperties(
                    true, true, properties.capableTableScanNodes, properties.totalLifespans, properties.recoveryEligible,
                    properties.partitionColumns, properties.partitionColumnUnionFind);
        }
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitProject(ProjectNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties childProps = node.getSource().accept(this, null);
        if (!childProps.isCurrentNodeCapable()) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        if (!partitionAwareEnabled) {
            return new GroupedExecutionTagger.GroupedExecutionProperties(
                    childProps.currentNodeCapable, childProps.subTreeUseful,
                    childProps.capableTableScanNodes, childProps.totalLifespans,
                    childProps.recoveryEligible);
        }
        // Track partition columns through variable assignments (identity and renaming projections)
        // Map new output var -> same TableScanColumn as the source var
        Map<VariableReferenceExpression, TableScanColumn> mappedPartitionColumns = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
            if (entry.getValue() instanceof VariableReferenceExpression) {
                VariableReferenceExpression sourceVar = (VariableReferenceExpression) entry.getValue();
                TableScanColumn tsc = childProps.partitionColumns.get(sourceVar);
                if (tsc != null) {
                    mappedPartitionColumns.put(entry.getKey(), tsc);
                }
            }
        }

        // Union-find stays the same (TableScanColumns don't change, only the var->TableScanColumn mapping changes)
        return new GroupedExecutionTagger.GroupedExecutionProperties(
                childProps.currentNodeCapable,
                childProps.subTreeUseful,
                childProps.capableTableScanNodes,
                childProps.totalLifespans,
                childProps.recoveryEligible,
                mappedPartitionColumns,
                childProps.partitionColumnUnionFind);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitCallDistributedProcedure(CallDistributedProcedureNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = node.getSource().accept(this, null);
        boolean recoveryEligible = properties.isRecoveryEligible();
        CallDistributedProcedureTarget target = node.getTarget().orElseThrow(() -> new VerifyException("target is absent"));
        recoveryEligible &= metadata.getConnectorCapabilities(session, target.getConnectorId()).contains(SUPPORTS_PAGE_SINK_COMMIT);

        return new GroupedExecutionTagger.GroupedExecutionProperties(
                properties.isCurrentNodeCapable(),
                properties.isSubTreeUseful(),
                properties.getCapableTableScanNodes(),
                properties.getTotalLifespans(),
                recoveryEligible,
                properties.partitionColumns,
                properties.partitionColumnUnionFind);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitTableWriter(TableWriterNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties properties = node.getSource().accept(this, null);
        boolean recoveryEligible = properties.isRecoveryEligible();
        TableWriterNode.WriterTarget target = node.getTarget().orElseThrow(() -> new VerifyException("target is absent"));
        if (target instanceof TableWriterNode.CreateName || target instanceof TableWriterNode.InsertReference || target instanceof TableWriterNode.RefreshMaterializedViewReference) {
            recoveryEligible &= metadata.getConnectorCapabilities(session, target.getConnectorId()).contains(SUPPORTS_PAGE_SINK_COMMIT);
        }
        else {
            recoveryEligible = false;
        }
        return new GroupedExecutionTagger.GroupedExecutionProperties(
                properties.isCurrentNodeCapable(),
                properties.isSubTreeUseful(),
                properties.getCapableTableScanNodes(),
                properties.getTotalLifespans(),
                recoveryEligible,
                properties.partitionColumns,
                properties.partitionColumnUnionFind);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitTableScan(TableScanNode node, Void context)
    {
        TableLayout layout = metadata.getLayout(session, node.getTable());
        Optional<TableLayout.TablePartitioning> tablePartitioning = layout.getTablePartitioning();
        if (!tablePartitioning.isPresent()) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        List<ConnectorPartitionHandle> partitionHandles = nodePartitioningManager.listPartitionHandles(session, tablePartitioning.get().getPartitioningHandle());
        if (ImmutableList.of(NOT_PARTITIONED).equals(partitionHandles)) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }

        // Collect partition columns from DiscretePredicates only when partition-aware execution is enabled.
        // When disabled, skip entirely to avoid unnecessary tracking overhead.
        Map<VariableReferenceExpression, TableScanColumn> partitionColumns = new HashMap<>();
        UnionFind<TableScanColumn> unionFind = new UnionFind<>();
        if (partitionAwareEnabled) {
            Optional<DiscretePredicates> discretePredicates = layout.getDiscretePredicates();
            if (discretePredicates.isPresent()) {
                Set<ColumnHandle> partitionCols = new HashSet<>(discretePredicates.get().getColumns());
                for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                    if (partitionCols.contains(entry.getValue())) {
                        TableScanColumn tsc = new TableScanColumn(node.getId(), entry.getValue());
                        partitionColumns.put(entry.getKey(), tsc);
                        unionFind.add(tsc);
                    }
                }
            }
        }

        return new GroupedExecutionTagger.GroupedExecutionProperties(
                true,
                false,
                ImmutableList.of(node.getId()),
                partitionHandles.size(),
                metadata.getConnectorCapabilities(session, node.getTable().getConnectorId()).contains(SUPPORTS_REWINDABLE_SPLIT_SOURCE),
                partitionColumns,
                unionFind);
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitIndexSource(IndexSourceNode node, Void context)
    {
        return getSourceNodeGroupedExecutionProperties(node.getId(), node.getTableHandle());
    }

    private GroupedExecutionTagger.GroupedExecutionProperties getSourceNodeGroupedExecutionProperties(PlanNodeId nodeId, TableHandle tableHandle)
    {
        Optional<TableLayout.TablePartitioning> tablePartitioning = metadata.getLayout(session, tableHandle).getTablePartitioning();
        if (!tablePartitioning.isPresent()) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        List<ConnectorPartitionHandle> partitionHandles = nodePartitioningManager.listPartitionHandles(session, tablePartitioning.get().getPartitioningHandle());
        if (ImmutableList.of(NOT_PARTITIONED).equals(partitionHandles)) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }
        else {
            return new GroupedExecutionTagger.GroupedExecutionProperties(
                    true,
                    false,
                    ImmutableList.of(nodeId),
                    partitionHandles.size(),
                    metadata.getConnectorCapabilities(session, tableHandle.getConnectorId()).contains(SUPPORTS_REWINDABLE_SPLIT_SOURCE));
        }
    }

    @Override
    public GroupedExecutionTagger.GroupedExecutionProperties visitIndexJoin(IndexJoinNode node, Void context)
    {
        GroupedExecutionTagger.GroupedExecutionProperties probe = node.getProbeSource().accept(this, null);
        GroupedExecutionTagger.GroupedExecutionProperties index = node.getIndexSource().accept(this, null);

        if (!groupedExecutionEnabled) {
            return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
        }

        // For index join with colocated execution, both probe and index sides must be capable
        // and have the same number of lifespans (buckets)
        if (probe.currentNodeCapable && index.currentNodeCapable) {
            if (probe.totalLifespans != index.totalLifespans) {
                return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
            }
            // Include both probe and index side scan nodes for grouped
            // execution. Each bucket group gets its own index split,
            // ensuring file alignment for colocated lookup joins.
            ImmutableList.Builder<PlanNodeId> allScanNodes = ImmutableList.builder();
            allScanNodes.addAll(probe.capableTableScanNodes);
            allScanNodes.addAll(index.capableTableScanNodes);
            return new GroupedExecutionTagger.GroupedExecutionProperties(
                    true,
                    true,
                    allScanNodes.build(),
                    probe.totalLifespans,
                    probe.recoveryEligible && index.recoveryEligible);
        }

        // Probe-only grouped execution for index joins is not currently
        // supported. The index side requires colocated grouped execution
        // with matching bucket counts for correct split scheduling.
        return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
    }

    private GroupedExecutionTagger.GroupedExecutionProperties processChildren(PlanNode node)
    {
        // Each fragment has a partitioning handle, which is derived from leaf nodes in the fragment.
        // Leaf nodes with different partitioning handle are not allowed to share a single fragment
        // (except for special cases as detailed in addSourceDistribution).
        // As a result, it is not necessary to check the compatibility between node.getSources because
        // they are guaranteed to be compatible.

        // * If any child is "not capable", return "not capable"
        // * When all children are capable ("capable and useful" or "capable but not useful")
        //   * Usefulness:
        //     * if any child is "useful", this node is "useful"
        //     * if no children is "useful", this node is "not useful"
        //   * Recovery Eligibility:
        //     * if all children is "recovery eligible", this node is "recovery eligible"
        //     * if any child is "not recovery eligible", this node is "not recovery eligible"
        boolean anyUseful = false;
        OptionalInt totalLifespans = OptionalInt.empty();
        boolean allRecoveryEligible = true;
        ImmutableList.Builder<PlanNodeId> capableTableScanNodes = ImmutableList.builder();
        // For partition-aware: take intersection of usable partition columns
        Set<VariableReferenceExpression> intersectedPartitionCols = null;
        Map<VariableReferenceExpression, TableScanColumn> mergedPartitionColumns = new HashMap<>();
        UnionFind<TableScanColumn> mergedUnionFind = new UnionFind<>();
        for (PlanNode source : node.getSources()) {
            GroupedExecutionTagger.GroupedExecutionProperties properties = source.accept(this, null);
            if (!properties.isCurrentNodeCapable()) {
                return GroupedExecutionTagger.GroupedExecutionProperties.notCapable();
            }
            anyUseful |= properties.isSubTreeUseful();
            allRecoveryEligible &= properties.isRecoveryEligible();
            if (!totalLifespans.isPresent()) {
                totalLifespans = OptionalInt.of(properties.totalLifespans);
            }
            else {
                checkState(totalLifespans.getAsInt() == properties.totalLifespans, format("Mismatched number of lifespans among children nodes. Expected: %s, actual: %s", totalLifespans.getAsInt(), properties.totalLifespans));
            }

            capableTableScanNodes.addAll(properties.capableTableScanNodes);
            mergedPartitionColumns.putAll(properties.partitionColumns);
            mergedUnionFind.addAll(properties.partitionColumnUnionFind);
            if (intersectedPartitionCols == null) {
                intersectedPartitionCols = new HashSet<>(properties.getUsablePartitionColumns());
            }
            else {
                intersectedPartitionCols.retainAll(properties.getUsablePartitionColumns());
            }
        }
        // Retain only partition columns for intersected variables
        Set<VariableReferenceExpression> retained = intersectedPartitionCols != null ? intersectedPartitionCols : ImmutableSet.of();
        mergedPartitionColumns.keySet().retainAll(retained);
        return new GroupedExecutionTagger.GroupedExecutionProperties(
                true, anyUseful, capableTableScanNodes.build(), totalLifespans.getAsInt(), allRecoveryEligible,
                mergedPartitionColumns,
                mergedUnionFind);
    }

    /**
     * Identifies a partition column by its originating table scan node and column handle.
     */
    public static class TableScanColumn
    {
        private final PlanNodeId scanNodeId;
        private final ColumnHandle columnHandle;

        public TableScanColumn(PlanNodeId scanNodeId, ColumnHandle columnHandle)
        {
            this.scanNodeId = requireNonNull(scanNodeId, "scanNodeId is null");
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        }

        public PlanNodeId getScanNodeId()
        {
            return scanNodeId;
        }

        public ColumnHandle getColumnHandle()
        {
            return columnHandle;
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
            TableScanColumn that = (TableScanColumn) o;
            return Objects.equals(scanNodeId, that.scanNodeId)
                    && Objects.equals(columnHandle, that.columnHandle);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(scanNodeId, columnHandle);
        }

        @Override
        public String toString()
        {
            return scanNodeId + ":" + columnHandle;
        }
    }

    /**
     * Simple union-find (disjoint set) data structure for tracking equivalence classes.
     */
    static class UnionFind<T>
    {
        private final Map<T, T> parent = new HashMap<>();

        public void add(T element)
        {
            parent.putIfAbsent(element, element);
        }

        public T find(T element)
        {
            T root = element;
            while (!parent.get(root).equals(root)) {
                root = parent.get(root);
            }
            // Path compression
            T current = element;
            while (!current.equals(root)) {
                T next = parent.get(current);
                parent.put(current, root);
                current = next;
            }
            return root;
        }

        public void union(T a, T b)
        {
            add(a);
            add(b);
            T rootA = find(a);
            T rootB = find(b);
            if (!rootA.equals(rootB)) {
                parent.put(rootA, rootB);
            }
        }

        /**
         * Copy all entries from another union-find into this one, preserving equivalence relationships.
         */
        public void addAll(UnionFind<T> other)
        {
            // First add all elements
            for (T element : other.parent.keySet()) {
                add(element);
            }
            // Then union elements that share the same root in the other union-find
            Map<T, Set<T>> otherClasses = other.getEquivalenceClasses();
            for (Set<T> group : otherClasses.values()) {
                T first = null;
                for (T element : group) {
                    if (first == null) {
                        first = element;
                    }
                    else {
                        union(first, element);
                    }
                }
            }
        }

        public UnionFind<T> copy()
        {
            UnionFind<T> result = new UnionFind<>();
            result.parent.putAll(this.parent);
            return result;
        }

        public Map<T, Set<T>> getEquivalenceClasses()
        {
            Map<T, Set<T>> classes = new HashMap<>();
            for (T element : parent.keySet()) {
                classes.computeIfAbsent(find(element), k -> new HashSet<>()).add(element);
            }
            return classes;
        }
    }

    static class GroupedExecutionProperties
    {
        // currentNodeCapable:
        //   Whether grouped execution is possible with the current node.
        //   For example, a table scan is capable iff it supports addressable split discovery.
        // subTreeUseful:
        //   Whether grouped execution is beneficial in the current node, or any node below it.
        //   For example, a JOIN can benefit from grouped execution because build can be flushed early, reducing peak memory requirement.
        //
        // In the current implementation, subTreeUseful implies currentNodeCapable.
        // In theory, this doesn't have to be the case. Take an example where a GROUP BY feeds into the build side of a JOIN.
        // Even if JOIN cannot take advantage of grouped execution, it could still be beneficial to execute the GROUP BY with grouped execution
        // (e.g. when the underlying aggregation's intermediate group state may be larger than aggregation output).

        private final boolean currentNodeCapable;
        private final boolean subTreeUseful;
        private final List<PlanNodeId> capableTableScanNodes;
        private final int totalLifespans;
        private final boolean recoveryEligible;
        // Maps each usable partition variable to its origin (scan node + column handle).
        // Used in partition-aware grouped execution to identify partition columns that
        // have matching values across tables and can be scheduled in the same lifespan.
        private final Map<VariableReferenceExpression, TableScanColumn> partitionColumns;
        // Union-find tracking which partition columns from different table scans are
        // equivalent through equi-join conditions and should use the same canonical name.
        private final UnionFind<TableScanColumn> partitionColumnUnionFind;

        public GroupedExecutionProperties(boolean currentNodeCapable, boolean subTreeUseful, List<PlanNodeId> capableTableScanNodes, int totalLifespans, boolean recoveryEligible)
        {
            this(currentNodeCapable, subTreeUseful, capableTableScanNodes, totalLifespans, recoveryEligible,
                    ImmutableMap.of(), new UnionFind<>());
        }

        public GroupedExecutionProperties(
                boolean currentNodeCapable,
                boolean subTreeUseful,
                List<PlanNodeId> capableTableScanNodes,
                int totalLifespans,
                boolean recoveryEligible,
                Map<VariableReferenceExpression, TableScanColumn> partitionColumns,
                UnionFind<TableScanColumn> partitionColumnUnionFind)
        {
            this.currentNodeCapable = currentNodeCapable;
            this.subTreeUseful = subTreeUseful;
            this.capableTableScanNodes = ImmutableList.copyOf(requireNonNull(capableTableScanNodes, "capableTableScanNodes is null"));
            this.totalLifespans = totalLifespans;
            this.recoveryEligible = recoveryEligible;
            this.partitionColumns = ImmutableMap.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
            this.partitionColumnUnionFind = requireNonNull(partitionColumnUnionFind, "partitionColumnUnionFind is null");
            // Verify that `subTreeUseful` implies `currentNodeCapable`
            checkArgument(!subTreeUseful || currentNodeCapable);
            // Verify that `recoveryEligible` implies `currentNodeCapable`
            checkArgument(!recoveryEligible || currentNodeCapable);
            checkArgument(currentNodeCapable == !capableTableScanNodes.isEmpty());
        }

        public static GroupedExecutionProperties notCapable()
        {
            return new GroupedExecutionProperties(false, false, ImmutableList.of(), 1, false);
        }

        public boolean isCurrentNodeCapable()
        {
            return currentNodeCapable;
        }

        public boolean isSubTreeUseful()
        {
            return subTreeUseful;
        }

        public List<PlanNodeId> getCapableTableScanNodes()
        {
            return capableTableScanNodes;
        }

        public int getTotalLifespans()
        {
            return totalLifespans;
        }

        public boolean isRecoveryEligible()
        {
            return recoveryEligible;
        }

        public Set<VariableReferenceExpression> getUsablePartitionColumns()
        {
            return partitionColumns.keySet();
        }

        public Map<VariableReferenceExpression, TableScanColumn> getPartitionColumns()
        {
            return partitionColumns;
        }

        public UnionFind<TableScanColumn> getPartitionColumnUnionFind()
        {
            return partitionColumnUnionFind;
        }
    }
}

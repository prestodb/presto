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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.TopNNode.Step;
import com.facebook.presto.spi.plan.TopNRowNumberNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getOptimizeTopNUsingRowIdMinColumnSavings;
import static com.facebook.presto.SystemSessionProperties.isOptimizeTopNUsingRowIdEnabled;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.PlannerUtils.addColumnToTableScan;
import static com.facebook.presto.sql.planner.PlannerUtils.addPassThroughVariable;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.findTableScanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.isDeterministicScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.isScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.restrictOutput;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Late materialization for TopN over wide tables using $row_id.
 *
 * Rewrites:
 *   TopN(N, orderBy=[a, b])
 *     └─ Source(a, b, c, d, e, ...)
 *
 * Into:
 *   TopN(N, orderBy=[a, b])
 *     └─ InnerJoin($row_id = $row_id_clone)
 *       ├─ Source_with_row_id(a, b, c, d, e, ..., $row_id)
 *       └─ TopN(N, orderBy=[a_clone, b_clone])
 *           └─ ClonedSource(a_clone, b_clone, $row_id_clone)
 *
 * The inner TopN sorts only the narrow clone (sort keys + $row_id). $row_id is the table's unique
 * column, so joining the wide source back to the narrow winners on it is strictly 1:1 — an INNER join
 * keeps exactly the N matching rows (what a SemiJoin would), while being eligible for a colocated /
 * grouped join on the shared $row_id partitioning (avoiding the broadcast a SemiJoin was forced into).
 * The outer TopN re-sorts only N rows (cheap).
 *
 * The same rewrite is applied to {@link TopNRowNumberNode} (the
 * row_number()/rank()/dense_rank() OVER (PARTITION BY ... ORDER BY ...) with
 * WHERE rn &lt;= N pattern, produced by {@link WindowFilterPushDown}). There the
 * narrow key set is partitionBy ∪ orderBy: the inner TopNRowNumber selects, per partition, the same
 * $row_id set and assigns each kept row its ranking value, and the INNER join restricts the wide scan to
 * those rows. Because the ranking is a pure function of the partition/order keys (identical on the narrow
 * clone and the wide source) and the join is 1:1, the inner ranking value is already the final ranking
 * for every kept row. So there is NO outer TopNRowNumber at all: the inner ranking is carried through the
 * join and surfaced by a Project as the ranking variable — eliminating a redundant re-rank and the wide
 * re-shuffle it would force, for every maxRowCountPerPartition.
 */
public class OptimizeTopNUsingRowId
        implements PlanOptimizer
{
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public OptimizeTopNUsingRowId(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isOptimizeTopNUsingRowIdEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            Rewriter rewriter = new Rewriter(session, metadata, idAllocator, variableAllocator, metadata.getFunctionAndTypeManager());
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewritten, rewriter.isPlanChanged());
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final int minColumnSavings;
        private boolean planChanged;

        private Rewriter(Session session, Metadata metadata, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.minColumnSavings = getOptimizeTopNUsingRowIdMinColumnSavings(session);
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            // Guard: only SINGLE step (before distribution)
            if (node.getStep() != Step.SINGLE) {
                return replaceSource(node, source);
            }

            // Guard: source must be a scan-filter-project chain
            if (!isScanFilterProject(source)) {
                return replaceSource(node, source);
            }

            // Guard: source must be deterministic
            if (!isDeterministicScanFilterProject(source, functionAndTypeManager)) {
                return replaceSource(node, source);
            }

            // Find the underlying TableScanNode
            Optional<TableScanNode> tableScanOpt = findTableScanNode(source);
            if (!tableScanOpt.isPresent()) {
                return replaceSource(node, source);
            }
            TableScanNode tableScan = tableScanOpt.get();

            // Check unique column from table layout
            TableLayout layout = metadata.getLayout(session, tableScan.getTable());
            Optional<ColumnHandle> uniqueColumnOpt = layout.getUniqueColumn();
            if (!uniqueColumnOpt.isPresent()) {
                return replaceSource(node, source);
            }
            ColumnHandle uniqueColumnHandle = uniqueColumnOpt.get();

            // Check heuristic: enough non-sort-key columns to justify the rewrite
            Set<VariableReferenceExpression> sortKeySet = new HashSet<>(node.getOrderingScheme().getOrderByVariables());
            int totalColumns = source.getOutputVariables().size();
            int columnSavings = totalColumns - sortKeySet.size();
            if (columnSavings < minColumnSavings) {
                return replaceSource(node, source);
            }

            // === Build the rewritten plan ===

            // 1. Add $row_id to the original source
            VariableReferenceExpression rowIdVar = variableAllocator.newVariable("$row_id",
                    metadata.getColumnMetadata(session, tableScan.getTable(), uniqueColumnHandle).getType());
            TableScanNode augmentedTableScan = addColumnToTableScan(tableScan, uniqueColumnHandle, rowIdVar, idAllocator);
            Optional<PlanNode> augmentedSourceOpt = addPassThroughVariable(source, rowIdVar, idAllocator);
            if (!augmentedSourceOpt.isPresent()) {
                return replaceSource(node, source);
            }
            // Replace the original tableScan with the augmented one at the bottom
            augmentedSourceOpt = replaceTableScan(augmentedSourceOpt.get(), augmentedTableScan, idAllocator);
            if (!augmentedSourceOpt.isPresent()) {
                return replaceSource(node, source);
            }
            PlanNode augmentedSource = augmentedSourceOpt.get();

            // 2. Clone narrow source: sort keys only + $row_id
            List<VariableReferenceExpression> sortKeys = node.getOrderingScheme().getOrderByVariables();
            Map<VariableReferenceExpression, VariableReferenceExpression> varMap = new HashMap<>();
            PlanNode narrowClone = clonePlanNode(source, session, metadata, idAllocator, sortKeys, varMap);

            // Add $row_id to the cloned narrow source too
            Optional<TableScanNode> clonedTableScanOpt = findTableScanNode(narrowClone);
            if (!clonedTableScanOpt.isPresent()) {
                return replaceSource(node, source);
            }
            VariableReferenceExpression clonedRowIdVar = variableAllocator.newVariable("$row_id_clone",
                    metadata.getColumnMetadata(session, tableScan.getTable(), uniqueColumnHandle).getType());
            TableScanNode clonedAugmentedTableScan = addColumnToTableScan(clonedTableScanOpt.get(), uniqueColumnHandle, clonedRowIdVar, idAllocator);
            Optional<PlanNode> narrowCloneOpt = addPassThroughVariable(narrowClone, clonedRowIdVar, idAllocator);
            if (!narrowCloneOpt.isPresent()) {
                return replaceSource(node, source);
            }
            narrowCloneOpt = replaceTableScan(narrowCloneOpt.get(), clonedAugmentedTableScan, idAllocator);
            if (!narrowCloneOpt.isPresent()) {
                return replaceSource(node, source);
            }
            narrowClone = narrowCloneOpt.get();

            // Restrict narrow clone to only sort keys (mapped) + cloned $row_id
            List<VariableReferenceExpression> narrowOutputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(sortKeys.stream().map(v -> varMap.getOrDefault(v, v)).collect(toImmutableList()))
                    .add(clonedRowIdVar)
                    .build();
            narrowClone = restrictOutput(narrowClone, idAllocator, narrowOutputs);

            // 3. Build inner TopN over narrow clone with mapped ordering scheme
            List<Ordering> clonedOrderings = node.getOrderingScheme().getOrderBy().stream()
                    .map(o -> new Ordering(varMap.getOrDefault(o.getVariable(), o.getVariable()), o.getSortOrder()))
                    .collect(toImmutableList());
            OrderingScheme clonedOrderingScheme = new OrderingScheme(clonedOrderings);
            TopNNode innerTopN = new TopNNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    narrowClone,
                    node.getCount(),
                    clonedOrderingScheme,
                    Step.SINGLE);

            // 4. INNER join the wide source back to the narrow winners on the unique $row_id. Because $row_id is
            //    unique (and never null) the join is strictly 1:1, so it keeps exactly the rows a SemiJoin would —
            //    but as a JoinNode it is eligible for a colocated/grouped join on the shared $row_id partitioning
            //    (the SemiJoin path was forced REPLICATED, i.e. broadcast). Force PARTITIONED so both sides
            //    partition on $row_id, enabling a colocated/grouped join when the connector is co-bucketed on
            //    $row_id. Output only the left (wide) columns.
            JoinNode rowIdJoin = new JoinNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    INNER,
                    augmentedSource,
                    innerTopN,
                    ImmutableList.of(new EquiJoinClause(rowIdVar, clonedRowIdVar)),
                    augmentedSource.getOutputVariables(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    ImmutableMap.of());

            // 5. Build outer TopN with original ordering scheme over the joined result to establish sorted order.
            // Don't project away $row_id here — PruneUnreferencedOutputs will handle it.
            // Projecting it away here would break StreamPropertyDerivations' unique column consistency check.
            TopNNode outerTopN = new TopNNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    rowIdJoin,
                    node.getCount(),
                    node.getOrderingScheme(),
                    Step.SINGLE);

            planChanged = true;
            return outerTopN;
        }

        private static TopNNode replaceSource(TopNNode topNNode, PlanNode newSource)
        {
            if (topNNode.getSource() == newSource) {
                return topNNode;
            }
            return new TopNNode(
                    topNNode.getSourceLocation(),
                    topNNode.getId(),
                    topNNode.getStatsEquivalentPlanNode(),
                    newSource,
                    topNNode.getCount(),
                    topNNode.getOrderingScheme(),
                    topNNode.getStep());
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            // Guard: only the final (non-partial) node, analogous to the Step.SINGLE guard for TopN
            if (node.isPartial()) {
                return replaceSource(node, source);
            }

            // Guard: source must be a scan-filter-project chain
            if (!isScanFilterProject(source)) {
                return replaceSource(node, source);
            }

            // Guard: source must be deterministic
            if (!isDeterministicScanFilterProject(source, functionAndTypeManager)) {
                return replaceSource(node, source);
            }

            // Find the underlying TableScanNode
            Optional<TableScanNode> tableScanOpt = findTableScanNode(source);
            if (!tableScanOpt.isPresent()) {
                return replaceSource(node, source);
            }
            TableScanNode tableScan = tableScanOpt.get();

            // Check unique column from table layout
            TableLayout layout = metadata.getLayout(session, tableScan.getTable());
            Optional<ColumnHandle> uniqueColumnOpt = layout.getUniqueColumn();
            if (!uniqueColumnOpt.isPresent()) {
                return replaceSource(node, source);
            }
            ColumnHandle uniqueColumnHandle = uniqueColumnOpt.get();

            // Check heuristic: enough non-key columns to justify the rewrite.
            // Narrow key set = partitionBy ∪ orderBy variables.
            LinkedHashSet<VariableReferenceExpression> narrowKeySet = new LinkedHashSet<>();
            narrowKeySet.addAll(node.getPartitionBy());
            narrowKeySet.addAll(node.getOrderingScheme().getOrderByVariables());
            List<VariableReferenceExpression> narrowKeys = ImmutableList.copyOf(narrowKeySet);
            int totalColumns = source.getOutputVariables().size();
            int columnSavings = totalColumns - narrowKeys.size();
            if (columnSavings < minColumnSavings) {
                return replaceSource(node, source);
            }

            // === Build the rewritten plan ===

            // 1. Add $row_id to the original source
            VariableReferenceExpression rowIdVar = variableAllocator.newVariable("$row_id",
                    metadata.getColumnMetadata(session, tableScan.getTable(), uniqueColumnHandle).getType());
            TableScanNode augmentedTableScan = addColumnToTableScan(tableScan, uniqueColumnHandle, rowIdVar, idAllocator);
            Optional<PlanNode> augmentedSourceOpt = addPassThroughVariable(source, rowIdVar, idAllocator);
            if (!augmentedSourceOpt.isPresent()) {
                return replaceSource(node, source);
            }
            augmentedSourceOpt = replaceTableScan(augmentedSourceOpt.get(), augmentedTableScan, idAllocator);
            if (!augmentedSourceOpt.isPresent()) {
                return replaceSource(node, source);
            }
            PlanNode augmentedSource = augmentedSourceOpt.get();

            // 2. Clone narrow source: partition/order keys only + $row_id
            Map<VariableReferenceExpression, VariableReferenceExpression> varMap = new HashMap<>();
            PlanNode narrowClone = clonePlanNode(source, session, metadata, idAllocator, narrowKeys, varMap);

            // Add $row_id to the cloned narrow source too
            Optional<TableScanNode> clonedTableScanOpt = findTableScanNode(narrowClone);
            if (!clonedTableScanOpt.isPresent()) {
                return replaceSource(node, source);
            }
            VariableReferenceExpression clonedRowIdVar = variableAllocator.newVariable("$row_id_clone",
                    metadata.getColumnMetadata(session, tableScan.getTable(), uniqueColumnHandle).getType());
            TableScanNode clonedAugmentedTableScan = addColumnToTableScan(clonedTableScanOpt.get(), uniqueColumnHandle, clonedRowIdVar, idAllocator);
            Optional<PlanNode> narrowCloneOpt = addPassThroughVariable(narrowClone, clonedRowIdVar, idAllocator);
            if (!narrowCloneOpt.isPresent()) {
                return replaceSource(node, source);
            }
            narrowCloneOpt = replaceTableScan(narrowCloneOpt.get(), clonedAugmentedTableScan, idAllocator);
            if (!narrowCloneOpt.isPresent()) {
                return replaceSource(node, source);
            }
            narrowClone = narrowCloneOpt.get();

            // Restrict narrow clone to only the mapped narrow keys + cloned $row_id
            List<VariableReferenceExpression> narrowOutputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(narrowKeys.stream().map(v -> varMap.getOrDefault(v, v)).collect(toImmutableList()))
                    .add(clonedRowIdVar)
                    .build();
            narrowClone = restrictOutput(narrowClone, idAllocator, narrowOutputs);

            // 3. Build inner TopNRowNumber over the narrow clone with mapped specification
            List<VariableReferenceExpression> clonedPartitionBy = node.getPartitionBy().stream()
                    .map(v -> varMap.getOrDefault(v, v))
                    .collect(toImmutableList());
            List<Ordering> clonedOrderings = node.getOrderingScheme().getOrderBy().stream()
                    .map(o -> new Ordering(varMap.getOrDefault(o.getVariable(), o.getVariable()), o.getSortOrder()))
                    .collect(toImmutableList());
            DataOrganizationSpecification clonedSpecification = new DataOrganizationSpecification(
                    clonedPartitionBy,
                    Optional.of(new OrderingScheme(clonedOrderings)));
            VariableReferenceExpression innerRowNumberVar = variableAllocator.newVariable("inner_row_number", node.getRowNumberVariable().getType());
            TopNRowNumberNode innerTopNRowNumber = new TopNRowNumberNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    narrowClone,
                    clonedSpecification,
                    node.getRankingFunction(),
                    innerRowNumberVar,
                    node.getMaxRowCountPerPartition(),
                    false,
                    Optional.empty());

            // 4. INNER join the wide source back to the narrow per-partition winners on the unique $row_id.
            //    Because $row_id is unique (and never null) the join is strictly 1:1, so it keeps exactly the rows
            //    a SemiJoin would — but as a JoinNode it is eligible for a colocated/grouped join on the shared
            //    $row_id partitioning (the SemiJoin path was forced REPLICATED, i.e. broadcast). Force PARTITIONED
            //    so both sides partition on $row_id, enabling a colocated/grouped join when the connector is
            //    co-bucketed on $row_id. Output the left (wide) columns plus the inner ranking value, which is
            //    reused below instead of being recomputed.
            List<VariableReferenceExpression> joinOutputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(augmentedSource.getOutputVariables())
                    .add(innerRowNumberVar)
                    .build();
            JoinNode rowIdJoin = new JoinNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    INNER,
                    augmentedSource,
                    innerTopNRowNumber,
                    ImmutableList.of(new EquiJoinClause(rowIdVar, clonedRowIdVar)),
                    joinOutputs,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    ImmutableMap.of());

            // 5. The join key is the table's unique column ($row_id), so the join-back is strictly 1:1 and the
            // inner TopNRowNumber has already assigned the final ranking value to every kept row — the ranking is
            // a pure function of the partition/order keys, which are identical on the narrow clone and the wide
            // source — and has already restricted each partition to rank <= maxRowCountPerPartition. So the outer
            // TopNRowNumber that recomputes the ranking and re-limits is entirely redundant, and it forces a wide
            // re-shuffle. Replace it with a projection that surfaces the inner ranking as the node's ranking
            // variable. Keep $row_id so StreamPropertyDerivations' unique-column consistency check still holds;
            // PruneUnreferencedOutputs drops the unused columns later.
            Assignments rankingProjection = Assignments.builder()
                    .putAll(identityAssignments(augmentedSource.getOutputVariables()))
                    .put(node.getRowNumberVariable(), innerRowNumberVar)
                    .build();
            planChanged = true;
            return new ProjectNode(idAllocator.getNextId(), rowIdJoin, rankingProjection);
        }

        private static PlanNode replaceSource(TopNRowNumberNode node, PlanNode newSource)
        {
            if (node.getSource() == newSource) {
                return node;
            }
            return node.replaceChildren(ImmutableList.of(newSource));
        }

        /**
         * Replace the TableScanNode at the bottom of a Filter/Project chain with a new one.
         * Returns Optional.empty() if an unsupported node type is encountered.
         */
        private static Optional<PlanNode> replaceTableScan(PlanNode node, TableScanNode newTableScan, PlanNodeIdAllocator idAllocator)
        {
            if (node instanceof TableScanNode) {
                return Optional.of(newTableScan);
            }
            if (node instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node;
                return replaceTableScan(filterNode.getSource(), newTableScan, idAllocator)
                        .map(newSource -> new FilterNode(filterNode.getSourceLocation(), idAllocator.getNextId(), newSource, filterNode.getPredicate()));
            }
            if (node instanceof ProjectNode) {
                ProjectNode projectNode = (ProjectNode) node;
                return replaceTableScan(projectNode.getSource(), newTableScan, idAllocator)
                        .map(newSource -> new ProjectNode(idAllocator.getNextId(), newSource, projectNode.getAssignments()));
            }
            return Optional.empty();
        }
    }
}

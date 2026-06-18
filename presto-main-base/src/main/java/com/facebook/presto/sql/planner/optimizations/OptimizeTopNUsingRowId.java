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
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.TopNNode.Step;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getOptimizeTopNUsingRowIdMinColumnSavings;
import static com.facebook.presto.SystemSessionProperties.isOptimizeTopNUsingRowIdEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.PlannerUtils.addColumnToTableScan;
import static com.facebook.presto.sql.planner.PlannerUtils.addPassThroughVariable;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.findTableScanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.isDeterministicScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.isScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.restrictOutput;
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
 *     └─ Project(remove $row_id, semi_mark)
 *       └─ Filter(semi_mark = true)
 *         └─ SemiJoin(source=$row_id, filtering=$row_id_clone, output=semi_mark)
 *           ├─ Source_with_row_id(a, b, c, d, e, ..., $row_id)
 *           └─ TopN(N, orderBy=[a_clone, b_clone])
 *               └─ ClonedSource(a_clone, b_clone, $row_id_clone)
 *
 * The inner TopN sorts only the narrow clone (sort keys + $row_id).
 * The SemiJoin filters the full scan to only the N matching rows.
 * The outer TopN re-sorts only N rows (cheap).
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

            // 4. Build SemiJoinNode: source=$row_id from augmented original, filtering=$row_id_clone from inner TopN
            //    For small N (<= 100K), broadcast the TopN result to all workers to avoid a full repartition.
            VariableReferenceExpression semiJoinOutput = variableAllocator.newVariable("semiJoinOutput", BOOLEAN);
            Optional<SemiJoinNode.DistributionType> semiJoinDistribution = node.getCount() <= 100_000
                    ? Optional.of(SemiJoinNode.DistributionType.REPLICATED)
                    : Optional.empty();
            SemiJoinNode semiJoinNode = new SemiJoinNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    augmentedSource,
                    innerTopN,
                    rowIdVar,
                    clonedRowIdVar,
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    semiJoinDistribution,
                    ImmutableMap.of());

            // 5. Filter on semi_mark = true
            PlanNode filtered = new FilterNode(semiJoinNode.getSourceLocation(), idAllocator.getNextId(), semiJoinNode, semiJoinOutput);

            // 6. Build outer TopN with original ordering scheme over the filtered result.
            // Don't project away $row_id and semiJoinOutput here — PruneUnreferencedOutputs will handle it.
            // Projecting them away here would break StreamPropertyDerivations' unique column consistency check.
            TopNNode outerTopN = new TopNNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    filtered,
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

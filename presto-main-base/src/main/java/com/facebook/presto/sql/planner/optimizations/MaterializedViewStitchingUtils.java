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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressions;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public final class MaterializedViewStitchingUtils
{
    private MaterializedViewStitchingUtils() {}

    public static boolean canPerformPartitionLevelRefresh(
            PlanNode viewQueryPlan,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            RowExpressionDeterminismEvaluator determinismEvaluator,
            Lookup lookup)
    {
        boolean containsOuterJoin = searchFrom(viewQueryPlan, lookup)
                .where(node -> node instanceof JoinNode &&
                        (((JoinNode) node).getType() == LEFT ||
                                ((JoinNode) node).getType() == RIGHT ||
                                ((JoinNode) node).getType() == FULL))
                .matches();
        boolean containsNondeterministicExpressions = extractExpressions(viewQueryPlan, lookup).stream()
                .anyMatch(expression -> !determinismEvaluator.isDeterministic(expression));
        return !constraints.isEmpty() && !containsOuterJoin && !containsNondeterministicExpressions;
    }

    /**
     * Build a mapping from (table, column) pairs to their corresponding plan variables
     * by scanning all TableScanNodes in the plan.
     */
    public static Map<TableColumn, VariableReferenceExpression> buildColumnToVariableMapping(
            PlanNode plan,
            Metadata metadata,
            Session session,
            Lookup lookup)
    {
        ImmutableMap.Builder<TableColumn, VariableReferenceExpression> columnToVariable = ImmutableMap.builder();

        List<TableScanNode> tableScans = searchFrom(plan, lookup)
                .whereIsInstanceOfAny(ImmutableList.of(TableScanNode.class))
                .findAll()
                .stream()
                .map(node -> (TableScanNode) node)
                .collect(toImmutableList());

        for (TableScanNode tableScan : tableScans) {
            SchemaTableName scannedTable = metadata.getTableMetadata(session, tableScan.getTable()).getTable();

            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : tableScan.getAssignments().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                ColumnHandle columnHandle = entry.getValue();

                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScan.getTable(), columnHandle);
                columnToVariable.put(new TableColumn(scannedTable, columnMetadata.getName()), variable);
            }
        }

        return columnToVariable.build();
    }

    /**
     * Filters stale predicates from all tables to only include columns that have equivalence mappings.
     * For each table, predicates are filtered to retain only columns that can be mapped through
     * column equivalences. Tables with no mappable predicates will have an empty list in the result.
     */
    public static Map<SchemaTableName, List<TupleDomain<String>>> filterPredicatesToMappedColumns(
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            ColumnEquivalences columnEquivalences)
    {
        return constraints.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> filterPredicatesForTable(
                                entry.getValue().getPredicateDisjuncts(),
                                entry.getKey(),
                                columnEquivalences)));
    }

    private static List<TupleDomain<String>> filterPredicatesForTable(
            List<TupleDomain<String>> stalePredicates,
            SchemaTableName table,
            ColumnEquivalences columnEquivalences)
    {
        if (stalePredicates.isEmpty()) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<TupleDomain<String>> filteredPredicates = ImmutableList.builder();
        for (TupleDomain<String> predicate : stalePredicates) {
            if (!predicate.getDomains().isPresent()) {
                continue;
            }

            // Filter to only columns that have equivalence mappings
            Map<String, Domain> filteredDomains = predicate.getDomains().get().entrySet().stream()
                    .filter(columnDomain -> columnEquivalences.hasEquivalence(new TableColumn(table, columnDomain.getKey())))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!filteredDomains.isEmpty()) {
                filteredPredicates.add(TupleDomain.withColumnDomains(filteredDomains));
            }
        }
        return filteredPredicates.build();
    }

    /**
     * Captures column equivalence information derived from MV column mappings.
     * Each equivalence class contains all columns that are equal due to:
     * <ul>
     *   <li>Direct passthrough from base table to MV output</li>
     *   <li>Join conditions (e.g., A.dt = B.dt both map to mv.dt)</li>
     * </ul>
     */
    public static class ColumnEquivalences
    {
        // Maps each column to its equivalence class (the set of all equivalent columns)
        private final Map<TableColumn, Set<TableColumn>> columnToEquivalenceClass;
        // All known tables (base tables + data table) for validation
        private final Set<SchemaTableName> knownTables;

        public ColumnEquivalences(MaterializedViewDefinition materializedViewDefinition, SchemaTableName dataTable)
        {
            requireNonNull(materializedViewDefinition, "materializedViewDefinition is null");
            requireNonNull(dataTable, "dataTable is null");
            ImmutableMap.Builder<TableColumn, Set<TableColumn>> builder = ImmutableMap.builder();
            ImmutableSet.Builder<SchemaTableName> tablesBuilder = ImmutableSet.builder();
            tablesBuilder.add(dataTable);
            tablesBuilder.addAll(materializedViewDefinition.getBaseTables());

            for (MaterializedViewDefinition.ColumnMapping mapping : materializedViewDefinition.getColumnMappings()) {
                TableColumn viewColumn = mapping.getViewColumn();
                ImmutableSet.Builder<TableColumn> equivalentColumns = ImmutableSet.builder();

                // Include the MV data table column in the equivalence class
                equivalentColumns.add(new TableColumn(dataTable, viewColumn.getColumnName()));

                for (TableColumn baseColumn : mapping.getBaseTableColumns()) {
                    if (baseColumn.isDirectMapped().orElse(true)) {
                        equivalentColumns.add(baseColumn);
                    }
                }

                Set<TableColumn> equivalenceClass = equivalentColumns.build();
                if (equivalenceClass.size() > 1) {
                    for (TableColumn column : equivalenceClass) {
                        builder.put(column, equivalenceClass);
                    }
                }
            }

            this.columnToEquivalenceClass = builder.build();
            this.knownTables = tablesBuilder.build();
        }

        public boolean hasEquivalence(TableColumn column)
        {
            return columnToEquivalenceClass.containsKey(column);
        }

        /**
         * Returns equivalent predicates for other tables based on column equivalences.
         * Given a predicate on the source table, returns a map from each equivalent table
         * to its corresponding predicate with column names mapped through equivalences.
         */
        public Map<SchemaTableName, TupleDomain<String>> getEquivalentPredicates(
                SchemaTableName sourceTable,
                TupleDomain<String> predicate)
        {
            requireNonNull(sourceTable, "sourceTable is null");
            requireNonNull(predicate, "predicate is null");
            checkState(knownTables.contains(sourceTable),
                    "Unknown table: %s. Expected one of: %s", sourceTable, knownTables);

            if (!predicate.getDomains().isPresent()) {
                return ImmutableMap.of();
            }

            Map<SchemaTableName, Map<String, Domain>> domainsByTargetTable = new HashMap<>();

            for (Map.Entry<String, Domain> entry : predicate.getDomains().get().entrySet()) {
                String columnName = entry.getKey();
                Domain domain = entry.getValue();

                TableColumn sourceColumn = new TableColumn(sourceTable, columnName);
                Set<TableColumn> equivalents = columnToEquivalenceClass.get(sourceColumn);
                if (equivalents == null) {
                    continue;
                }

                for (TableColumn equivalent : equivalents) {
                    if (!equivalent.equals(sourceColumn)) {
                        domainsByTargetTable
                                .computeIfAbsent(equivalent.getTableName(), table -> new HashMap<>())
                                .put(equivalent.getColumnName(), domain);
                    }
                }
            }

            return domainsByTargetTable.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            tableToDomains -> TupleDomain.withColumnDomains(ImmutableMap.copyOf(tableToDomains.getValue()))));
        }
    }

    /**
     * Apply stale predicates to filter the plan to only recompute stale data.
     * Pushes predicates down to TableScans:
     * <ul>
     *   <li>Stale table: applies the stale predicate AND NOT(exclusion predicates)</li>
     *   <li>Joined tables with equivalent partition columns: applies the same stale predicate
     *       (propagated through column equivalences) to enable partition pruning</li>
     *   <li>Previously processed stale tables: applies NOT(their stale predicate) to avoid duplicates</li>
     *   <li>Fresh tables in UNION with stale table: applies FALSE to prevent duplicates</li>
     *   <li>Fresh tables outside UNION or in JOIN/INTERSECT/EXCEPT: no filter (need complete data)</li>
     * </ul>
     */
    public static PlanNode applyStalePredicates(
            PlanNode plan,
            SchemaTableName staleTable,
            List<TupleDomain<String>> stalePredicates,
            Map<SchemaTableName, List<TupleDomain<String>>> exclusions,
            ColumnEquivalences columnEquivalences,
            RowExpressionDomainTranslator translator,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            Session session,
            Lookup lookup)
    {
        Map<TableColumn, VariableReferenceExpression> columnToVariable = buildColumnToVariableMapping(plan, metadata, session, lookup);

        List<RowExpression> predicateExpressions = stalePredicates.stream()
                .map(predicate -> predicate.transform(col -> columnToVariable.get(new TableColumn(staleTable, col))))
                .map(translator::toPredicate)
                .collect(toImmutableList());
        RowExpression stalePredicate = or(predicateExpressions);

        // Get equivalent predicates for joined tables
        Map<SchemaTableName, RowExpression> propagatedPredicates = new HashMap<>();
        for (TupleDomain<String> predicate : stalePredicates) {
            Map<SchemaTableName, TupleDomain<String>> equivalentPredicates = columnEquivalences.getEquivalentPredicates(staleTable, predicate);
            for (Map.Entry<SchemaTableName, TupleDomain<String>> entry : equivalentPredicates.entrySet()) {
                SchemaTableName targetTable = entry.getKey();
                TupleDomain<VariableReferenceExpression> domain = entry.getValue()
                        .transform(col -> columnToVariable.get(new TableColumn(targetTable, col)));
                propagatedPredicates.merge(targetTable, translator.toPredicate(domain), LogicalRowExpressions::or);
            }
        }

        // Build exclusion predicates for previously processed stale tables
        Map<SchemaTableName, RowExpression> exclusionPredicates = new HashMap<>();
        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> exclusion : exclusions.entrySet()) {
            SchemaTableName excludedTable = exclusion.getKey();
            List<RowExpression> excludedExpressions = exclusion.getValue().stream()
                    .map(predicate -> predicate.transform(column -> columnToVariable.get(new TableColumn(excludedTable, column))))
                    .map(translator::toPredicate)
                    .collect(toImmutableList());
            exclusionPredicates.put(excludedTable, not(metadata.getFunctionAndTypeManager(), or(excludedExpressions)));
        }

        return new StalePredicateRewriter(
                staleTable,
                stalePredicate,
                propagatedPredicates,
                exclusionPredicates,
                idAllocator,
                metadata,
                session).rewrite(plan);
    }

    /**
     * Rewriter that pushes stale predicates down to TableScan nodes.
     * The context tracks whether we're inside a UNION containing the stale table,
     * or inside a set operation (EXCEPT/INTERSECT) where predicate propagation is disabled.
     *
     * <ul>
     *   <li>Stale table: applies the stale predicate</li>
     *   <li>Joined tables with equivalent partition columns: applies the propagated stale predicate
     *       to enable partition pruning (but NOT inside EXCEPT/INTERSECT)</li>
     *   <li>Previously processed stale tables: applies NOT(their stale predicate) AND propagated
     *       predicate (if applicable) to avoid duplicates</li>
     *   <li>Fresh tables in UNION containing stale table: applies FALSE to prevent duplicates</li>
     *   <li>Fresh tables in other UNIONs or inside JOIN without equivalence: no predicate</li>
     * </ul>
     */
    private static class StalePredicateRewriter
            extends SimplePlanRewriter<StalePredicateRewriter.RewriteState>
    {
        private final SchemaTableName staleTable;
        private final RowExpression stalePredicate;
        private final Map<SchemaTableName, RowExpression> propagatedPredicates;
        private final Map<SchemaTableName, RowExpression> exclusionPredicates;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;

        public StalePredicateRewriter(
                SchemaTableName staleTable,
                RowExpression stalePredicate,
                Map<SchemaTableName, RowExpression> propagatedPredicates,
                Map<SchemaTableName, RowExpression> exclusionPredicates,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                Session session)
        {
            this.staleTable = requireNonNull(staleTable, "staleTable is null");
            this.stalePredicate = requireNonNull(stalePredicate, "stalePredicate is null");
            this.propagatedPredicates = ImmutableMap.copyOf(requireNonNull(propagatedPredicates, "propagatedPredicates is null"));
            this.exclusionPredicates = ImmutableMap.copyOf(requireNonNull(exclusionPredicates, "exclusionPredicates is null"));
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        public PlanNode rewrite(PlanNode node)
        {
            return rewriteWith(this, node, RewriteState.allowPredicatePropagation());
        }

        private boolean containsStaleTable(PlanNode node)
        {
            return searchFrom(node)
                    .where(n -> n instanceof TableScanNode &&
                            metadata.getTableMetadata(session, ((TableScanNode) n).getTable())
                                    .getTable()
                                    .equals(staleTable))
                    .matches();
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<RewriteState> context)
        {
            // Only set isInUnionWithStaleTable to true if this UNION contains the stale table
            // Preserve the inSetOperation flag
            RewriteState newState = context.get().withInUnionWithStaleTable(containsStaleTable(node));
            return context.defaultRewrite(node, newState);
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<RewriteState> context)
        {
            return context.defaultRewrite(node, RewriteState.insideSetOperation());
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<RewriteState> context)
        {
            return context.defaultRewrite(node, RewriteState.insideSetOperation());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<RewriteState> context)
        {
            return context.defaultRewrite(node, RewriteState.allowPredicatePropagation());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<RewriteState> context)
        {
            SchemaTableName scannedTable = metadata.getTableMetadata(session, node.getTable()).getTable();
            RewriteState state = context.get();

            // Stale table: apply the stale partition predicate
            if (scannedTable.equals(staleTable)) {
                return new FilterNode(
                        node.getSourceLocation(),
                        idAllocator.getNextId(),
                        node,
                        stalePredicate);
            }

            // Fresh table in UNION with stale table: FALSE (to prevent duplicates)
            RowExpression basePredicate = state.isInUnionWithStaleTable() ? FALSE_CONSTANT : TRUE_CONSTANT;
            // Table outside UNION, exclusion predicate (previously processed stale table with this predicate)
            RowExpression exclusionPredicate = exclusionPredicates.getOrDefault(scannedTable, TRUE_CONSTANT);
            // Table outside UNION with equivalent partition columns: propagated stale predicate
            // But NOT inside EXCEPT/INTERSECT - those need complete data from non-stale tables
            RowExpression propagatedPredicate = state.isInSetOperation() ? TRUE_CONSTANT : propagatedPredicates.getOrDefault(scannedTable, TRUE_CONSTANT);
            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), node, and(basePredicate, exclusionPredicate, propagatedPredicate));
        }

        /**
         * Tracks the rewrite context as we traverse the plan tree.
         */
        public static class RewriteState
        {
            private final boolean isInUnionWithStaleTable;
            private final boolean inSetOperation;

            private RewriteState(boolean isInUnionWithStaleTable, boolean inSetOperation)
            {
                this.isInUnionWithStaleTable = isInUnionWithStaleTable;
                this.inSetOperation = inSetOperation;
            }

            public static RewriteState allowPredicatePropagation()
            {
                return new RewriteState(false, false);
            }

            public static RewriteState insideSetOperation()
            {
                return new RewriteState(false, true);
            }

            public RewriteState withInUnionWithStaleTable(boolean inUnionWithStaleTable)
            {
                return new RewriteState(inUnionWithStaleTable, this.inSetOperation);
            }

            public boolean isInUnionWithStaleTable()
            {
                return isInUnionWithStaleTable;
            }

            public boolean isInSetOperation()
            {
                return inSetOperation;
            }
        }
    }
}

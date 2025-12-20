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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_STITCHING_FALLBACK;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Builds the delta plan for materialized view differential stitching.
 *
 * <p>Uses the standard IVM delta algebra for monotonic operators (Join, Union, Intersect),
 * falls back to partition replacement for anti-monotonic operators (Except), and handles
 * non-monotonic operators (Aggregation) via partition-level recompute rather than row-level IVM.
 *
 * <p>Terminology (matching IVM formalism):
 * <ul>
 *   <li>R = unchanged rows (from non-stale partitions)</li>
 *   <li>R' = current state (all rows)</li>
 *   <li>∆R = delta (rows from stale partitions)</li>
 * </ul>
 *
 * <p>Identity for partition-aligned staleness: R' = R ∪ ∆R
 *
 * <p>Delta rules by operator:
 * <ul>
 *   <li>Join: ∆(R ⋈ S) = (∆R ⋈ S') ∪ (R ⋈ ∆S)</li>
 *   <li>Union: ∆(R ∪ S) = ∆R ∪ ∆S</li>
 *   <li>Intersect: ∆(R ∩ S) = (∆R ∩ S') ∪ (R ∩ ∆S)</li>
 *   <li>Except: Uses partition replacement (see below)</li>
 *   <li>Selection/Projection/Aggregation: ∆(op(R)) = op(∆R)</li>
 * </ul>
 *
 * <p><b>EXCEPT handling:</b> EXCEPT is anti-monotonic in the right input, requiring
 * ∆⁺/∆⁻ tracking per the formal rule: ∆⁺(R − S) = (∆⁺R − S') ∪ (R ∩ ∆⁻S).
 * Since we don't track deletions separately, we fall back to partition replacement:
 * when S has stale partitions, we identify affected output partitions and recompute
 * from the current base table state.
 */
public class DifferentialPlanRewriter
{
    private final Metadata metadata;
    private final Session session;
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final RowExpressionDomainTranslator translator;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private final Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints;
    private final PassthroughColumnEquivalences columnEquivalences;
    private final Lookup lookup;
    private final WarningCollector warningCollector;

    public DifferentialPlanRewriter(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.translator = new RowExpressionDomainTranslator(metadata);
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
        this.staleConstraints = ImmutableMap.copyOf(requireNonNull(staleConstraints, "staleConstraints is null"));
        this.columnEquivalences = requireNonNull(columnEquivalences, "columnEquivalences is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    /**
     * Builds a stitched query plan combining fresh MV data with recomputed delta.
     * This is the main entry point for query-time stitching.
     *
     * @return Optional containing the stitched plan, or empty if stitching is not possible
     */
    public static Optional<PlanNode> buildStitchedPlan(
            Metadata metadata,
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            MaterializedViewDefinition materializedViewDefinition,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        SchemaTableName dataTable = new SchemaTableName(materializedViewDefinition.getSchema(), materializedViewDefinition.getTable());
        PassthroughColumnEquivalences columnEquivalences = new PassthroughColumnEquivalences(materializedViewDefinition, dataTable);

        Map<SchemaTableName, List<TupleDomain<String>>> filteredConstraints = filterPredicatesToMappedColumns(constraints, columnEquivalences);
        // If any base table is stale yet has no mapped predicates, stitching is not possible
        if (filteredConstraints.values().stream().anyMatch(List::isEmpty)) {
            return Optional.empty();
        }

        PlanNode freshPlan = buildDataTableBranch(metadata, session, node, filteredConstraints, columnEquivalences, dataTable, idAllocator, lookup);
        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                filteredConstraints,
                columnEquivalences,
                lookup,
                warningCollector);

        DeltaPlanResult deltaResult;
        try {
            deltaResult = builder.buildDeltaPlan(node.getViewQueryPlan(), node.getViewQueryMappings());
        }
        catch (UnsupportedOperationException e) {
            warningCollector.add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot use differential stitching for materialized view " + node.getMaterializedViewName() +
                            ": " + e.getMessage() + ". Falling back to full recompute."));
            return Optional.empty();
        }

        return Optional.of(buildUnionNode(
                node,
                freshPlan,
                node.getDataTableMappings(),
                deltaResult.getPlan(),
                deltaResult.getMapping(),
                idAllocator));
    }

    /**
     * Filters stale predicates from all tables to only include columns that have equivalence mappings.
     */
    private static Map<SchemaTableName, List<TupleDomain<String>>> filterPredicatesToMappedColumns(
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            PassthroughColumnEquivalences columnEquivalences)
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
            PassthroughColumnEquivalences columnEquivalences)
    {
        return stalePredicates.stream()
                .filter(predicate -> predicate.getDomains().isPresent())
                .map(predicate -> {
                    Map<String, Domain> filteredDomains = predicate.getDomains().get().entrySet().stream()
                            .filter(entry -> columnEquivalences.hasEquivalence(new TableColumn(table, entry.getKey())))
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                    return TupleDomain.withColumnDomains(filteredDomains);
                })
                .filter(predicate -> !predicate.isAll())
                .collect(toImmutableList());
    }

    private static PlanNode buildDataTableBranch(
            Metadata metadata,
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable,
            PlanNodeIdAllocator idAllocator,
            Lookup lookup)
    {
        PlanNode dataTablePlan = node.getDataTablePlan();

        // Build negated stale predicate for the data table: NOT(stale_partition_1 OR stale_partition_2 OR ...)
        List<TupleDomain<String>> stalePredicates = collectStalePredicatesForDataTable(constraints, columnEquivalences, dataTable);

        if (stalePredicates.isEmpty()) {
            return dataTablePlan;
        }

        Map<TableColumn, VariableReferenceExpression> columnMapping = buildColumnToVariableMapping(metadata, session, dataTablePlan, lookup);
        RowExpressionDomainTranslator translator = new RowExpressionDomainTranslator(metadata);

        List<RowExpression> staleExpressions = stalePredicates.stream()
                .map(predicate -> predicate.transform(col -> columnMapping.get(new TableColumn(dataTable, col))))
                .filter(pred -> !pred.isAll() && !pred.isNone())
                .map(translator::toPredicate)
                .collect(toImmutableList());

        if (staleExpressions.isEmpty()) {
            return dataTablePlan;
        }

        RowExpression stalePredicate = or(staleExpressions);
        RowExpression freshPredicate = not(metadata.getFunctionAndTypeManager(), stalePredicate);

        return new FilterNode(dataTablePlan.getSourceLocation(), idAllocator.getNextId(), dataTablePlan, freshPredicate);
    }

    private static List<TupleDomain<String>> collectStalePredicatesForDataTable(
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable)
    {
        ImmutableList.Builder<TupleDomain<String>> result = ImmutableList.builder();
        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> entry : constraints.entrySet()) {
            SchemaTableName baseTable = entry.getKey();
            for (TupleDomain<String> stalePredicate : entry.getValue()) {
                Map<SchemaTableName, TupleDomain<String>> equivalentPredicates =
                        columnEquivalences.getEquivalentPredicates(baseTable, stalePredicate);
                TupleDomain<String> dataTablePredicate = equivalentPredicates.get(dataTable);
                if (dataTablePredicate != null && !dataTablePredicate.isAll()) {
                    result.add(dataTablePredicate);
                }
            }
        }
        return result.build();
    }

    private static Map<TableColumn, VariableReferenceExpression> buildColumnToVariableMapping(
            Metadata metadata,
            Session session,
            PlanNode plan,
            Lookup lookup)
    {
        ImmutableMap.Builder<TableColumn, VariableReferenceExpression> builder = ImmutableMap.builder();
        searchFrom(plan, lookup)
                .where(TableScanNode.class::isInstance)
                .findAll()
                .stream()
                .map(TableScanNode.class::cast)
                .forEach(tableScan -> {
                    SchemaTableName tableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
                    for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : tableScan.getAssignments().entrySet()) {
                        ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScan.getTable(), entry.getValue());
                        builder.put(new TableColumn(tableName, columnMetadata.getName()), entry.getKey());
                    }
                });
        return builder.build();
    }

    private static PlanNode buildUnionNode(
            MaterializedViewScanNode node,
            PlanNode freshPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> freshMapping,
            PlanNode deltaPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping,
            PlanNodeIdAllocator idAllocator)
    {
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs =
                ImmutableListMultimap.builder();

        for (VariableReferenceExpression outputVar : node.getOutputVariables()) {
            VariableReferenceExpression freshVar = freshMapping.get(outputVar);
            VariableReferenceExpression deltaVar = deltaMapping.get(outputVar);
            if (freshVar != null) {
                outputsToInputs.put(outputVar, freshVar);
            }
            if (deltaVar != null) {
                outputsToInputs.put(outputVar, deltaVar);
            }
        }

        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mapping = outputsToInputs.build();
        return new UnionNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                ImmutableList.of(freshPlan, deltaPlan),
                ImmutableList.copyOf(mapping.keySet()),
                SetOperationNodeUtils.fromListMultimap(mapping));
    }

    /**
     * Builds a delta plan for the given view query plan.
     *
     * @param viewQueryPlan The view query plan to transform
     * @param viewQueryMappings Mapping from MV output variables to view query variables
     * @return DeltaPlanResult containing the delta plan and composed mapping (MV output var → delta var)
     * @throws UnsupportedOperationException if the plan contains unsupported nodes
     */
    public DeltaPlanResult buildDeltaPlan(
            PlanNode viewQueryPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMappings)
    {
        DeltaContext result = viewQueryPlan.accept(new DeltaBuilder(), null);
        Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = result.getDeltaMapping();

        // Compose mappings: MV output var → view query var → delta var
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> composedMapping = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : viewQueryMappings.entrySet()) {
            VariableReferenceExpression deltaVar = deltaMapping.get(entry.getValue());
            if (deltaVar != null) {
                composedMapping.put(entry.getKey(), deltaVar);
            }
        }

        return new DeltaPlanResult(result.getDelta(), composedMapping.build());
    }

    private class DeltaBuilder
            extends InternalPlanVisitor<DeltaContext, Void>
    {
        @Override
        public DeltaContext visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported node type: " + node.getClass().getSimpleName());
        }

        @Override
        public DeltaContext visitTableScan(TableScanNode node, Void context)
        {
            SchemaTableName tableName = metadata.getTableMetadata(session, node.getTable()).getTable();
            List<TupleDomain<String>> stalePredicates = staleConstraints.getOrDefault(tableName, ImmutableList.of());

            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = buildVariableMapping(node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping = buildVariableMapping(node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping = buildVariableMapping(node.getOutputVariables());

            TableScanNode deltaScan = new SymbolMapper(deltaMapping, warningCollector).map(node, idAllocator.getNextId());
            TableScanNode currentScan = new SymbolMapper(currentMapping, warningCollector).map(node, idAllocator.getNextId());
            TableScanNode unchangedScan = new SymbolMapper(unchangedMapping, warningCollector).map(node, idAllocator.getNextId());

            RowExpression stalePredicate = buildStalePredicate(node, stalePredicates, deltaMapping);
            PlanNode delta = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), deltaScan, stalePredicate);

            RowExpression unchangedPredicate = not(metadata.getFunctionAndTypeManager(), buildStalePredicate(node, stalePredicates, unchangedMapping));
            PlanNode unchanged = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), unchangedScan, unchangedPredicate);

            return new DeltaContext(delta, currentScan, unchanged, deltaMapping, currentMapping, unchangedMapping);
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> buildVariableMapping(
                List<VariableReferenceExpression> variables)
        {
            return variables.stream()
                    .collect(toImmutableMap(
                            var -> var,
                            var -> variableAllocator.newVariable(var.getName(), var.getType())));
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> completeMapping(
                Map<VariableReferenceExpression, VariableReferenceExpression> baseMapping,
                List<VariableReferenceExpression> requiredVariables)
        {
            return Stream.concat(
                    baseMapping.entrySet().stream(),
                    requiredVariables.stream()
                            .distinct()
                            .filter(var -> !baseMapping.containsKey(var))
                            .map(var -> Map.entry(var, variableAllocator.newVariable(var.getName(), var.getType()))))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> collectAndCompleteMapping(
                List<DeltaContext> children,
                java.util.function.Function<DeltaContext, Map<VariableReferenceExpression, VariableReferenceExpression>> mappingExtractor,
                List<VariableReferenceExpression> outputVariables)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> baseMapping = children.stream()
                    .flatMap(child -> mappingExtractor.apply(child).entrySet().stream())
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> replacement));
            return completeMapping(baseMapping, outputVariables);
        }

        @Override
        public DeltaContext visitFilter(FilterNode node, Void context)
        {
            checkDeterministic(node.getPredicate(), "filter predicate");
            return visitUnaryNode(node, context, (mapper, child) -> mapper.map(node, child, idAllocator.getNextId()));
        }

        @Override
        public DeltaContext visitProject(ProjectNode node, Void context)
        {
            node.getAssignments().getExpressions().forEach(expr -> checkDeterministic(expr, "projection"));
            return visitUnaryNodeWithNewMappings(node, context, (mapper, child) -> mapper.map(node, child, idAllocator.getNextId()));
        }

        @Override
        public DeltaContext visitAggregation(AggregationNode node, Void context)
        {
            node.getAggregations().values().forEach(agg -> agg.getCall().getArguments()
                    .forEach(expr -> checkDeterministic(expr, "aggregation")));
            return visitUnaryNodeWithNewMappings(node, context, (mapper, child) -> mapper.map(node, child, idAllocator.getNextId()));
        }

        @Override
        public DeltaContext visitSort(SortNode node, Void context)
        {
            // Sort cannot be stitched: UNION of sorted subsets is not globally sorted
            throw new UnsupportedOperationException(
                    "Sort cannot be differentially stitched: UNION of sorted partitions does not preserve global ordering");
        }

        @Override
        public DeltaContext visitLimit(LimitNode node, Void context)
        {
            // Limit cannot be stitched: UNION of limited subsets may have wrong row count
            throw new UnsupportedOperationException(
                    "Limit cannot be differentially stitched: UNION of limited partitions may return incorrect row count");
        }

        @Override
        public DeltaContext visitTopN(TopNNode node, Void context)
        {
            // TopN cannot be stitched: UNION of top-N subsets is not global top-N
            throw new UnsupportedOperationException(
                    "TopN cannot be differentially stitched: UNION of top-N partitions does not preserve global top-N ordering");
        }

        @Override
        public DeltaContext visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        private void checkDeterministic(RowExpression expression, String context)
        {
            if (!determinismEvaluator.isDeterministic(expression)) {
                throw new UnsupportedOperationException("Non-deterministic expression in " + context);
            }
        }

        private <T extends PlanNode> DeltaContext visitUnaryNode(
                T node,
                Void context,
                BiFunction<SymbolMapper, PlanNode, PlanNode> mapperFunction)
        {
            DeltaContext child = node.getSources().get(0).accept(this, context);

            PlanNode delta = mapperFunction.apply(new SymbolMapper(child.getDeltaMapping(), warningCollector), child.getDelta());
            PlanNode current = mapperFunction.apply(new SymbolMapper(child.getCurrentMapping(), warningCollector), child.getCurrent());
            PlanNode unchanged = mapperFunction.apply(new SymbolMapper(child.getUnchangedMapping(), warningCollector), child.getUnchanged());

            return new DeltaContext(delta, current, unchanged, child.getDeltaMapping(), child.getCurrentMapping(), child.getUnchangedMapping());
        }

        private <T extends PlanNode> DeltaContext visitUnaryNodeWithNewMappings(
                T node,
                Void context,
                BiFunction<SymbolMapper, PlanNode, PlanNode> mapperFunction)
        {
            DeltaContext child = node.getSources().get(0).accept(this, context);

            // Complete mappings to include all output variables the node will produce
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping =
                    completeMapping(child.getDeltaMapping(), node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping =
                    completeMapping(child.getCurrentMapping(), node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping =
                    completeMapping(child.getUnchangedMapping(), node.getOutputVariables());

            PlanNode delta = mapperFunction.apply(new SymbolMapper(deltaMapping, warningCollector), child.getDelta());
            PlanNode current = mapperFunction.apply(new SymbolMapper(currentMapping, warningCollector), child.getCurrent());
            PlanNode unchanged = mapperFunction.apply(new SymbolMapper(unchangedMapping, warningCollector), child.getUnchanged());

            return new DeltaContext(delta, current, unchanged, deltaMapping, currentMapping, unchangedMapping);
        }

        @Override
        public DeltaContext visitJoin(JoinNode node, Void context)
        {
            // Only inner joins are supported - outer joins have different IVM rules
            if (node.getType() != INNER) {
                throw new UnsupportedOperationException("Outer joins not supported: " + node.getType());
            }
            node.getFilter().ifPresent(filter -> checkDeterministic(filter, "join filter"));

            // ∆(R ⋈ S) = (∆R ⋈ S') ∪ (R ⋈ ∆S)
            // Current join: R' ⋈ S'
            DeltaContext leftCurrent = node.getLeft().accept(this, context);
            DeltaContext rightCurrent = node.getRight().accept(this, context);
            Map<VariableReferenceExpression, VariableReferenceExpression> baseCurrentMapping = ImmutableMap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                    .putAll(leftCurrent.getCurrentMapping()).putAll(rightCurrent.getCurrentMapping()).buildKeepingLast();
            Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping = completeMapping(baseCurrentMapping, node.getOutputVariables());
            JoinNode currentJoin = new SymbolMapper(currentMapping, warningCollector).map(
                    node, leftCurrent.getCurrent(), rightCurrent.getCurrent(), idAllocator.getNextId());

            // NonStale join: R ⋈ S (for propagation through the plan)
            Map<VariableReferenceExpression, VariableReferenceExpression> baseUnchangedMapping = ImmutableMap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                    .putAll(leftCurrent.getUnchangedMapping()).putAll(rightCurrent.getUnchangedMapping()).buildKeepingLast();
            Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping = completeMapping(baseUnchangedMapping, node.getOutputVariables());
            JoinNode unchangedJoin = new SymbolMapper(unchangedMapping, warningCollector).map(
                    node, leftCurrent.getUnchanged(), rightCurrent.getUnchanged(), idAllocator.getNextId());

            // First delta term: ∆R ⋈ S' (delta from left, current from right)
            DeltaContext leftDeltaContext = node.getLeft().accept(this, context);
            DeltaContext rightCurrentContext = node.getRight().accept(this, context);
            Map<VariableReferenceExpression, VariableReferenceExpression> baseDeltaLeftMapping = ImmutableMap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                    .putAll(leftDeltaContext.getDeltaMapping()).putAll(rightCurrentContext.getCurrentMapping()).buildKeepingLast();
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaLeftMapping = completeMapping(baseDeltaLeftMapping, node.getOutputVariables());
            JoinNode deltaLeft = new SymbolMapper(deltaLeftMapping, warningCollector).map(node, leftDeltaContext.getDelta(), rightCurrentContext.getCurrent(), idAllocator.getNextId());

            // Second delta term: R ⋈ ∆S (unchanged from left, delta from right)
            DeltaContext leftUnchangedContext = node.getLeft().accept(this, context);
            DeltaContext rightDeltaContext = node.getRight().accept(this, context);
            Map<VariableReferenceExpression, VariableReferenceExpression> baseDeltaRightMapping = ImmutableMap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                    .putAll(leftUnchangedContext.getUnchangedMapping()).putAll(rightDeltaContext.getDeltaMapping()).buildKeepingLast();
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaRightMapping = completeMapping(baseDeltaRightMapping, node.getOutputVariables());
            JoinNode deltaRight = new SymbolMapper(deltaRightMapping, warningCollector).map(node, leftUnchangedContext.getUnchanged(), rightDeltaContext.getDelta(), idAllocator.getNextId());

            // Union the delta terms - no DISTINCT needed because the terms are disjoint
            // (first term covers stale R, second term covers non-stale R with stale S)
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = new HashMap<>();
            UnionNode deltaUnion = createBinaryUnion(node, deltaLeft, deltaRight, deltaMapping);

            return new DeltaContext(deltaUnion, currentJoin, unchangedJoin, deltaMapping, currentMapping, unchangedMapping);
        }

        @Override
        public DeltaContext visitUnion(UnionNode node, Void context)
        {
            // ∆(R ∪ S) = ∆R ∪ ∆S
            List<DeltaContext> children = node.getSources().stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());

            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = collectAndCompleteMapping(children, DeltaContext::getDeltaMapping, node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping = collectAndCompleteMapping(children, DeltaContext::getCurrentMapping, node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping = collectAndCompleteMapping(children, DeltaContext::getUnchangedMapping, node.getOutputVariables());

            List<PlanNode> deltaSources = children.stream().map(DeltaContext::getDelta).collect(toImmutableList());
            List<PlanNode> currentSources = children.stream().map(DeltaContext::getCurrent).collect(toImmutableList());
            List<PlanNode> unchangedSources = children.stream().map(DeltaContext::getUnchanged).collect(toImmutableList());

            UnionNode deltaUnion = new SymbolMapper(deltaMapping, warningCollector).map(node, deltaSources, idAllocator.getNextId());
            UnionNode currentUnion = new SymbolMapper(currentMapping, warningCollector).map(node, currentSources, idAllocator.getNextId());
            UnionNode unchangedUnion = new SymbolMapper(unchangedMapping, warningCollector).map(node, unchangedSources, idAllocator.getNextId());

            return new DeltaContext(deltaUnion, currentUnion, unchangedUnion, deltaMapping, currentMapping, unchangedMapping);
        }

        @Override
        public DeltaContext visitIntersect(IntersectNode node, Void context)
        {
            // ∆(R ∩ S) = (∆R ∩ S') ∪ (R ∩ ∆S)
            // Using R (unchanged) instead of R' (current) in the second term makes these disjoint,
            // so no DISTINCT is needed.
            //
            // Optimization: Since INTERSECT only produces rows present in ALL inputs, we can
            // filter non-delta inputs to the stale partitions. This reduces scan size without
            // affecting correctness.
            List<PlanNode> sources = node.getSources();

            // Build current and unchanged operations
            List<DeltaContext> contexts = sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping = collectAndCompleteMapping(contexts, DeltaContext::getCurrentMapping, node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping = collectAndCompleteMapping(contexts, DeltaContext::getUnchangedMapping, node.getOutputVariables());
            List<PlanNode> currentSources = contexts.stream().map(DeltaContext::getCurrent).collect(toImmutableList());
            List<PlanNode> unchangedSources = contexts.stream().map(DeltaContext::getUnchanged).collect(toImmutableList());
            IntersectNode currentOp = new SymbolMapper(currentMapping, warningCollector).map(node, currentSources, idAllocator.getNextId());
            IntersectNode unchangedOp = new SymbolMapper(unchangedMapping, warningCollector).map(node, unchangedSources, idAllocator.getNextId());

            // Delta from left staleness: intersect(∆R, S' filtered to R's stale partitions)
            List<DeltaContext> deltaLeftContexts = sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> deltaLeftMappingBuilder = ImmutableMap.builder();
            deltaLeftMappingBuilder.putAll(deltaLeftContexts.get(0).getDeltaMapping());
            List<PlanNode> deltaLeftSources = new ArrayList<>();
            deltaLeftSources.add(deltaLeftContexts.get(0).getDelta());
            for (int i = 1; i < deltaLeftContexts.size(); i++) {
                deltaLeftMappingBuilder.putAll(deltaLeftContexts.get(i).getCurrentMapping());
                // Optimization: filter S' to R's stale partitions
                PlanNode currentSource = deltaLeftContexts.get(i).getCurrent();
                RowExpression propagatedPredicate = buildPropagatedStalePredicate(currentSource, sources.get(0));
                PlanNode filteredSource = new FilterNode(
                        currentSource.getSourceLocation(),
                        idAllocator.getNextId(),
                        currentSource,
                        propagatedPredicate);
                deltaLeftSources.add(filteredSource);
            }
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaLeftMapping = completeMapping(deltaLeftMappingBuilder.buildKeepingLast(), node.getOutputVariables());
            IntersectNode deltaLeft = new SymbolMapper(deltaLeftMapping, warningCollector).map(node, deltaLeftSources, idAllocator.getNextId());

            // Delta from right staleness: intersect(R filtered to S's stale partitions, ∆S)
            List<DeltaContext> deltaRightContexts = sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> deltaRightMappingBuilder = ImmutableMap.builder();
            deltaRightMappingBuilder.putAll(deltaRightContexts.get(0).getUnchangedMapping());
            List<PlanNode> deltaRightSources = new ArrayList<>();

            // Optimization: filter R to S's stale partitions (OR of all right-side stale predicates)
            PlanNode unchangedFirst = deltaRightContexts.get(0).getUnchanged();
            List<RowExpression> rightStalePredicates = new ArrayList<>();
            for (int i = 1; i < sources.size(); i++) {
                RowExpression predicate = buildPropagatedStalePredicate(unchangedFirst, sources.get(i));
                rightStalePredicates.add(predicate);
            }
            PlanNode filteredFirst = new FilterNode(
                    unchangedFirst.getSourceLocation(),
                    idAllocator.getNextId(),
                    unchangedFirst,
                    or(rightStalePredicates));
            deltaRightSources.add(filteredFirst);

            for (int i = 1; i < deltaRightContexts.size(); i++) {
                deltaRightMappingBuilder.putAll(deltaRightContexts.get(i).getDeltaMapping());
                deltaRightSources.add(deltaRightContexts.get(i).getDelta());
            }
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaRightMapping = completeMapping(deltaRightMappingBuilder.buildKeepingLast(), node.getOutputVariables());
            SymbolMapper deltaRightMapper = new SymbolMapper(deltaRightMapping, warningCollector);
            ImmutableMap<VariableReferenceExpression, List<VariableReferenceExpression>> remappedVariables = node.getVariableMapping().entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> deltaRightMapper.map(entry.getKey()),
                            entry -> deltaRightMapper.map(entry.getValue())));
            IntersectNode deltaRight = new IntersectNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    deltaRightSources,
                    deltaRightMapper.map(node.getOutputVariables()),
                    remappedVariables);

            // Union the delta terms - no DISTINCT needed because the terms are disjoint
            // (deltaLeft covers stale R, deltaRight covers non-stale R with stale S)
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = new HashMap<>();
            UnionNode deltaUnion = createBinaryUnion(node, deltaLeft, deltaRight, deltaMapping);

            return new DeltaContext(deltaUnion, currentOp, unchangedOp, deltaMapping, currentMapping, unchangedMapping);
        }

        @Override
        public DeltaContext visitExcept(ExceptNode node, Void context)
        {
            // EXCEPT is anti-monotonic in the right input. Per "Decision Tree: Deltas with Deletes":
            // staleness is partition-aligned → use partition replacement strategy.
            //
            // deltaLeft: (∆R - S') handles stale left side (delta algebra)
            // deltaRight: ((R restricted to S's stale partitions) - S') handles stale right side (partition replacement)
            List<PlanNode> sources = node.getSources();

            // Build current and unchanged operations
            List<DeltaContext> contexts = sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping = collectAndCompleteMapping(contexts, DeltaContext::getCurrentMapping, node.getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping = collectAndCompleteMapping(contexts, DeltaContext::getUnchangedMapping, node.getOutputVariables());
            List<PlanNode> currentSources = contexts.stream().map(DeltaContext::getCurrent).collect(toImmutableList());
            List<PlanNode> unchangedSources = contexts.stream().map(DeltaContext::getUnchanged).collect(toImmutableList());
            ExceptNode currentOp = new SymbolMapper(currentMapping, warningCollector).map(node, currentSources, idAllocator.getNextId());
            ExceptNode unchangedOp = new SymbolMapper(unchangedMapping, warningCollector).map(node, unchangedSources, idAllocator.getNextId());

            // Delta from left staleness: except(∆R, S')
            List<DeltaContext> deltaLeftContexts = sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> deltaLeftMappingBuilder = ImmutableMap.builder();
            deltaLeftMappingBuilder.putAll(deltaLeftContexts.get(0).getDeltaMapping());
            List<PlanNode> deltaLeftSources = new ArrayList<>();
            deltaLeftSources.add(deltaLeftContexts.get(0).getDelta());
            for (int i = 1; i < deltaLeftContexts.size(); i++) {
                deltaLeftMappingBuilder.putAll(deltaLeftContexts.get(i).getCurrentMapping());
                deltaLeftSources.add(deltaLeftContexts.get(i).getCurrent());
            }
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaLeftMapping = completeMapping(deltaLeftMappingBuilder.buildKeepingLast(), node.getOutputVariables());
            ExceptNode deltaLeft = new SymbolMapper(deltaLeftMapping, warningCollector).map(node, deltaLeftSources, idAllocator.getNextId());

            // Delta from right staleness: (R filtered by S's stale partitions) - S'
            // Visit again - can't reuse deltaLeftContexts because both terms use current[1..n] for S'
            List<DeltaContext> deltaRightContexts = sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            PlanNode leftUnchanged = deltaRightContexts.get(0).getUnchanged();

            // Build stale predicates from right-side TableScans and rewrite to left-side columns
            // using column equivalences from MV metadata
            RowExpression propagatedPredicate = buildPropagatedStalePredicate(leftUnchanged, sources.get(1));

            // Filter R (unchanged) by the propagated stale predicate
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> deltaRightMappingBuilder = ImmutableMap.builder();
            deltaRightMappingBuilder.putAll(deltaRightContexts.get(0).getUnchangedMapping());
            PlanNode filteredLeft = new FilterNode(
                    leftUnchanged.getSourceLocation(),
                    idAllocator.getNextId(),
                    leftUnchanged,
                    propagatedPredicate);

            // Build except(filtered R, S')
            List<PlanNode> deltaRightSources = new ArrayList<>();
            deltaRightSources.add(filteredLeft);
            for (int i = 1; i < deltaRightContexts.size(); i++) {
                deltaRightMappingBuilder.putAll(deltaRightContexts.get(i).getCurrentMapping());
                deltaRightSources.add(deltaRightContexts.get(i).getCurrent());
            }
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaRightMapping = completeMapping(deltaRightMappingBuilder.buildKeepingLast(), node.getOutputVariables());
            ExceptNode deltaRight = new SymbolMapper(deltaRightMapping, warningCollector).map(node, deltaRightSources, idAllocator.getNextId());

            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = new HashMap<>();
            UnionNode deltaUnion = createBinaryUnion(node, deltaLeft, deltaRight, deltaMapping);

            return new DeltaContext(deltaUnion, currentOp, unchangedOp, deltaMapping, currentMapping, unchangedMapping);
        }

        /**
         * Builds a stale predicate for the left side of EXCEPT by finding stale TableScans
         * in the right subtree and rewriting their stale predicates to left-side columns
         * using column equivalences from MV metadata.
         */
        private RowExpression buildPropagatedStalePredicate(PlanNode leftSubtree, PlanNode rightSubtree)
        {
            // Build column-to-variable mapping for the left subtree
            Map<TableColumn, VariableReferenceExpression> leftColumnMapping =
                    buildColumnToVariableMapping(metadata, session, leftSubtree, lookup);

            List<RowExpression> predicates = searchFrom(rightSubtree, lookup)
                    .where(TableScanNode.class::isInstance)
                    .findAll()
                    .stream()
                    .map(TableScanNode.class::cast)
                    .flatMap(tableScan -> rewriteStalePredicatesToLeftColumns(tableScan, leftColumnMapping).stream())
                    .collect(toImmutableList());

            return or(predicates);
        }

        /**
         * For a given TableScan on the right side, converts its stale predicates to RowExpressions
         * that reference left-side variables using column equivalences from MV metadata.
         */
        private List<RowExpression> rewriteStalePredicatesToLeftColumns(
                TableScanNode tableScan,
                Map<TableColumn, VariableReferenceExpression> leftColumnMapping)
        {
            SchemaTableName tableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
            List<TupleDomain<String>> stalePredicates = staleConstraints.getOrDefault(tableName, ImmutableList.of());
            return columnEquivalences.translatePredicatesToVariables(tableName, stalePredicates, leftColumnMapping, translator);
        }

        private UnionNode createBinaryUnion(
                PlanNode original,
                PlanNode left,
                PlanNode right,
                Map<VariableReferenceExpression, VariableReferenceExpression> outputMapping)
        {
            List<VariableReferenceExpression> leftOutputs = left.getOutputVariables();
            List<VariableReferenceExpression> rightOutputs = right.getOutputVariables();

            List<VariableReferenceExpression> unionOutputs = new ArrayList<>();
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping = new HashMap<>();

            for (int i = 0; i < leftOutputs.size(); i++) {
                VariableReferenceExpression leftVar = leftOutputs.get(i);
                VariableReferenceExpression rightVar = rightOutputs.get(i);
                VariableReferenceExpression outputVar = variableAllocator.newVariable(leftVar.getName(), leftVar.getType());
                unionOutputs.add(outputVar);
                variableMapping.put(outputVar, ImmutableList.of(leftVar, rightVar));

                if (i < original.getOutputVariables().size()) {
                    outputMapping.put(original.getOutputVariables().get(i), outputVar);
                }
            }

            return new UnionNode(
                    original.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    ImmutableList.of(left, right),
                    unionOutputs,
                    variableMapping);
        }
    }

    private RowExpression buildStalePredicate(
            TableScanNode node,
            List<TupleDomain<String>> stalePredicates,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
    {
        // Build column-to-variable mapping for the TableScan
        Map<String, VariableReferenceExpression> columnToVariable = node.getAssignments().entrySet().stream()
                .collect(toImmutableMap(
                        entry -> metadata.getColumnMetadata(session, node.getTable(), entry.getValue()).getName(),
                        entry -> variableMapping.get(entry.getKey())));
        List<RowExpression> predicates = stalePredicates.stream()
                .map(disjunct -> disjunct.transform(columnToVariable::get))
                .map(translator::toPredicate)
                .collect(toImmutableList());
        return or(predicates);
    }

    /**
     * Result of building a delta plan: the plan and its variable mapping.
     */
    public static class DeltaPlanResult
    {
        private final PlanNode plan;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping;

        DeltaPlanResult(PlanNode plan, Map<VariableReferenceExpression, VariableReferenceExpression> mapping)
        {
            this.plan = requireNonNull(plan, "plan is null");
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
        }

        public PlanNode getPlan()
        {
            return plan;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getMapping()
        {
            return mapping;
        }
    }

    /**
     * Three plan variants for IVM (matching the algebraic framework):
     * <ul>
     *   <li>delta (∆R): rows from stale partitions — what changed</li>
     *   <li>current (R'): complete current state</li>
     *   <li>unchanged (R): rows from non-stale partitions — R'[non-stale] = R[non-stale]</li>
     * </ul>
     *
     * <p>R' = R ∪ ∆R (for insert-only, partition-aligned staleness)
     */
    private static class DeltaContext
    {
        private final PlanNode delta;
        private final PlanNode current;
        private final PlanNode unchanged;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping;

        DeltaContext(
                PlanNode delta,
                PlanNode current,
                PlanNode unchanged,
                Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping,
                Map<VariableReferenceExpression, VariableReferenceExpression> currentMapping,
                Map<VariableReferenceExpression, VariableReferenceExpression> unchangedMapping)
        {
            this.delta = delta;
            this.current = current;
            this.unchanged = unchanged;
            this.deltaMapping = ImmutableMap.copyOf(deltaMapping);
            this.currentMapping = ImmutableMap.copyOf(currentMapping);
            this.unchangedMapping = ImmutableMap.copyOf(unchangedMapping);
        }

        PlanNode getDelta()
        {
            return delta;
        }

        PlanNode getCurrent()
        {
            return current;
        }

        PlanNode getUnchanged()
        {
            return unchanged;
        }

        Map<VariableReferenceExpression, VariableReferenceExpression> getDeltaMapping()
        {
            return deltaMapping;
        }

        Map<VariableReferenceExpression, VariableReferenceExpression> getCurrentMapping()
        {
            return currentMapping;
        }

        Map<VariableReferenceExpression, VariableReferenceExpression> getUnchangedMapping()
        {
            return unchangedMapping;
        }
    }
}

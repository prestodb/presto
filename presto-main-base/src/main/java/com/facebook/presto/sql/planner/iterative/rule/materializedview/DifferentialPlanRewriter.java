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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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
 * <p>Uses standard IVM delta algebra for monotonic operators (Join, Union, Intersect),
 * and falls back to partition-level recompute for Except and Aggregation.
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

        NodeWithMapping deltaResult;
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
                deltaResult.getNode(),
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
     * @return NodeWithMapping containing the delta plan and composed mapping (MV output var → delta var)
     * @throws UnsupportedOperationException if the plan contains unsupported nodes
     */
    public NodeWithMapping buildDeltaPlan(
            PlanNode viewQueryPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMappings)
    {
        PlanVariants result = viewQueryPlan.accept(new DeltaBuilder(), null);
        Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = result.delta().getMapping();

        // Compose mappings: MV output var → view query var → delta var
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> composedMapping = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : viewQueryMappings.entrySet()) {
            VariableReferenceExpression deltaVar = deltaMapping.get(entry.getValue());
            if (deltaVar != null) {
                composedMapping.put(entry.getKey(), deltaVar);
            }
        }

        return new NodeWithMapping(result.delta().getNode(), composedMapping.build());
    }

    private class DeltaBuilder
            extends InternalPlanVisitor<PlanVariants, Void>
    {
        @Override
        public PlanVariants visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported node type: " + node.getClass().getSimpleName());
        }

        @Override
        public PlanVariants visitTableScan(TableScanNode node, Void context)
        {
            SchemaTableName tableName = metadata.getTableMetadata(session, node.getTable()).getTable();
            List<TupleDomain<String>> stalePredicates = staleConstraints.getOrDefault(tableName, ImmutableList.of());

            // Build three table scan variants with fresh variables
            NodeWithMapping deltaResult = buildTableScan(node, node.getOutputVariables());
            NodeWithMapping currentResult = buildTableScan(node, node.getOutputVariables());
            NodeWithMapping unchangedResult = buildTableScan(node, node.getOutputVariables());

            RowExpression stalePredicate = buildStalePredicate(node, stalePredicates, deltaResult.getMapping());
            RowExpression unchangedPredicate = not(metadata.getFunctionAndTypeManager(),
                    buildStalePredicate(node, stalePredicates, unchangedResult.getMapping()));

            // Apply stale/non-stale filters
            PlanNode deltaNode = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), deltaResult.getNode(), stalePredicate);
            PlanNode unchangedNode = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), unchangedResult.getNode(), unchangedPredicate);

            return new PlanVariants(
                    new NodeWithMapping(deltaNode, deltaResult.getMapping()),
                    currentResult,
                    new NodeWithMapping(unchangedNode, unchangedResult.getMapping()));
        }

        private NodeWithMapping buildTableScan(TableScanNode original, List<VariableReferenceExpression> variables)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping = createFreshMapping(variables);
            return new NodeWithMapping(new SymbolMapper(mapping, warningCollector).map(original, idAllocator.getNextId()), mapping);
        }

        private RowExpression buildStalePredicate(
                TableScanNode node,
                List<TupleDomain<String>> stalePredicates,
                Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
        {
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

        @Override
        public PlanVariants visitFilter(FilterNode node, Void context)
        {
            checkDeterministic(node.getPredicate(), "filter predicate");
            PlanVariants child = node.getSources().get(0).accept(this, context);

            // ∆(σ(R)) = σ(∆R), σ(R') = σ(R'), σ(R) = σ(R)
            return new PlanVariants(
                    buildFilter(node, child.delta()),
                    buildFilter(node, child.current()),
                    buildFilter(node, child.unchanged()));
        }

        private NodeWithMapping buildFilter(FilterNode original, NodeWithMapping source)
        {
            return new NodeWithMapping(
                    new SymbolMapper(source.getMapping(), warningCollector).map(original, source.getNode(), idAllocator.getNextId()),
                    source.getMapping());
        }

        @Override
        public PlanVariants visitProject(ProjectNode node, Void context)
        {
            node.getAssignments().getExpressions().forEach(expr -> checkDeterministic(expr, "projection"));
            PlanVariants child = node.getSources().get(0).accept(this, context);

            // ∆(π(R)) = π(∆R), π(R') = π(R'), π(R) = π(R)
            return new PlanVariants(
                    buildProject(node, child.delta()),
                    buildProject(node, child.current()),
                    buildProject(node, child.unchanged()));
        }

        private NodeWithMapping buildProject(ProjectNode original, NodeWithMapping source)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(source.getMapping(), original.getOutputVariables());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, source.getNode(), idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitAggregation(AggregationNode node, Void context)
        {
            node.getAggregations().values().forEach(agg -> agg.getCall().getArguments()
                    .forEach(expr -> checkDeterministic(expr, "aggregation")));
            PlanVariants child = node.getSources().get(0).accept(this, context);

            // ∆(γ(R)) = γ(∆R), γ(R') = γ(R'), γ(R) = γ(R)
            return new PlanVariants(
                    buildAggregation(node, child.delta()),
                    buildAggregation(node, child.current()),
                    buildAggregation(node, child.unchanged()));
        }

        private NodeWithMapping buildAggregation(AggregationNode original, NodeWithMapping source)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(source.getMapping(), original.getOutputVariables());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, source.getNode(), idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitJoin(JoinNode node, Void context)
        {
            // Only inner joins are supported - outer joins have different IVM rules
            if (node.getType() != INNER) {
                throw new UnsupportedOperationException("Outer joins not supported: " + node.getType());
            }
            node.getFilter().ifPresent(filter -> checkDeterministic(filter, "join filter"));

            // ∆(R ⋈ S) = (∆R ⋈ S') ∪ (R ⋈ ∆S)
            PlanVariants leftVariants = node.getLeft().accept(this, context);
            PlanVariants rightVariants = node.getRight().accept(this, context);

            // Current join: R' ⋈ S'
            NodeWithMapping currentResult = buildJoin(node, leftVariants.current(), rightVariants.current());

            // Unchanged join: R ⋈ S (for propagation through the plan)
            NodeWithMapping unchangedResult = buildJoin(node, cloneNodeWithMapping(leftVariants.unchanged()), rightVariants.unchanged());

            // First delta term: ∆R ⋈ S' (delta from left, current from right)
            NodeWithMapping deltaLeftResult = buildJoin(node, leftVariants.delta(), cloneNodeWithMapping(rightVariants.current()));

            // Second delta term: R ⋈ ∆S (unchanged from left, delta from right)
            // Uses the original left.unchanged() (other use was cloned above)
            NodeWithMapping deltaRightResult = buildJoin(node, leftVariants.unchanged(), rightVariants.delta());

            // Union the delta terms
            NodeWithMapping deltaResult = createBinaryUnion(node, deltaLeftResult.getNode(), deltaRightResult.getNode());

            return new PlanVariants(deltaResult, currentResult, unchangedResult);
        }

        private NodeWithMapping buildJoin(JoinNode original, NodeWithMapping left, NodeWithMapping right)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(left, right), original.getOutputVariables());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, left.getNode(), right.getNode(), idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitUnion(UnionNode node, Void context)
        {
            // ∆(R ∪ S) = ∆R ∪ ∆S
            List<PlanVariants> children = visitAllSources(node.getSources(), context);
            return new PlanVariants(
                    buildUnion(node, children.stream().map(PlanVariants::delta).collect(toImmutableList())),
                    buildUnion(node, children.stream().map(PlanVariants::current).collect(toImmutableList())),
                    buildUnion(node, children.stream().map(PlanVariants::unchanged).collect(toImmutableList())));
        }

        private NodeWithMapping buildUnion(UnionNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitIntersect(IntersectNode node, Void context)
        {
            // ∆(R ∩ S) = (∆R ∩ S'[R's stale]) ∪ (R[S's stale] ∩ ∆S)
            List<PlanNode> sources = node.getSources();
            List<PlanVariants> allVariants = visitAllSources(sources, context);

            // Current: R' ∩ S'
            NodeWithMapping currentResult = buildIntersect(node, allVariants.stream().map(PlanVariants::current).collect(toImmutableList()));

            // Unchanged: R ∩ S
            NodeWithMapping unchangedResult = buildIntersect(node, allVariants.stream().map(PlanVariants::unchanged).collect(toImmutableList()));

            // Delta: union of left and right delta terms
            IntersectNode deltaLeft = buildIntersectDeltaLeft(node, sources, allVariants);
            IntersectNode deltaRight = buildIntersectDeltaRight(node, sources, allVariants);
            NodeWithMapping deltaUnionResult = createBinaryUnion(node, deltaLeft, deltaRight);

            return new PlanVariants(deltaUnionResult, currentResult, unchangedResult);
        }

        private IntersectNode buildIntersectDeltaLeft(
                IntersectNode original,
                List<PlanNode> originalSources,
                List<PlanVariants> allVariants)
        {
            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(allVariants.get(0).delta());

            // Filter remaining sources' current to first source's stale partitions
            for (int i = 1; i < allVariants.size(); i++) {
                NodeWithMapping clonedCurrent = cloneNodeWithMapping(allVariants.get(i).current());
                RowExpression stalePredicate = buildPropagatedStalePredicate(clonedCurrent.getNode(), originalSources.get(0));
                sources.add(new NodeWithMapping(
                        buildFilter(clonedCurrent.getNode(), stalePredicate),
                        clonedCurrent.getMapping()));
            }
            return buildIntersectFromSources(original, sources);
        }

        private IntersectNode buildIntersectDeltaRight(
                IntersectNode original,
                List<PlanNode> originalSources,
                List<PlanVariants> allVariants)
        {
            // Clone and filter R's unchanged to S's stale partitions
            NodeWithMapping firstUnchanged = cloneNodeWithMapping(allVariants.get(0).unchanged());
            ImmutableList.Builder<RowExpression> stalePredicates = ImmutableList.builder();
            for (int i = 1; i < allVariants.size(); i++) {
                stalePredicates.add(buildPropagatedStalePredicate(firstUnchanged.getNode(), originalSources.get(i)));
            }
            NodeWithMapping filteredFirst = new NodeWithMapping(
                    buildFilter(firstUnchanged.getNode(), or(stalePredicates.build())),
                    firstUnchanged.getMapping());

            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(filteredFirst);
            for (int i = 1; i < allVariants.size(); i++) {
                sources.add(allVariants.get(i).delta());
            }
            return buildIntersectFromSources(original, sources);
        }

        private IntersectNode buildIntersectFromSources(IntersectNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId());
        }

        private List<PlanVariants> visitAllSources(List<PlanNode> sources, Void context)
        {
            return sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
        }

        private PlanNode buildFilter(PlanNode source, RowExpression predicate)
        {
            return new FilterNode(source.getSourceLocation(), idAllocator.getNextId(), source, predicate);
        }

        private NodeWithMapping buildIntersect(IntersectNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitExcept(ExceptNode node, Void context)
        {
            // EXCEPT is anti-monotonic in the right input.
            // deltaLeft: (∆A - B') handles stale left side via delta algebra
            // deltaRight: (A[B's stale] - B') handles stale right side via partition replacement
            List<PlanNode> sources = node.getSources();
            List<PlanVariants> allVariants = visitAllSources(sources, context);

            // Current: A' - B'
            NodeWithMapping currentResult = buildExcept(node, allVariants.stream().map(PlanVariants::current).collect(toImmutableList()));

            // Unchanged: A - B
            NodeWithMapping unchangedResult = buildExcept(node, allVariants.stream().map(PlanVariants::unchanged).collect(toImmutableList()));

            // Delta: union of left and right delta terms
            ExceptNode deltaLeft = buildExceptDeltaLeft(node, allVariants);
            ExceptNode deltaRight = buildExceptDeltaRight(node, sources, allVariants);
            NodeWithMapping deltaUnionResult = createBinaryUnion(node, deltaLeft, deltaRight);

            return new PlanVariants(deltaUnionResult, currentResult, unchangedResult);
        }

        private ExceptNode buildExceptDeltaLeft(ExceptNode original, List<PlanVariants> allVariants)
        {
            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(allVariants.get(0).delta());
            for (int i = 1; i < allVariants.size(); i++) {
                sources.add(cloneNodeWithMapping(allVariants.get(i).current()));
            }
            return buildExceptFromSources(original, sources);
        }

        private ExceptNode buildExceptDeltaRight(
                ExceptNode original,
                List<PlanNode> originalSources,
                List<PlanVariants> allVariants)
        {
            // Filter A's unchanged rows to only those matching B's stale partitions
            NodeWithMapping firstUnchanged = cloneNodeWithMapping(allVariants.get(0).unchanged());
            RowExpression stalePredicate = buildPropagatedStalePredicate(firstUnchanged.getNode(), originalSources.get(1));
            NodeWithMapping filteredFirst = new NodeWithMapping(
                    buildFilter(firstUnchanged.getNode(), stalePredicate),
                    firstUnchanged.getMapping());

            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(filteredFirst);
            for (int i = 1; i < allVariants.size(); i++) {
                sources.add(cloneNodeWithMapping(allVariants.get(i).current()));
            }
            return buildExceptFromSources(original, sources);
        }

        private ExceptNode buildExceptFromSources(ExceptNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId());
        }

        private NodeWithMapping buildExcept(ExceptNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitSort(SortNode node, Void context)
        {
            // Sort cannot be stitched: UNION of sorted subsets is not globally sorted
            throw new UnsupportedOperationException(
                    "Sort cannot be differentially stitched: UNION of sorted partitions does not preserve global ordering");
        }

        @Override
        public PlanVariants visitLimit(LimitNode node, Void context)
        {
            // Limit cannot be stitched: UNION of limited subsets may have wrong row count
            throw new UnsupportedOperationException(
                    "Limit cannot be differentially stitched: UNION of limited partitions may return incorrect row count");
        }

        @Override
        public PlanVariants visitTopN(TopNNode node, Void context)
        {
            // TopN cannot be stitched: UNION of top-N subsets is not global top-N
            throw new UnsupportedOperationException(
                    "TopN cannot be differentially stitched: UNION of top-N partitions does not preserve global top-N ordering");
        }

        @Override
        public PlanVariants visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        private void checkDeterministic(RowExpression expression, String context)
        {
            if (!determinismEvaluator.isDeterministic(expression)) {
                throw new UnsupportedOperationException("Non-deterministic expression in " + context);
            }
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

        private Map<VariableReferenceExpression, VariableReferenceExpression> createFreshMapping(
                List<VariableReferenceExpression> variables)
        {
            return variables.stream()
                    .collect(toImmutableMap(
                            Function.identity(),
                            expression -> variableAllocator.newVariable(expression.getName(), expression.getType())));
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> extendMapping(
                Map<VariableReferenceExpression, VariableReferenceExpression> existing,
                List<VariableReferenceExpression> outputVariables)
        {
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> builder = ImmutableMap.builder();
            builder.putAll(existing);
            outputVariables.stream()
                    .filter(variable -> !existing.containsKey(variable))
                    .forEach(variable -> builder.put(variable, variableAllocator.newVariable(variable.getName(), variable.getType())));
            return builder.buildKeepingLast();
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> combineMapping(NodeWithMapping... sources)
        {
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> builder = ImmutableMap.builder();
            Arrays.stream(sources).map(NodeWithMapping::getMapping).forEach(builder::putAll);
            return builder.buildKeepingLast();
        }

        private NodeWithMapping createBinaryUnion(PlanNode original, PlanNode left, PlanNode right)
        {
            List<VariableReferenceExpression> leftOutputs = left.getOutputVariables();
            List<VariableReferenceExpression> rightOutputs = right.getOutputVariables();

            List<VariableReferenceExpression> unionOutputs = new ArrayList<>();
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping = new HashMap<>();
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputMapping = ImmutableMap.builder();

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

            UnionNode unionNode = new UnionNode(
                    original.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    ImmutableList.of(left, right),
                    unionOutputs,
                    variableMapping);

            return new NodeWithMapping(unionNode, outputMapping.build());
        }

        private NodeWithMapping cloneNodeWithMapping(NodeWithMapping original)
        {
            SubtreeRemappingVisitor visitor = new SubtreeRemappingVisitor();
            PlanNode clonedPlan = original.getNode().accept(visitor, null);
            Map<VariableReferenceExpression, VariableReferenceExpression> variableRenaming = visitor.getMapping();

            // Compose mappings: for each (origVar, currVar) in original mapping,
            // the new mapping is (origVar, renaming[currVar])
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> newMapping = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : original.getMapping().entrySet()) {
                VariableReferenceExpression renamedVariable = variableRenaming.get(entry.getValue());
                if (renamedVariable != null) {
                    newMapping.put(entry.getKey(), renamedVariable);
                }
            }

            return new NodeWithMapping(clonedPlan, newMapping.build());
        }

        private class SubtreeRemappingVisitor
                extends InternalPlanVisitor<PlanNode, Void>
        {
            private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping = new HashMap<>();

            public Map<VariableReferenceExpression, VariableReferenceExpression> getMapping()
            {
                return ImmutableMap.copyOf(mapping);
            }

            private void ensureVariablesMapped(List<VariableReferenceExpression> variables)
            {
                for (VariableReferenceExpression variable : variables) {
                    mapping.computeIfAbsent(variable, v -> variableAllocator.newVariable(v.getName(), v.getType()));
                }
            }

            private SymbolMapper getMapper()
            {
                return new SymbolMapper(mapping, warningCollector);
            }

            @Override
            public PlanNode visitPlan(PlanNode node, Void context)
            {
                throw new UnsupportedOperationException(
                        "Cannot clone node type: " + node.getClass().getSimpleName() +
                        ". Add visitXxx method to SubtreeRemappingVisitor to support this node type.");
            }

            @Override
            public PlanNode visitTableScan(TableScanNode node, Void context)
            {
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitFilter(FilterNode node, Void context)
            {
                PlanNode newSource = node.getSource().accept(this, context);
                // Filter passes through source variables, no new allocations needed
                return getMapper().map(node, newSource, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitProject(ProjectNode node, Void context)
            {
                PlanNode newSource = node.getSource().accept(this, context);
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSource, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitAggregation(AggregationNode node, Void context)
            {
                PlanNode newSource = node.getSource().accept(this, context);
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSource, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitJoin(JoinNode node, Void context)
            {
                PlanNode newLeft = node.getLeft().accept(this, context);
                PlanNode newRight = node.getRight().accept(this, context);
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newLeft, newRight, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitUnion(UnionNode node, Void context)
            {
                List<PlanNode> newSources = node.getSources().stream()
                        .map(source -> source.accept(this, context))
                        .collect(toImmutableList());
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSources, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitIntersect(IntersectNode node, Void context)
            {
                List<PlanNode> newSources = node.getSources().stream()
                        .map(source -> source.accept(this, context))
                        .collect(toImmutableList());
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSources, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitExcept(ExceptNode node, Void context)
            {
                List<PlanNode> newSources = node.getSources().stream()
                        .map(source -> source.accept(this, context))
                        .collect(toImmutableList());
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSources, idAllocator.getNextId());
            }
        }
    }

    /**
     * A plan node paired with its variable mapping (original variable → new variable).
     * Used for results of node building operations.
     */
    public static class NodeWithMapping
    {
        private final PlanNode node;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping;

        NodeWithMapping(PlanNode node, Map<VariableReferenceExpression, VariableReferenceExpression> mapping)
        {
            this.node = requireNonNull(node, "node is null");
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
        }

        public PlanNode getNode()
        {
            return node;
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
    private static class PlanVariants
    {
        private final NodeWithMapping delta;
        private final NodeWithMapping current;
        private final NodeWithMapping unchanged;

        PlanVariants(NodeWithMapping delta, NodeWithMapping current, NodeWithMapping unchanged)
        {
            this.delta = requireNonNull(delta, "delta is null");
            this.current = requireNonNull(current, "current is null");
            this.unchanged = requireNonNull(unchanged, "unchanged is null");
        }

        NodeWithMapping delta()
        {
            return delta;
        }

        NodeWithMapping current()
        {
            return current;
        }

        NodeWithMapping unchanged()
        {
            return unchanged;
        }
    }
}

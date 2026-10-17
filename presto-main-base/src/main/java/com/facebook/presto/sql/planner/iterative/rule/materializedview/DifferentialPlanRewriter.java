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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ChangedRowsPredicate;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.RefreshMaterializedViewNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_STITCHING_FALLBACK;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

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
 * <p><b>How this class signals failure.</b> Two mechanisms, deliberately distinct:
 *
 * <ul>
 *   <li>{@link UnsupportedOperationException} means "this view cannot be stitched" and is the
 *       fallback signal. {@code buildStitchedPlan} catches it around both branch builds, reports a
 *       warning and returns empty, so the query degrades to a full recompute. Throwing is how a
 *       visitor deep in the plan walk reaches that boundary without threading an Optional through
 *       every operator.
 *   <li>{@code checkState} marks an internal invariant the eligibility gate is supposed to make
 *       unreachable -- a missing variable mapping, a set operation without two sources. These sit
 *       outside the fallback boundary, so one firing fails the query rather than degrading. That is
 *       intentional: it is a planner bug, not an unsupported view, and silently falling back would
 *       hide it.
 * </ul>
 *
 * <p>The distinction matters when adding a shape: anything a user can legitimately write must reach
 * the first mechanism, never the second.
 *
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
    private final Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates;
    private final PassthroughColumnEquivalences columnEquivalences;
    private final Lookup lookup;
    private final WarningCollector warningCollector;
    /**
     * The aggregation whose delta is to be merged with the stored values rather than expanded back
     * to whole groups, or empty when there is none. Read by {@code requiresExpandedDelta} so that
     * the delta branch aggregates the changed rows alone.
     */
    private final Optional<MergeableAggregation> deltaMergeRoot;

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
        this(metadata, session, idAllocator, variableAllocator, staleConstraints, ImmutableMap.of(), columnEquivalences, lookup, warningCollector);
    }

    /**
     * @param changedRowsPredicates per-base row-level changed-rows predicates. A base listed here
     *        gets a row-level delta leaf; a base absent from it keeps the partition-level leaf, which
     *        is how one candidate mixes granularities across V3 and V2 bases.
     */
    public DifferentialPlanRewriter(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        this(metadata, session, idAllocator, variableAllocator, staleConstraints, changedRowsPredicates, columnEquivalences, lookup, warningCollector, Optional.empty());
    }

    /**
     * @param deltaMergeRoot the aggregation to merge rather than expand, decided by the caller before
     *        either branch is built because it changes how both are constructed.
     */
    private DifferentialPlanRewriter(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector,
            Optional<MergeableAggregation> deltaMergeRoot)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.translator = new RowExpressionDomainTranslator(metadata);
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
        this.staleConstraints = ImmutableMap.copyOf(requireNonNull(staleConstraints, "staleConstraints is null"));
        this.changedRowsPredicates = ImmutableMap.copyOf(requireNonNull(changedRowsPredicates, "changedRowsPredicates is null"));
        this.columnEquivalences = requireNonNull(columnEquivalences, "columnEquivalences is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.deltaMergeRoot = requireNonNull(deltaMergeRoot, "deltaMergeRoot is null");
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
        return buildStitchedPlan(metadata, session, node, constraints, ImmutableMap.of(), materializedViewDefinition, variableAllocator, idAllocator, lookup, warningCollector);
    }

    /**
     * Builds a stitched plan whose leaves are row-level for every base the connector reports
     * changed rows for, and partition-level for the rest.
     *
     * <p>Row-level identifiers are the view query's grouping keys, because Case B expansion
     * recomputes whole affected groups and the fresh branch has to exclude exactly the storage rows
     * for those groups. Keying the anti-join at any other grain would leave the two branches
     * overlapping or gapped.
     *
     * @param changedRowsPredicates per-base row-level changed-rows predicates; empty yields the
     *        partition-level candidate
     */
    public static Optional<PlanNode> buildStitchedPlan(
            Metadata metadata,
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            MaterializedViewDefinition materializedViewDefinition,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        SchemaTableName dataTable = new SchemaTableName(materializedViewDefinition.getSchema(), materializedViewDefinition.getTable());
        PassthroughColumnEquivalences columnEquivalences = new PassthroughColumnEquivalences(materializedViewDefinition, dataTable);

        Map<SchemaTableName, List<TupleDomain<String>>> filteredConstraints = filterPredicatesToMappedColumns(constraints, columnEquivalences);
        // Needing a mapped partition predicate is a partition-level precondition. A row-level base
        // is identified by its changed-rows predicate instead, and commonly has no usable partition
        // predicate at all -- an unpartitioned table being the obvious case -- so applying this to
        // a row-level base would reject exactly the candidate that can still be built.
        boolean staleBaseWithoutIdentifier = filteredConstraints.entrySet().stream()
                .anyMatch(entry -> entry.getValue().isEmpty()
                        && !isRowLevel(changedRowsPredicates.get(entry.getKey())));
        if (staleBaseWithoutIdentifier) {
            warningCollector.add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot use differential stitching for materialized view " + node.getMaterializedViewName() +
                            ": a stale base table has no predicates mapped to data-table columns. Falling back to full recompute."));
            return Optional.empty();
        }

        // A view whose aggregates can be merged from their stored values takes a different shape:
        // the delta is aggregated as it stands and added to what storage already holds, instead of
        // being expanded back to every row of each affected group. Decided before the delta is built,
        // because it changes how both branches are constructed.
        //
        // Merging is only sound when the delta is a pure increment on top of what storage already
        // holds, which takes three things of the change set rather than of the view:
        //
        //   - every changed-rows predicate is row-level, so that the delta is the changed rows
        //     themselves rather than a recomputation of whole stale partitions;
        //   - no stale base is missing one, because such a base keeps a partition-level leaf whose
        //     delta replaces a group's value instead of adding to it;
        //   - the range is additions only. A modified row is still present and still matches the
        //     disjuncts, indistinguishable from an appended one, so merging would add the new
        //     value on top of the old one that storage is still carrying.
        //
        // Fail any of them and the expansion path remains correct, which is why this falls back to
        // it rather than declining to stitch.
        // Row-level serves exactly two shapes over an append-only range, and declines everything
        // else here rather than building a plan that falls back later. The two are the only ones
        // where the materialized rows can be reused instead of recomputed:
        //
        //   row-preserving    the delta is the appended rows; storage keeps every row it has
        //   mergeable aggregate   the delta is an increment; storage supplies the value to add to
        //
        // Anything else either needs the affected groups' other base rows -- which costs a scan of
        // the whole base, measurably more than recomputing the view outright -- or cannot place its
        // rows at all. Both are worse than the partition-level candidate standing beside this one.
        Optional<MergeableAggregation> mergeRoot = Optional.empty();
        boolean rowPreservingAdditions = false;
        if (!changedRowsPredicates.isEmpty()) {
            if (!isPureIncrement(changedRowsPredicates, filteredConstraints)) {
                warningCollector.add(new PrestoWarning(
                        MATERIALIZED_VIEW_STITCHING_FALLBACK,
                        "Cannot use row-level stitching for materialized view " + node.getMaterializedViewName() +
                                ": the change set is not additions only for every stale base table."));
                return Optional.empty();
            }
            ViewShape shape = classifyViewShape(node.getViewQueryPlan(), lookup);
            if (shape == ViewShape.AGGREGATING) {
                mergeRoot = mergeableAggregationRoot(node.getViewQueryPlan(), metadata, lookup);
            }
            if (shape != ViewShape.ROW_PRESERVING && !mergeRoot.isPresent()) {
                warningCollector.add(new PrestoWarning(
                        MATERIALIZED_VIEW_STITCHING_FALLBACK,
                        "Cannot use row-level stitching for materialized view " + node.getMaterializedViewName() +
                                ": the view is neither row-preserving nor an aggregation whose every aggregate can be " +
                                "merged from its stored value."));
                return Optional.empty();
            }
            rowPreservingAdditions = shape == ViewShape.ROW_PRESERVING;
        }

        // Constructed after the gate so the merge decision can be final: the delta branch and the
        // fresh branch both depend on it, and they have to agree.
        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                filteredConstraints,
                changedRowsPredicates,
                columnEquivalences,
                lookup,
                warningCollector,
                mergeRoot);

        PlanNode freshPlan;
        NodeWithMapping deltaResult;
        try {
            // The fresh branch shares the column-binding logic with the delta branch, so it has to
            // sit inside the fallback boundary too: an unstitchable plan must degrade to a full
            // recompute rather than fail the query.
            freshPlan = buildDataTableBranch(metadata, session, node, filteredConstraints, changedRowsPredicates, columnEquivalences, dataTable,
                    rowPreservingAdditions, mergeRoot.isPresent(), idAllocator, variableAllocator, lookup);
            deltaResult = builder.buildDeltaPlan(node.getViewQueryPlan(), node.getViewQueryMappings());
        }
        catch (UnsupportedOperationException e) {
            warningCollector.add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot use differential stitching for materialized view " + node.getMaterializedViewName() +
                            ": " + e.getMessage() + ". Falling back to full recompute."));
            return Optional.empty();
        }

        PlanNode stitched = buildUnionNode(
                node.getOutputVariables(),
                freshPlan,
                node.getDataTableMappings(),
                deltaResult.getNode(),
                deltaResult.getMapping(),
                idAllocator);

        if (mergeRoot.isPresent()) {
            // The delta carries an increment, not a replacement. If the merge cannot be built the
            // union alone would double-count every affected group, so this falls back to a full
            // recompute rather than returning it.
            Optional<PlanNode> merged = buildMergeAggregation(node.getOutputVariables(), node.getViewQueryMappings(), mergeRoot.get(), stitched, metadata, idAllocator, variableAllocator);
            if (!merged.isPresent()) {
                warningCollector.add(new PrestoWarning(
                        MATERIALIZED_VIEW_STITCHING_FALLBACK,
                        "Cannot merge the delta into the stored aggregates for materialized view " + node.getMaterializedViewName() +
                                ": an output is neither a grouping key nor a mergeable aggregate. Falling back to full recompute."));
                return Optional.empty();
            }
            return merged;
        }
        return Optional.of(stitched);
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

    /**
     * Builds the fresh branch: the MV storage rows that do not need recomputing.
     *
     * <p>Two mechanisms, chosen per base table, because they are not interchangeable:
     *
     * <ul>
     *   <li>A partition-level stale predicate maps to the storage table's own columns, so the
     *       branch excludes affected rows with {@code Filter(NOT stale_predicate)}. The connector
     *       can prune partitions from that predicate, and it stays correct when a partition's rows
     *       are all deleted -- the predicate still names the partition even though no base row
     *       survives to identify it.</li>
     *   <li>A row-level changed-rows predicate is expressed over change-tracking columns that have
     *       no equivalent on the storage table, so it cannot become a filter there. The branch
     *       anti-joins against the {@code affected_identifiers} relation instead.</li>
     * </ul>
     *
     * <p>Exactly one mechanism applies per base: applying both would exclude more from the fresh
     * branch than the delta branch recomputes, dropping rows. A row is excluded when any base made
     * it stale, so the mechanisms compose across bases.
     */
    private static PlanNode buildDataTableBranch(
            Metadata metadata,
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable,
            boolean rowPreservingAdditions,
            boolean mergingDelta,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Lookup lookup)
    {
        PlanNode freshPlan = node.getDataTablePlan();
        Map<TableColumn, VariableReferenceExpression> storageColumns =
                buildColumnToVariableMapping(metadata, session, freshPlan, lookup);

        ImmutableList.Builder<TupleDomain<String>> storagePredicates = ImmutableList.builder();
        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> entry : constraints.entrySet()) {
            SchemaTableName baseTable = entry.getKey();
            if (entry.getValue().isEmpty() || isRowLevel(changedRowsPredicates.get(baseTable))) {
                continue;
            }
            storagePredicates.addAll(equivalentDataTablePredicates(entry.getValue(), baseTable, columnEquivalences, dataTable));
        }
        freshPlan = filterOutStaleRows(metadata, freshPlan, storagePredicates.build(), storageColumns, dataTable, idAllocator);

        return freshPlan;
    }

    /**
     * Whether the delta, taken over every stale base, is a pure increment on what storage already
     * holds -- the precondition for reusing the materialized rows rather than recomputing them.
     *
     * <p>Three things, all properties of the change set rather than of the view:
     *
     * <ul>
     *   <li>every changed-rows predicate is row-level, so the delta is the changed rows themselves
     *       rather than a recomputation of whole stale partitions;
     *   <li>no stale base is missing one, because such a base keeps a partition-level leaf whose
     *       delta replaces a value instead of adding to it, and whose storage filter can delete rows
     *       a row-level sibling contributed;
     *   <li>the range is additions only. A modified row is still present and still matches the
     *       disjuncts, indistinguishable from an appended one, so reusing the materialized row would
     *       keep a value that should have been replaced.
     * </ul>
     */
    private static boolean isPureIncrement(
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints)
    {
        return !changedRowsPredicates.isEmpty()
                && changedRowsPredicates.values().stream().allMatch(DifferentialPlanRewriter::isRowLevel)
                && changedRowsPredicates.values().stream().allMatch(ChangedRowsPredicate::isAdditionsOnly)
                && changedRowsPredicates.keySet().containsAll(staleConstraints.keySet());
    }

    private static boolean isRowLevel(ChangedRowsPredicate changedRows)
    {
        return changedRows != null && !changedRows.getDataDisjuncts().isEmpty();
    }

    /**
     * Rewrites a base table's stale disjuncts onto the equivalent storage table columns.
     */
    private static List<TupleDomain<String>> equivalentDataTablePredicates(
            List<TupleDomain<String>> disjuncts,
            SchemaTableName baseTable,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable)
    {
        ImmutableList.Builder<TupleDomain<String>> result = ImmutableList.builder();
        for (TupleDomain<String> disjunct : disjuncts) {
            TupleDomain<String> dataTablePredicate = columnEquivalences.getEquivalentPredicates(baseTable, disjunct).get(dataTable);
            if (dataTablePredicate == null || dataTablePredicate.isAll()) {
                throw new UnsupportedOperationException(format(
                        "stale predicate for %s has no equivalent over the storage table's columns", baseTable));
            }
            result.add(dataTablePredicate);
        }
        return result.build();
    }

    private static PlanNode filterOutStaleRows(
            Metadata metadata,
            PlanNode freshPlan,
            List<TupleDomain<String>> stalePredicates,
            Map<TableColumn, VariableReferenceExpression> storageColumns,
            SchemaTableName dataTable,
            PlanNodeIdAllocator idAllocator)
    {
        if (stalePredicates.isEmpty()) {
            return freshPlan;
        }

        RowExpressionDomainTranslator translator = new RowExpressionDomainTranslator(metadata);
        ImmutableList.Builder<RowExpression> staleExpressions = ImmutableList.builder();
        for (TupleDomain<String> predicate : stalePredicates) {
            TupleDomain<VariableReferenceExpression> bound =
                    predicate.transform(column -> storageColumns.get(new TableColumn(dataTable, column)));
            // An unbound predicate widens to all(), which would negate to FALSE and empty the fresh
            // branch. Decline instead of silently dropping every fresh row.
            if (bound.isAll()) {
                throw new UnsupportedOperationException(format(
                        "stale predicate is not expressible over the columns %s reads", dataTable));
            }
            staleExpressions.add(translator.toPredicate(bound));
        }

        return new FilterNode(
                freshPlan.getSourceLocation(),
                idAllocator.getNextId(),
                freshPlan,
                not(metadata.getFunctionAndTypeManager(), or(staleExpressions.build())));
    }

    /**
     * Maps every variable in the plan that ultimately carries a base table column back to that
     * column.
     *
     * <p>Reading only a TableScan's own assignments is not enough: by the time an aggregation is
     * reached its grouping keys have usually been renamed, most often by the GroupIdNode the
     * planner inserts, which turns {@code region} into something like {@code region$gid}. Such a
     * variable still carries exactly the base column, so it has to resolve to it -- otherwise a
     * row-level candidate can never identify its affected groups and always declines.
     *
     * <p>Only renames are followed. A computed expression is deliberately left unresolved, because
     * a predicate on a base column says nothing about the value of a function of it.
     */
    private static Map<VariableReferenceExpression, TableColumn> resolveBaseColumnsByVariable(
            Metadata metadata,
            Session session,
            PlanNode plan,
            Lookup lookup)
    {
        Map<VariableReferenceExpression, TableColumn> resolved = new HashMap<>();
        buildColumnToVariableMapping(metadata, session, plan, lookup)
                .forEach((column, variable) -> resolved.putIfAbsent(variable, column));

        // A rename may sit above another rename, so propagate until nothing new resolves.
        List<PlanNode> renamingNodes = ImmutableList.<PlanNode>builder()
                .addAll(searchFrom(plan, lookup).where(ProjectNode.class::isInstance).findAll())
                .addAll(searchFrom(plan, lookup).where(GroupIdNode.class::isInstance).findAll())
                .build();

        boolean progressed = true;
        while (progressed) {
            progressed = false;
            for (PlanNode node : renamingNodes) {
                for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> rename : renamesOf(node).entrySet()) {
                    TableColumn source = resolved.get(rename.getValue());
                    if (source != null && resolved.putIfAbsent(rename.getKey(), source) == null) {
                        progressed = true;
                    }
                }
            }
        }
        return ImmutableMap.copyOf(resolved);
    }

    /**
     * The output-to-input variable renames a node performs, ignoring anything that computes a value.
     */
    private static Map<VariableReferenceExpression, VariableReferenceExpression> renamesOf(PlanNode node)
    {
        if (node instanceof GroupIdNode) {
            return ((GroupIdNode) node).getGroupingColumns();
        }
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> renames = ImmutableMap.builder();
        ((ProjectNode) node).getAssignments().getMap().forEach((output, expression) -> {
            if (expression instanceof VariableReferenceExpression) {
                renames.put(output, (VariableReferenceExpression) expression);
            }
        });
        return renames.buildKeepingLast();
    }

    /**
     * What identifies a materialized row, which decides how the fresh branch excludes the rows the
     * delta is going to recompute.
     *
     * <p>Answered in one place because three callers need it and each used to ask separately: the
     * refresh safety check, the identifier resolution that drives the anti-join, and the
     * pass-through for views that need no exclusion. Any disagreement between them is a wrong plan
     * rather than a slower one.
     */
    enum ViewShape
    {
        /** One output row per group; the grouping keys identify it. */
        AGGREGATING,
        /** One output row per base row; the base row's identity identifies it. */
        ROW_PRESERVING,
        /** Row identity is not expressible, so row-level cannot exclude correctly. */
        UNSUPPORTED,
    }

    /**
     * The plan nodes a row-level candidate knows how to reason about.
     *
     * <p>Deliberately a whitelist. Every other node declines, including ones added to the planner
     * after this was written, because the question a shape has to answer is not "does the delta
     * builder handle this node" but "does keeping the materialized rows and adding the delta's rows
     * to them produce the view". A node can satisfy the first and fail the second.
     *
     * <p>{@code IntersectNode} and {@code ExceptNode} are the case that proved it. Both carry
     * {@code DISTINCT} semantics, and {@code EXCEPT} is anti-monotonic in its right input: appending
     * to the right side has to *remove* rows from the view. Classified as row-preserving they took
     * the additions-only shortcut, which keeps every materialized row, and the view went on
     * returning a row the base no longer contained.
     */
    private static final QualifiedObjectName BUILTIN_SUM = QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "sum");
    private static final QualifiedObjectName BUILTIN_COUNT = QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "count");

    private static final Set<Class<? extends PlanNode>> ROW_LEVEL_SUPPORTED_NODES = ImmutableSet.of(
            TableScanNode.class,
            FilterNode.class,
            ProjectNode.class,
            UnionNode.class,
            AggregationNode.class);

    /**
     * A join makes an output row depend on a row from each side, so identifying it needs a
     * composite origin the storage table does not carry; such a view stays partition-level. More
     * than one aggregation leaves the identity ambiguous -- which level's keys name the row -- so
     * it is not claimed either.
     *
     * <p>Every aggregation counts, not only the grouping ones. A global aggregate -- {@code SELECT
     * SUM(x) FROM t}, no {@code GROUP BY} -- produces a single row that no grouping key names, so
     * neither branch can place it: the fresh branch has no identifier to exclude the stale row by,
     * and the delta's own aggregate is a partial value rather than a replacement. Counting only
     * grouping aggregations classified such a view as row-preserving, which kept the stale row and
     * unioned the delta's partial aggregate beside it -- two rows where the view defines one.
     *
     * <p>Anything outside {@link #ROW_LEVEL_SUPPORTED_NODES} declines before either of those
     * questions is asked. Previously this returned {@code ROW_PRESERVING} for any plan that merely
     * held no join and no aggregation, so the shapes it could not actually serve were stopped only
     * by the delta builder refusing to visit them -- and for a set operation, which the delta builder
     * does visit, nothing stopped them at all.
     */
    static ViewShape classifyViewShape(PlanNode viewQueryPlan, Lookup lookup)
    {
        boolean unsupportedNode = searchFrom(viewQueryPlan, lookup)
                .where(node -> !ROW_LEVEL_SUPPORTED_NODES.contains(node.getClass()))
                .findFirst()
                .isPresent();
        if (unsupportedNode) {
            return ViewShape.UNSUPPORTED;
        }
        List<PlanNode> aggregations = searchFrom(viewQueryPlan, lookup)
                .where(AggregationNode.class::isInstance)
                .findAll();
        if (aggregations.isEmpty()) {
            return ViewShape.ROW_PRESERVING;
        }
        if (aggregations.size() > 1) {
            return ViewShape.UNSUPPORTED;
        }
        return ((AggregationNode) aggregations.get(0)).getGroupingKeys().isEmpty()
                ? ViewShape.UNSUPPORTED
                : ViewShape.AGGREGATING;
    }

    private static Map<TableColumn, VariableReferenceExpression> buildColumnToVariableMapping(
            Metadata metadata,
            Session session,
            PlanNode plan,
            Lookup lookup)
    {
        Map<TableColumn, VariableReferenceExpression> columnToVariable = new HashMap<>();
        for (PlanNode node : searchFrom(plan, lookup).where(TableScanNode.class::isInstance).findAll()) {
            TableScanNode tableScan = (TableScanNode) node;
            SchemaTableName tableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : tableScan.getAssignments().entrySet()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScan.getTable(), entry.getValue());
                TableColumn column = new TableColumn(tableName, columnMetadata.getName());
                VariableReferenceExpression previous = columnToVariable.putIfAbsent(column, entry.getKey());
                // Two variables bound to one base column means the subtree scans the table more than
                // once (a self-join, say), so there is no single variable a stale predicate on that
                // column can be rewritten to. Bail out rather than pick one arbitrarily; an
                // ImmutableMap builder would instead throw IllegalArgumentException, which is not
                // caught by the stitching fallback and would fail the query outright.
                if (previous != null && !previous.equals(entry.getKey())) {
                    throw new UnsupportedOperationException(format(
                            "base column %s is bound to more than one variable in the plan", column));
                }
            }
        }
        return ImmutableMap.copyOf(columnToVariable);
    }

    /**
     * The root aggregation of a view whose aggregates can be merged from their stored values,
     * or empty when no such merge is possible.
     *
     * <p>Recomputing an affected group by expanding the delta back to the group's other rows costs a
     * scan of the whole base, because the affected keys are only known once the delta has been read
     * and so cannot prune anything. A distributive aggregate needs none of that: its new value is a
     * function of its old value and the delta alone.
     *
     * <pre>
     *   SUM   : new = old + sum(delta)        merge is SUM
     *   COUNT : new = old + count(delta)      merge is SUM, not COUNT
     * </pre>
     *
     * <p>Only those two, and only over an append-only range. MIN and MAX merge the same way for
     * appends but not for removals -- a delete can take away the extreme, and no stored value can
     * recover it -- and the range being append-only is a property of the caller's predicate rather
     * than of this node, so they are left out. AVG is excluded because the stored average cannot be
     * merged without the count beside it, and holistic aggregates such as COUNT(DISTINCT) cannot be
     * merged at all.
     */
    private static Optional<MergeableAggregation> mergeableAggregationRoot(PlanNode viewQueryPlan, Metadata metadata, Lookup lookup)
    {
        // Planning a `SELECT key, SUM(x) ... GROUP BY key` leaves a rename-only projection above the
        // aggregation, so the aggregation is almost never the literal root. Walking down through
        // those projections records what each root output is called at the aggregation, which is
        // what classifying the view's outputs needs. A projection that computes anything -- rather
        // than passing a variable straight through -- stops the walk: an expression over an
        // aggregate is not itself mergeable.
        PlanNode current = lookup.resolve(viewQueryPlan);
        Map<VariableReferenceExpression, VariableReferenceExpression> aliases = new LinkedHashMap<>();
        for (VariableReferenceExpression output : current.getOutputVariables()) {
            aliases.put(output, output);
        }
        while (current instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) current;
            Map<VariableReferenceExpression, VariableReferenceExpression> next = new LinkedHashMap<>();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> alias : aliases.entrySet()) {
                RowExpression value = project.getAssignments().get(alias.getValue());
                if (!(value instanceof VariableReferenceExpression)) {
                    return Optional.empty();
                }
                next.put(alias.getKey(), (VariableReferenceExpression) value);
            }
            aliases = next;
            current = lookup.resolve(project.getSource());
        }
        if (!(current instanceof AggregationNode)) {
            return Optional.empty();
        }
        AggregationNode aggregation = (AggregationNode) current;
        if (aggregation.getGroupingKeys().isEmpty() || aggregation.getStep() != SINGLE) {
            return Optional.empty();
        }
        for (AggregationNode.Aggregation aggregate : aggregation.getAggregations().values()) {
            if (aggregate.isDistinct() || aggregate.getFilter().isPresent() || aggregate.getMask().isPresent()) {
                return Optional.empty();
            }
            if (!mergeFunctionName(aggregate, metadata).isPresent()) {
                return Optional.empty();
            }
        }
        return Optional.of(new MergeableAggregation(aggregation, aliases));
    }

    /**
     * An aggregation whose every aggregate can be merged, together with the name each output of the
     * view query plan carries at that aggregation.
     */
    private static final class MergeableAggregation
    {
        private final AggregationNode aggregation;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> aliases;

        private MergeableAggregation(AggregationNode aggregation, Map<VariableReferenceExpression, VariableReferenceExpression> aliases)
        {
            this.aggregation = aggregation;
            this.aliases = ImmutableMap.copyOf(aliases);
        }
    }

    /**
     * The function that merges this aggregate's stored value with the delta's, or empty when it has
     * none. Note COUNT merges with SUM: counts are added, not counted again.
     *
     * <p>Both the aggregate being recognised and the merge function returned are pinned to the
     * built-in namespace. Presto lets a catalog serve its own function namespace, so matching on the
     * bare name would accept a user-defined {@code sum} -- whose semantics need not be distributive
     * at all -- and then merge it with a function resolved against whatever the session's default
     * namespace happens to be. Both halves have to name the built-in for {@code old + delta} to be
     * the arithmetic this assumes.
     */
    private static Optional<QualifiedObjectName> mergeFunctionName(AggregationNode.Aggregation aggregate, Metadata metadata)
    {
        QualifiedObjectName name = metadata.getFunctionAndTypeManager()
                .getFunctionMetadata(aggregate.getFunctionHandle())
                .getName();
        if (BUILTIN_SUM.equals(name) || BUILTIN_COUNT.equals(name)) {
            return Optional.of(BUILTIN_SUM);
        }
        return Optional.empty();
    }

    /**
     * Wraps the stitched union in the aggregation that merges each group's stored value with the
     * increment the delta contributed.
     *
     * <p>The union carries one row per group from storage and, for the groups the delta touched, a
     * second row holding the delta's own aggregate. Re-aggregating by the grouping keys combines
     * them: {@code SUM} adds, and a {@code COUNT} is merged by adding the counts rather than
     * counting again. A group the delta never touched meets nothing to merge with, and summing a
     * single value returns it unchanged, which is why the fresh branch can keep every group instead
     * of anti-joining the affected ones away.
     *
     * <p>Returns empty when any output of the view cannot be classified as either a grouping key or
     * a mergeable aggregate, so that an unrecognised shape falls back rather than being merged on a
     * guess.
     */
    private static Optional<PlanNode> buildMergeAggregation(
            List<VariableReferenceExpression> outputVariables,
            Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMappings,
            MergeableAggregation viewAggregation,
            PlanNode union,
            Metadata metadata,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator)
    {
        FunctionAndTypeManager functions = metadata.getFunctionAndTypeManager();

        ImmutableList.Builder<VariableReferenceExpression> groupingKeys = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> merges = ImmutableMap.builder();
        Assignments.Builder renames = Assignments.builder();

        for (VariableReferenceExpression output : outputVariables) {
            VariableReferenceExpression rootVariable = viewQueryMappings.get(output);
            VariableReferenceExpression queryVariable = rootVariable == null ? null : viewAggregation.aliases.get(rootVariable);
            if (queryVariable == null) {
                return Optional.empty();
            }
            if (viewAggregation.aggregation.getGroupingKeys().contains(queryVariable)) {
                groupingKeys.add(output);
                renames.put(output, output);
                continue;
            }
            AggregationNode.Aggregation aggregate = viewAggregation.aggregation.getAggregations().get(queryVariable);
            if (aggregate == null) {
                return Optional.empty();
            }
            Optional<QualifiedObjectName> mergeName = mergeFunctionName(aggregate, metadata);
            if (!mergeName.isPresent()) {
                return Optional.empty();
            }
            // The merged value is a new variable: an aggregation cannot read and write one name.
            VariableReferenceExpression merged = variableAllocator.newVariable(output.getSourceLocation(), output.getName() + "_merged", output.getType());
            merges.put(merged, new AggregationNode.Aggregation(
                    new CallExpression(
                            mergeName.get().getObjectName(),
                            functions.lookupFunction(mergeName.get(), fromTypes(output.getType())),
                            output.getType(),
                            ImmutableList.of(output)),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty()));
            renames.put(output, merged);
        }

        List<VariableReferenceExpression> keys = groupingKeys.build();
        if (keys.isEmpty()) {
            return Optional.empty();
        }
        AggregationNode merged = new AggregationNode(
                union.getSourceLocation(),
                idAllocator.getNextId(),
                union,
                merges.build(),
                new AggregationNode.GroupingSetDescriptor(keys, 1, ImmutableSet.of()),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        // Rename the merged aggregates back to the view's own output variables so that everything
        // above the materialized view scan still resolves.
        return Optional.of(new ProjectNode(idAllocator.getNextId(), merged, renames.build()));
    }

    private static PlanNode buildUnionNode(
            List<VariableReferenceExpression> outputVariables,
            PlanNode freshPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> freshMapping,
            PlanNode deltaPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping,
            PlanNodeIdAllocator idAllocator)
    {
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs =
                ImmutableListMultimap.builder();

        for (VariableReferenceExpression outputVar : outputVariables) {
            VariableReferenceExpression freshVar = freshMapping.get(outputVar);
            VariableReferenceExpression deltaVar = deltaMapping.get(outputVar);
            checkState(freshVar != null && deltaVar != null,
                    "Missing mapping for output variable %s: freshVar=%s, deltaVar=%s", outputVar, freshVar, deltaVar);
            outputsToInputs.put(outputVar, freshVar);
            outputsToInputs.put(outputVar, deltaVar);
        }

        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mapping = outputsToInputs.build();
        return new UnionNode(
                freshPlan.getSourceLocation(),
                idAllocator.getNextId(),
                ImmutableList.of(freshPlan, deltaPlan),
                ImmutableList.copyOf(mapping.keySet()),
                SetOperationNodeUtils.fromListMultimap(mapping));
    }

    /**
     * A refresh plan that recomputes the view from its own stored aggregates plus the appended rows,
     * instead of from the base table.
     *
     * <p>{@link #buildDeltaPlanForRefresh} recomputes each affected group by rereading that group's
     * other base rows, and the affected keys are runtime values, so nothing prunes that scan. This
     * adds the delta's increment onto what storage already holds -- {@code total = stored + delta} --
     * and so reads one row per group plus the appended rows, never the base.
     *
     * <p>It emits every group, not only the affected ones, and that is what makes it safe to commit
     * whichever way the connector chooses: the output is a complete recomputation of the view, so
     * replacing the written partitions and overwriting the whole storage table produce the same
     * table. A partial delta is correct only under the first. That is also why this needs neither
     * partition-level staleness nor a storage table partitioned by the grouping columns, which the
     * expansion-based path does need.
     *
     * <p>Emitting every group is sound only because a mergeable view cannot drop one.
     * {@link #mergeableAggregationRoot} peels projections and stops at anything else, so a view with
     * a filter above its aggregation -- a {@code HAVING} -- is never mergeable, and a group can
     * therefore only appear or grow. If that ever changes this becomes unsafe for exactly the groups
     * that vanish.
     *
     * <p>The cost of emitting every group is write volume: partition-level writes only the stale
     * groups, this writes all of them. On a view with more groups than the engine's open-writer limit
     * that is a real difference -- though the same limit already blocks a plain full refresh, and
     * even a plain insert into a base with that many partitions, so it is a property of wide
     * partitioned tables rather than of this plan.
     *
     * <p>Returns empty when the change set is not a pure increment or the view is not mergeable, in
     * which case the caller keeps the expansion-based plan.
     */
    public static Optional<PlanNode> buildMergedRefreshPlan(
            RefreshMaterializedViewNode node,
            TableHandle storageTableHandle,
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        if (!isPureIncrement(changedRowsPredicates, constraints)) {
            return Optional.empty();
        }
        Optional<MergeableAggregation> mergeRoot = mergeableAggregationRoot(node.getSource(), metadata, lookup);
        if (!mergeRoot.isPresent()) {
            return Optional.empty();
        }

        List<VariableReferenceExpression> outputVariables = node.getOutputVariables();
        Optional<TableScanNode> storageScan = buildStorageScan(metadata, session, storageTableHandle, outputVariables, idAllocator, variableAllocator);
        if (!storageScan.isPresent()) {
            return Optional.empty();
        }

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                constraints,
                changedRowsPredicates,
                columnEquivalences,
                lookup,
                warningCollector,
                mergeRoot);

        // Both the delta plan and the merge are expressed in the refresh node's own output
        // variables, so each maps to itself.
        Map<VariableReferenceExpression, VariableReferenceExpression> outputIdentity =
                outputVariables.stream().collect(toImmutableMap(identity(), identity()));

        NodeWithMapping deltaResult;
        try {
            deltaResult = builder.buildDeltaPlan(node.getSource(), outputIdentity);
        }
        catch (UnsupportedOperationException e) {
            return Optional.empty();
        }

        Map<VariableReferenceExpression, VariableReferenceExpression> storageMapping = new LinkedHashMap<>();
        List<VariableReferenceExpression> storageVariables = storageScan.get().getOutputVariables();
        for (int index = 0; index < outputVariables.size(); index++) {
            storageMapping.put(outputVariables.get(index), storageVariables.get(index));
        }

        PlanNode union = buildUnionNode(
                outputVariables,
                storageScan.get(),
                storageMapping,
                deltaResult.getNode(),
                deltaResult.getMapping(),
                idAllocator);

        return buildMergeAggregation(
                outputVariables,
                outputIdentity,
                mergeRoot.get(),
                union,
                metadata,
                idAllocator,
                variableAllocator);
    }

    /**
     * A scan of the materialized view's storage table, one variable per view output.
     *
     * <p>Storage columns correspond to view outputs by position, which is the correspondence
     * {@code RelationPlanner} relies on when it builds the stitching scan. A storage table whose
     * visible column count disagrees with the view's output count is not one this can read, so it
     * declines rather than pairing columns that may not match.
     *
     * <p>The handle is re-resolved by name rather than reused. The one the refresh node carries is
     * the write target -- Iceberg reports it as "&lt;storage table&gt;$data" -- and scanning that
     * inside the same statement yields the output snapshot, which is empty. Reusing it silently
     * replaced each affected group with the delta alone, discarding the stored value the merge is
     * supposed to add to.
     */
    private static Optional<TableScanNode> buildStorageScan(
            Metadata metadata,
            Session session,
            TableHandle storageTableHandle,
            List<VariableReferenceExpression> outputVariables,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator)
    {
        SchemaTableName storageTable = metadata.getTableMetadata(session, storageTableHandle).getTable();
        Optional<TableHandle> readHandle = metadata.getHandleVersion(
                session,
                QualifiedObjectName.valueOf(
                        storageTableHandle.getConnectorId().getCatalogName(),
                        storageTable.getSchemaName(),
                        storageTable.getTableName()),
                Optional.empty());
        if (!readHandle.isPresent()) {
            return Optional.empty();
        }

        List<ColumnMetadata> columns = metadata.getTableMetadata(session, readHandle.get()).getColumns().stream()
                .filter(column -> !column.isHidden())
                .collect(toImmutableList());
        if (columns.size() != outputVariables.size()) {
            return Optional.empty();
        }
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, readHandle.get());

        ImmutableList.Builder<VariableReferenceExpression> scanVariables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        for (int index = 0; index < columns.size(); index++) {
            ColumnHandle handle = columnHandles.get(columns.get(index).getName());
            if (handle == null) {
                return Optional.empty();
            }
            VariableReferenceExpression variable = variableAllocator.newVariable(
                    outputVariables.get(index).getSourceLocation(),
                    columns.get(index).getName(),
                    outputVariables.get(index).getType());
            scanVariables.add(variable);
            assignments.put(variable, handle);
        }

        return Optional.of(new TableScanNode(
                Optional.empty(),
                idAllocator.getNextId(),
                readHandle.get(),
                scanVariables.build(),
                assignments.build(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty()));
    }

    /**
     * Builds the delta a refresh writes, by recomputing the affected groups from the base table.
     *
     * <p>With row-level changed-rows predicates the delta recomputes only the groups the connector
     * reports as changed, instead of every group in a stale partition. Recomputing a group means
     * rereading that group's other base rows, and the affected keys are not known until the delta
     * has been read, so nothing prunes that scan -- which is why
     * {@link #buildMergedRefreshPlan} is tried first for the views that can merge instead.
     *
     * <p>Because this emits only the affected groups, it is correct only when the refresh commits by
     * replacing the partitions of the files it wrote. The caller is responsible for having
     * established that: {@code IncrementalRefreshRule} requires partition-level staleness and a
     * storage table partitioned by the grouping columns before taking this path, because replacing a
     * partition silently discards anything else that partition held.
     */
    public static Optional<PlanNode> buildDeltaPlanForRefresh(
            RefreshMaterializedViewNode node,
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        PlanNode sourcePlan = node.getSource();

        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                sourcePlan.getOutputVariables().stream()
                        .collect(toImmutableMap(identity(), identity()));

        DifferentialPlanRewriter rewriter = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                constraints,
                changedRowsPredicates,
                columnEquivalences,
                lookup,
                warningCollector);

        NodeWithMapping deltaResult;
        try {
            deltaResult = rewriter.buildDeltaPlan(sourcePlan, identityMapping);
        }
        catch (UnsupportedOperationException e) {
            return Optional.empty();
        }

        // Build projection to map delta variables back to original output variables
        Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = deltaResult.getMapping();
        List<VariableReferenceExpression> originalOutputs = node.getOutputVariables();

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression originalVar : originalOutputs) {
            VariableReferenceExpression deltaVar = deltaMapping.get(originalVar);
            checkState(deltaVar != null,
                    "Delta plan is missing mapping for output variable %s. " +
                    "This indicates an incomplete variable mapping from the differential plan rewriter.", originalVar);
            assignments.put(originalVar, deltaVar);
        }

        ProjectNode projectedDelta = new ProjectNode(
                idAllocator.getNextId(),
                deltaResult.getNode(),
                assignments.build());

        return Optional.of(projectedDelta);
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
            checkState(deltaVar != null,
                    "Missing delta mapping for view query variable %s", entry.getValue());
            composedMapping.put(entry.getKey(), deltaVar);
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

            ChangedRowsPredicate changedRows = changedRowsPredicates.get(tableName);
            if (changedRows != null && !changedRows.getDataDisjuncts().isEmpty()) {
                return rowLevelTableScan(node, tableName, changedRows);
            }
            return partitionLevelTableScan(node, tableName);
        }

        /**
         * The partition-level leaf: the delta is the base rows in a stale partition, and the stale
         * boundary is expressed over the partition columns, so the closure records them and an
         * aggregation above can take Case A when they cover its grouping keys.
         */
        private PlanVariants partitionLevelTableScan(TableScanNode node, SchemaTableName tableName)
        {
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
                    new NodeWithMapping(unchangedNode, unchangedResult.getMapping()),
                    Optional.of(stalePredicates.stream()
                            .flatMap(predicate -> predicate.getDomains().orElse(ImmutableMap.of()).keySet().stream())
                            .map(column -> new TableColumn(tableName, column))
                            .collect(toImmutableSet())));
        }

        /**
         * The row-level leaf: the delta is the base rows the connector reports as changed. The
         * predicate is expressed over change-tracking columns the view query does not project, so
         * each variant re-reads the table with those columns added and projects them away again,
         * leaving the schema the parent operators expect.
         *
         * <p>The closure is None because a row-level boundary says nothing about any grouping key.
         * That is what forces an aggregation above into Case B expansion, which is correct for any
         * aggregation function.
         */
        private PlanVariants rowLevelTableScan(TableScanNode node, SchemaTableName tableName, ChangedRowsPredicate changedRows)
        {
            Set<ColumnHandle> predicateColumns = changedRows.getDataDisjuncts().stream()
                    .flatMap(disjunct -> disjunct.getDomains().orElse(ImmutableMap.of()).keySet().stream())
                    .collect(toImmutableSet());

            RowLevelScan delta = buildRowLevelScan(node, predicateColumns);
            RowLevelScan unchanged = buildRowLevelScan(node, predicateColumns);
            NodeWithMapping current = buildTableScan(node, node.getOutputVariables());

            RowExpression changedPredicate = buildChangedRowsPredicate(tableName, changedRows, delta);
            RowExpression unchangedPredicate = not(metadata.getFunctionAndTypeManager(),
                    buildChangedRowsPredicate(tableName, changedRows, unchanged));

            return new PlanVariants(
                    delta.filterAndRestore(node, changedPredicate),
                    current,
                    unchanged.filterAndRestore(node, unchangedPredicate),
                    Optional.empty());
        }

        private RowExpression buildChangedRowsPredicate(SchemaTableName tableName, ChangedRowsPredicate changedRows, RowLevelScan scan)
        {
            ImmutableList.Builder<RowExpression> disjuncts = ImmutableList.builder();
            for (TupleDomain<ColumnHandle> disjunct : changedRows.getDataDisjuncts()) {
                TupleDomain<VariableReferenceExpression> bound = disjunct.transform(scan::variableFor);
                if (bound.isAll()) {
                    throw new UnsupportedOperationException(format(
                            "changed-rows predicate for %s is not expressible over the scanned columns", tableName));
                }
                disjuncts.add(translator.toPredicate(bound));
            }
            return or(disjuncts.build());
        }

        /**
         * Rebuilds a scan with extra columns appended, remembering both the mapping the parent
         * operators use and the variable each extra column landed on.
         */
        private RowLevelScan buildRowLevelScan(TableScanNode original, Set<ColumnHandle> extraColumns)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping = createFreshMapping(original.getOutputVariables());
            TableScanNode remapped = new SymbolMapper(mapping, warningCollector).map(original, idAllocator.getNextId());

            Map<ColumnHandle, VariableReferenceExpression> alreadyRead = new HashMap<>();
            remapped.getAssignments().forEach((variable, handle) -> alreadyRead.putIfAbsent(handle, variable));

            List<VariableReferenceExpression> outputs = new ArrayList<>(remapped.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>(remapped.getAssignments());
            ImmutableMap.Builder<ColumnHandle, VariableReferenceExpression> extras = ImmutableMap.builder();
            for (ColumnHandle handle : extraColumns) {
                VariableReferenceExpression present = alreadyRead.get(handle);
                if (present != null) {
                    extras.put(handle, present);
                    continue;
                }
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, original.getTable(), handle);
                VariableReferenceExpression variable = variableAllocator.newVariable(columnMetadata.getName(), columnMetadata.getType());
                outputs.add(variable);
                assignments.put(variable, handle);
                extras.put(handle, variable);
            }

            TableScanNode scan = new TableScanNode(
                    remapped.getSourceLocation(),
                    idAllocator.getNextId(),
                    remapped.getTable(),
                    ImmutableList.copyOf(outputs),
                    ImmutableMap.copyOf(assignments),
                    remapped.getTableConstraints(),
                    remapped.getCurrentConstraint(),
                    remapped.getEnforcedConstraint(),
                    remapped.getCteMaterializationInfo());

            return new RowLevelScan(scan, mapping, extras.build());
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
            ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
            for (TupleDomain<String> disjunct : stalePredicates) {
                TupleDomain<VariableReferenceExpression> bound = disjunct.transform(columnToVariable::get);
                // TupleDomain.transform silently drops columns the mapper cannot resolve, so an
                // unbound disjunct widens to all() and toPredicate turns it into TRUE. That would
                // make the delta branch recompute the whole base while the fresh branch still
                // contributes rows, duplicating output and breaking the disjointness invariant.
                if (bound.isAll()) {
                    throw new UnsupportedOperationException(format(
                            "stale predicate for %s is not expressible over the scanned columns",
                            metadata.getTableMetadata(session, node.getTable()).getTable()));
                }
                predicates.add(translator.toPredicate(bound));
            }
            // An empty disjunct list means this base table is not stale, so an empty delta is correct.
            return or(predicates.build());
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
                    buildFilter(node, child.unchanged()),
                    child.deltaClosureColumns());
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
            //
            // The closure passes through untouched. It names base table columns, and a projection
            // does not change which base columns the leaf predicate is expressed over -- only what
            // the rows are called by the time they arrive. Whether a projection preserved the
            // correspondence is decided where it matters, in requiresExpandedDelta, which resolves
            // each grouping key back to a base column and expands whenever one cannot be resolved.
            // Discarding the closure here instead made Case A unreachable for any plan with a
            // projection, which is nearly all of them, so every aggregation paid for the expansion.
            return new PlanVariants(
                    buildProject(node, child.delta()),
                    buildProject(node, child.current()),
                    buildProject(node, child.unchanged()),
                    child.deltaClosureColumns());
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

            NodeWithMapping delta = child.delta();
            if (requiresExpandedDelta(node, child)) {
                delta = buildExpandedDelta(node, child);
            }

            return new PlanVariants(
                    buildAggregation(node, delta),
                    buildAggregation(node, child.current()),
                    buildAggregation(node, child.unchanged()),
                    Optional.empty());
        }

        private boolean requiresExpandedDelta(AggregationNode node, PlanVariants child)
        {
            if (node.getGroupingKeys().isEmpty()) {
                return false;
            }

            // Expansion exists to recompute a group from scratch. A merged aggregate does not need
            // the group's other rows at all, so the delta is aggregated as it stands.
            if (deltaMergeRoot.isPresent() && deltaMergeRoot.get().aggregation == node) {
                return false;
            }

            Optional<Set<TableColumn>> closure = child.deltaClosureColumns();
            if (!closure.isPresent()) {
                return true;
            }

            // A known-but-empty closure means no column bounds the delta, so Case A's precondition
            // -- every group lies wholly inside or wholly outside it -- holds vacuously and there is
            // nothing for the grouping keys to cover. Checking this before resolving them matters
            // for a UNION: intersecting the branches' closures leaves the empty set whenever the
            // branches name columns of different tables, and a union output cannot be resolved back
            // to one base column, so the resolution below would choose the expansion for every
            // UNION-defined view.
            if (closure.get().isEmpty()) {
                return false;
            }

            // Resolve through renames: by this point the planner has usually renamed the grouping
            // keys, most often via GroupIdNode, and reading the scan's own assignments alone would
            // find none of them and always choose the expansion.
            Map<VariableReferenceExpression, TableColumn> baseColumns =
                    resolveBaseColumnsByVariable(metadata, session, node.getSource(), lookup);

            ImmutableSet.Builder<TableColumn> groupingColumns = ImmutableSet.builder();
            for (VariableReferenceExpression groupingKey : node.getGroupingKeys()) {
                TableColumn column = baseColumns.get(groupingKey);
                if (column == null) {
                    // A grouping key computed from a base column, rather than renamed from one,
                    // tells us nothing about how groups line up with the stale boundary. Expand.
                    return true;
                }
                groupingColumns.add(column);
            }
            // Case A applies when the stale boundary is expressed only over grouping columns, so
            // every group is wholly inside or wholly outside the delta.
            return !groupingColumns.build().containsAll(closure.get());
        }

        private NodeWithMapping buildExpandedDelta(AggregationNode aggregation, PlanVariants child)
        {
            // The caller also emits child.current() as this aggregation's current variant, so the
            // expansion must read its own copy. Sharing the instance would place one subtree, with
            // its plan node ids and variables, at two positions in the same plan.
            NodeWithMapping current = cloneNodeWithMapping(child.current());

            List<VariableReferenceExpression> deltaGroupingKeys = aggregation.getGroupingKeys().stream()
                    .map(child.delta().getMapping()::get)
                    .collect(toImmutableList());
            List<VariableReferenceExpression> currentGroupingKeys = aggregation.getGroupingKeys().stream()
                    .map(current.getMapping()::get)
                    .collect(toImmutableList());

            checkState(!deltaGroupingKeys.contains(null) && !currentGroupingKeys.contains(null),
                    "Missing grouping-key mapping for expanded materialized view delta");

            // Expansion now serves only the partition-level delta, whose stale set is a plan-time
            // constant, so the affected groups come from the delta itself.
            PlanNode affectedGroups = new AggregationNode(
                    aggregation.getSourceLocation(),
                    idAllocator.getNextId(),
                    child.delta().getNode(),
                    ImmutableMap.of(),
                    new AggregationNode.GroupingSetDescriptor(deltaGroupingKeys, 1, ImmutableSet.of()),
                    ImmutableList.of(),
                    SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            List<VariableReferenceExpression> affectedGroupKeys = deltaGroupingKeys;

            FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
            List<RowExpression> matches = new ArrayList<>();
            for (int index = 0; index < currentGroupingKeys.size(); index++) {
                VariableReferenceExpression currentKey = currentGroupingKeys.get(index);
                VariableReferenceExpression affectedKey = affectedGroupKeys.get(index);
                matches.add(new CallExpression(
                        "NOT",
                        functionResolution.notFunction(),
                        BooleanType.BOOLEAN,
                        ImmutableList.of(new CallExpression(
                                OperatorType.IS_DISTINCT_FROM.name(),
                                functionResolution.comparisonFunction(OperatorType.IS_DISTINCT_FROM, currentKey.getType(), affectedKey.getType()),
                                BooleanType.BOOLEAN,
                                ImmutableList.of(currentKey, affectedKey)))));
            }

            List<VariableReferenceExpression> joinOutputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(current.getNode().getOutputVariables())
                    .addAll(affectedGroups.getOutputVariables())
                    .build();
            JoinNode matchingRows = new JoinNode(
                    aggregation.getSourceLocation(),
                    idAllocator.getNextId(),
                    INNER,
                    current.getNode(),
                    affectedGroups,
                    ImmutableList.of(),
                    joinOutputs,
                    Optional.of(and(matches)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());

            Assignments.Builder assignments = Assignments.builder();
            for (VariableReferenceExpression variable : current.getNode().getOutputVariables()) {
                assignments.put(variable, variable);
            }
            return new NodeWithMapping(
                    new ProjectNode(
                            aggregation.getSourceLocation(),
                            idAllocator.getNextId(),
                            matchingRows,
                            assignments.build(),
                            ProjectNode.Locality.LOCAL),
                    current.getMapping());
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

            return new PlanVariants(deltaResult, currentResult, unchangedResult, intersectClosures(leftVariants, rightVariants));
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
                    buildUnion(node, children.stream().map(PlanVariants::unchanged).collect(toImmutableList())),
                    intersectClosures(children));
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
            // ∆(R ∩ S) = (∆R ∩ S') ∪ (R ∩ ∆S)
            // The delta formula assumes binary INTERSECT; n-ary INTERSECT should be
            // decomposed into a binary tree before reaching this code.
            List<PlanNode> sources = node.getSources();
            checkState(sources.size() == 2,
                    "INTERSECT with more than 2 sources is not supported for differential stitching, found %s sources",
                    sources.size());
            List<PlanVariants> allVariants = visitAllSources(sources, context);

            // Current: R' ∩ S'
            NodeWithMapping currentResult = buildIntersect(node, allVariants.stream().map(PlanVariants::current).collect(toImmutableList()));

            // Unchanged: R ∩ S
            NodeWithMapping unchangedResult = buildIntersect(node, allVariants.stream().map(PlanVariants::unchanged).collect(toImmutableList()));

            // Delta: union of left and right delta terms
            IntersectNode deltaLeft = buildIntersectDeltaLeft(node, sources, allVariants);
            IntersectNode deltaRight = buildIntersectDeltaRight(node, sources, allVariants);
            NodeWithMapping deltaUnionResult = createBinaryUnion(node, deltaLeft, deltaRight);

            return new PlanVariants(deltaUnionResult, currentResult, unchangedResult, intersectClosures(allVariants));
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
            // The delta formula assumes binary EXCEPT; n-ary EXCEPT should be
            // decomposed into a binary tree before reaching this code.
            List<PlanNode> sources = node.getSources();
            checkState(sources.size() == 2,
                    "EXCEPT with more than 2 sources is not supported for differential stitching, found %s sources",
                    sources.size());
            List<PlanVariants> allVariants = visitAllSources(sources, context);

            // Current: A' - B'
            NodeWithMapping currentResult = buildExcept(node, allVariants.stream().map(PlanVariants::current).collect(toImmutableList()));

            // Unchanged: A - B'
            // The right side must be B' (current) because rows added to B since the last
            // refresh may cancel out rows in A. Using stale B would include rows that no
            // longer survive the EXCEPT.
            ImmutableList.Builder<NodeWithMapping> unchangedSources = ImmutableList.builder();
            unchangedSources.add(allVariants.get(0).unchanged());
            for (int i = 1; i < allVariants.size(); i++) {
                unchangedSources.add(cloneNodeWithMapping(allVariants.get(i).current()));
            }
            NodeWithMapping unchangedResult = buildExcept(node, unchangedSources.build());

            // Delta: union of left and right delta terms
            ExceptNode deltaLeft = buildExceptDeltaLeft(node, allVariants);
            ExceptNode deltaRight = buildExceptDeltaRight(node, sources, allVariants);
            NodeWithMapping deltaUnionResult = createBinaryUnion(node, deltaLeft, deltaRight);

            return new PlanVariants(deltaUnionResult, currentResult, unchangedResult, intersectClosures(allVariants));
        }

        private Optional<Set<TableColumn>> intersectClosures(PlanVariants... variants)
        {
            return intersectClosures(Arrays.asList(variants));
        }

        private Optional<Set<TableColumn>> intersectClosures(List<PlanVariants> variants)
        {
            if (variants.stream().anyMatch(variant -> !variant.deltaClosureColumns().isPresent())) {
                return Optional.empty();
            }

            if (variants.isEmpty()) {
                return Optional.of(ImmutableSet.of());
            }

            ImmutableSet.Builder<TableColumn> result = ImmutableSet.builder();
            for (TableColumn column : variants.get(0).deltaClosureColumns().get()) {
                if (variants.stream().allMatch(variant -> variant.deltaClosureColumns().get().contains(column))) {
                    result.add(column);
                }
            }
            return Optional.of(result.build());
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
         * Builds a stale predicate for the left side of INTERSECT/EXCEPT by finding stale TableScans
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

            @Override
            public PlanNode visitGroupReference(GroupReference node, Void context)
            {
                throw new IllegalStateException(
                        "GroupReference should have been resolved by DeltaBuilder before cloning. " +
                        "This indicates the plan was not fully resolved before SubtreeRemappingVisitor was invoked.");
            }
        }
    }

    /**
     * A base scan re-read with extra change-tracking columns appended, so a predicate over columns
     * the view query does not project can still be applied at the leaf.
     */
    private class RowLevelScan
    {
        private final TableScanNode scan;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping;
        private final Map<ColumnHandle, VariableReferenceExpression> extraColumns;

        RowLevelScan(
                TableScanNode scan,
                Map<VariableReferenceExpression, VariableReferenceExpression> mapping,
                Map<ColumnHandle, VariableReferenceExpression> extraColumns)
        {
            this.scan = requireNonNull(scan, "scan is null");
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
            this.extraColumns = ImmutableMap.copyOf(requireNonNull(extraColumns, "extraColumns is null"));
        }

        VariableReferenceExpression variableFor(ColumnHandle column)
        {
            return extraColumns.get(column);
        }

        /**
         * Applies the predicate and projects back to the columns the view query scan produced, so
         * the appended change-tracking columns do not leak into the parent operators.
         */
        NodeWithMapping filterAndRestore(TableScanNode original, RowExpression predicate)
        {
            FilterNode filtered = new FilterNode(scan.getSourceLocation(), idAllocator.getNextId(), scan, predicate);
            List<VariableReferenceExpression> restored = original.getOutputVariables().stream()
                    .map(mapping::get)
                    .collect(toImmutableList());
            if (restored.equals(scan.getOutputVariables())) {
                return new NodeWithMapping(filtered, mapping);
            }
            return new NodeWithMapping(
                    new ProjectNode(
                            scan.getSourceLocation(),
                            idAllocator.getNextId(),
                            filtered,
                            identityAssignments(restored),
                            ProjectNode.Locality.LOCAL),
                    mapping);
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
        private final Optional<Set<TableColumn>> deltaClosureColumns;

        PlanVariants(NodeWithMapping delta, NodeWithMapping current, NodeWithMapping unchanged)
        {
            this(delta, current, unchanged, Optional.empty());
        }

        PlanVariants(NodeWithMapping delta, NodeWithMapping current, NodeWithMapping unchanged, Optional<Set<TableColumn>> deltaClosureColumns)
        {
            this.delta = requireNonNull(delta, "delta is null");
            this.current = requireNonNull(current, "current is null");
            this.unchanged = requireNonNull(unchanged, "unchanged is null");
            this.deltaClosureColumns = requireNonNull(deltaClosureColumns, "deltaClosureColumns is null")
                    .map(ImmutableSet::copyOf);
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

        Optional<Set<TableColumn>> deltaClosureColumns()
        {
            return deltaClosureColumns;
        }
    }
}

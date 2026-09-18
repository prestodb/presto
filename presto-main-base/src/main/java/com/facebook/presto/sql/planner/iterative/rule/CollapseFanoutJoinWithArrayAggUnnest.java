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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ActualProperties;
import com.facebook.presto.sql.planner.optimizations.PropertyDerivations;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isLegacyUnnest;
import static com.facebook.presto.SystemSessionProperties.isOptimizeJoinFanOut;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.planner.PlannerUtils.createArrayAggregation;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Collapses a fan-out equi-join: a join one of whose sides is provably non-unique on its join keys
 * while being unique on some strict superset of them — turning the {@code 1-to-N} join into a
 * {@code N-to-1} join plus a cheap local {@code UNNEST}.
 *
 * <p>The canonical aggregation shape is:
 * <pre>
 * SELECT a.*, b.k2, b.measure
 * FROM a
 * JOIN (SELECT k1, k2, SUM(v) AS measure FROM t GROUP BY k1, k2) b
 *   ON a.k1 = b.k1                 -- join key k1 is a STRICT SUBSET of (k1, k2)
 * </pre>
 * which is rewritten to:
 * <pre>
 * SELECT a.*, t.r[1] AS k2, t.r[2] AS measure
 * FROM a
 * JOIN (SELECT k1, array_agg(row(k2, measure)) AS data
 *       FROM (SELECT k1, k2, SUM(v) AS measure FROM t GROUP BY k1, k2)
 *       GROUP BY k1) b
 *   ON a.k1 = b.k1
 * CROSS JOIN UNNEST(data) AS t(r)         -- legacy array-of-rows UNNEST: one ROW column r
 * </pre>
 *
 * <p>All non-key columns are packed into a single {@code array_agg(row(...))}, so alignment is
 * automatic — each array element is a complete source row — and no {@code ORDER BY} or cross-array
 * coordination is required (the array's element order is irrelevant since the join output is a
 * multiset). The array is re-expanded with {@code UNNEST}, emitting the form that matches the
 * session's unnest semantics: under {@code legacy_unnest} a single {@code ROW} column {@code r}
 * whose fields are recovered as {@code r[i]} (a {@code DEREFERENCE}), otherwise one flattened
 * column per field. The Java engine handles both forms; the native (Velox) engine currently only
 * supports the legacy single-{@code ROW} form for array-of-rows {@code UNNEST}.
 *
 * <p>The collapse transformation is independent of the collapsed side's node type: given the
 * resolved side {@code S}, the outer join keys {@code J}, and {@code R = S.outputs − J}, the rewrite
 * always (1) projects {@code row(R...)} and {@code array_agg}s it grouped by {@code J} (now unique on
 * {@code J}), (2) rebuilds the outer join {@code N-to-1}, (3) {@code UNNEST}s the array into one
 * {@code ROW} column, and (4) dereferences that row back into {@code R}. Only the eligibility
 * detection varies.
 *
 * <p>The rewrite is losslessly semantics-preserving: {@code array_agg} packs the non-key columns
 * and {@code UNNEST} unpacks them, reproducing the same multiset of rows. The row multiplication
 * moves out of the distributed join (smaller build, unique-key join, less shuffle of duplicated
 * rows) into a streaming local {@code UNNEST}.
 *
 * <h2>Eligibility: is the side a fan-out?</h2>
 *
 * <p>Correctness does not depend on the side fanning out — the pack/unpack pair is an identity for
 * any side. Fan-out detection is a <em>profitability</em> guard: with one row per join key the
 * rewrite only adds an aggregation and an unnest.
 *
 * <p>It is answered at the join node from the grouping the side advertises through
 * {@code PropertyDerivations}, which is applied node by node so that a node type it does not cover
 * (a {@code UnionNode}, a CTE node) reports unknown properties instead of failing the derivation. An {@link AggregationNode} (including a {@code DISTINCT}) reports
 * {@code LocalProperties.grouped(groupingKeys)}, and {@code ActualProperties} carry that up through
 * the filters, projections, sorts and limits that sit between it and the join, and across an inner
 * join from its probe. A side grouped on a strict superset of the join keys holds several rows per
 * join key. Deliberately NOT the {@code LogicalProperties} constraints framework: that is gated
 * behind {@code exploit_constraints}, and nothing here may depend on it. The cost is that a side
 * whose grouping keys are projected away before the join is not detected, and that grouping carries
 * no cardinality, so an at-most-one-row side may be collapsed pointlessly (harmless).
 *
 * <h2>Outer joins</h2>
 *
 * <p>The collapsed side may be either the preserved or the null-supplying side of an outer join.
 * A preserved side always carries a non-empty array, so it unnests directly. A null-supplying side
 * produces a {@code NULL} array for every unmatched preserved row, and since the only available
 * unnest is {@code CROSS JOIN UNNEST} (there is no outer/left unnest) that row would be dropped.
 * The array is therefore wrapped in {@code COALESCE(data, ARRAY[CAST(NULL AS row(...))])} above the
 * join, so an unmatched row unnests to exactly one row whose packed columns all dereference to
 * {@code NULL} — precisely the null-extended row the outer join must emit. {@code array_agg} over a
 * group never returns an empty array, so a {@code NULL} array is the only unmatched signal.
 *
 * <p>The null-supplying side is tried first, since that is where a fan-out build sits in the common
 * {@code a LEFT JOIN (SELECT ... GROUP BY k1, k2) b ON a.k1 = b.k1} shape. FULL outer joins are not
 * collapsed: a side that fans out on both sides of a FULL join is almost always a modelling bug
 * rather than a shape worth optimizing. Cross joins have no equi-criteria to collapse on.
 *
 * <p>This rule is gated behind {@code optimize_join_fan_out} and is disabled by default.
 */
public class CollapseFanoutJoinWithArrayAggUnnest
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();
    private static final String ARRAY_CONSTRUCTOR = "ARRAY";

    // The order in which the two sides are considered for collapse, as values of collapseLeft.
    private static final List<Boolean> RIGHT_SIDE_FIRST = ImmutableList.of(false, true);
    private static final List<Boolean> LEFT_SIDE_FIRST = ImmutableList.of(true, false);

    private final Metadata metadata;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public CollapseFanoutJoinWithArrayAggUnnest(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeJoinFanOut(session);
    }

    @Override
    public Result apply(JoinNode join, Captures captures, Context context)
    {
        JoinType type = join.getType();
        // FULL preserves neither side; cross joins have no equi-criteria to collapse on.
        if (type == JoinType.FULL || join.getCriteria().isEmpty()) {
            return Result.empty();
        }

        // Bail if the join already carries dynamic filters. The rewrite replaces one side of the
        // join (and its output variables) with a collapse aggregation, which would invalidate any
        // dynamic filter referencing that side. This rule is registered ahead of PredicatePushDown
        // (the pass that derives join dynamic filters), so in the normal pipeline the map is empty
        // here; this guard simply preserves any dynamic filters that were attached upstream by
        // declining to fire rather than dropping them.
        if (!join.getDynamicFilters().isEmpty()) {
            return Result.empty();
        }

        // Either side of an INNER, LEFT or RIGHT join can be collapsed. Prefer the build side of an
        // INNER join, and the null-supplying side of an outer join, since that is where a fan-out
        // build normally sits.
        for (boolean collapseLeft : type == JoinType.RIGHT ? LEFT_SIDE_FIRST : RIGHT_SIDE_FIRST) {
            Result result = tryCollapseSide(join, collapseLeft, context);
            if (!result.isEmpty()) {
                return result;
            }
        }
        return Result.empty();
    }

    /**
     * Whether the chosen side supplies nulls, i.e. the join emits a row with all of that side's
     * columns set to {@code NULL} when the other (preserved) side finds no match.
     */
    private static boolean isNullSupplyingSide(JoinType type, boolean collapseLeft)
    {
        return (type == JoinType.LEFT && !collapseLeft) || (type == JoinType.RIGHT && collapseLeft);
    }

    /**
     * Attempts to collapse the chosen side of the join ({@code collapseLeft == true} → left/probe,
     * else right/build). Returns {@link Result#empty()} when the side is not a collapsible fan-out.
     */
    private Result tryCollapseSide(JoinNode join, boolean collapseLeft, Context context)
    {
        PlanNode collapseSideNode = collapseLeft ? join.getLeft() : join.getRight();
        PlanNode otherSideNode = collapseLeft ? join.getRight() : join.getLeft();

        // The fully resolved preserved side. The collapse packs everything this side produces
        // except the outer join keys, regardless of its internal structure (aggregation or join).
        PlanNode resolved = context.getLookup().resolve(collapseSideNode);

        // The side's join keys (the variables this side contributes to the equi-criteria). These
        // are output variables of the resolved side no matter what its internal structure is.
        LinkedHashSet<VariableReferenceExpression> joinKeys = new LinkedHashSet<>();
        for (EquiJoinClause clause : join.getCriteria()) {
            joinKeys.add(collapseLeft ? clause.getLeft() : clause.getRight());
        }

        // Eligibility: the side must fan out over the join keys.
        if (!isFanoutSide(collapseSideNode, joinKeys, context)) {
            return Result.empty();
        }

        // Columns to pack: all of the side's outputs except the join keys.
        List<VariableReferenceExpression> packedColumns = resolved.getOutputVariables().stream()
                .filter(variable -> !joinKeys.contains(variable))
                .collect(toImmutableList());
        if (packedColumns.isEmpty()) {
            return Result.empty();
        }

        // The join filter must not reference any packed column (those become unavailable at the
        // collapsed join); referencing the other side or the join keys is fine.
        if (join.getFilter().isPresent()) {
            Set<VariableReferenceExpression> filterVariables = VariablesExtractor.extractUnique(join.getFilter().get());
            if (filterVariables.stream().anyMatch(packedColumns::contains)) {
                return Result.empty();
            }
        }

        List<VariableReferenceExpression> joinKeyList = ImmutableList.copyOf(joinKeys);

        // 1. Bottom projection over the resolved side: pass the join keys through and pack all the
        // non-key columns into a single row(...) per source row.
        List<Type> fieldTypes = packedColumns.stream().map(VariableReferenceExpression::getType).collect(toImmutableList());
        RowType rowType = RowType.anonymous(fieldTypes);
        RowExpression rowExpression = new SpecialFormExpression(ROW_CONSTRUCTOR, rowType, ImmutableList.copyOf(packedColumns));
        VariableReferenceExpression rowVariable = context.getVariableAllocator().newVariable("row", rowType);

        Assignments.Builder bottomAssignments = Assignments.builder();
        for (VariableReferenceExpression joinKey : joinKeyList) {
            bottomAssignments.put(joinKey, joinKey);
        }
        bottomAssignments.put(rowVariable, rowExpression);
        ProjectNode bottomProject = new ProjectNode(
                resolved.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                resolved,
                bottomAssignments.build(),
                LOCAL);

        // 2. Collapse aggregation: group by the join keys and array_agg the packed row, making the
        // side unique on the join key. Packing the whole row into ONE array (rather than one array
        // per column) makes alignment automatic — each array element is a complete source row — so
        // no ORDER BY or cross-array coordination is needed; the array's order is irrelevant since
        // the join output is a multiset.
        Aggregation arrayAggregation = createArrayAggregation(functionAndTypeManager, rowVariable);
        Type arrayType = arrayAggregation.getCall().getType();
        VariableReferenceExpression arrayVariable = context.getVariableAllocator().newVariable("data", arrayType);
        AggregationNode collapseAggregation = new AggregationNode(
                resolved.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                bottomProject,
                ImmutableMap.of(arrayVariable, arrayAggregation),
                singleGroupingSet(joinKeyList),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // 3. New join with the collapsed side. Output: other-side columns the parent needs, the
        // join keys, and the packed array. Respect the left-before-right output ordering.
        List<VariableReferenceExpression> otherSideOutputs = join.getOutputVariables().stream()
                .filter(otherSideNode.getOutputVariables()::contains)
                .collect(toImmutableList());
        List<VariableReferenceExpression> collapseSideOutputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(joinKeyList)
                .add(arrayVariable)
                .build();

        PlanNode newLeft;
        PlanNode newRight;
        List<VariableReferenceExpression> newJoinOutputs;
        if (collapseLeft) {
            newLeft = collapseAggregation;
            newRight = otherSideNode;
            newJoinOutputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(collapseSideOutputs)
                    .addAll(otherSideOutputs)
                    .build();
        }
        else {
            newLeft = otherSideNode;
            newRight = collapseAggregation;
            newJoinOutputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(otherSideOutputs)
                    .addAll(collapseSideOutputs)
                    .build();
        }

        // Build the rewritten join. Hash variables are intentionally dropped: they reference the
        // original side's output variables, which no longer exist after the side is replaced by the
        // collapse aggregation (its only non-key output is the packed array); they are re-derived by
        // a later optimizer pass. Dynamic filters are propagated through unchanged: apply() bails when
        // the join already carries any, but threading them keeps the rewrite correct if that ever
        // changes (the join type, criteria, and keys are all preserved).
        JoinNode newJoin = new JoinNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                join.getType(),
                newLeft,
                newRight,
                join.getCriteria(),
                newJoinOutputs,
                join.getFilter(),
                Optional.empty(),
                Optional.empty(),
                join.getDistributionType(),
                join.getDynamicFilters());

        // 4. When the collapsed side supplies nulls, an unmatched row of the preserved side carries a
        // NULL array, and CROSS JOIN UNNEST (the only unnest available) would drop that row. Replace
        // the NULL with a single-element array holding a NULL row, so the row survives the unnest with
        // every packed column dereferencing to NULL — exactly the null-extended row the outer join
        // must emit. An array_agg group always has at least one row, so NULL is the only such signal.
        List<VariableReferenceExpression> replicateVariables = newJoinOutputs.stream()
                .filter(variable -> !variable.equals(arrayVariable))
                .collect(toImmutableList());

        PlanNode unnestSource = newJoin;
        VariableReferenceExpression unnestVariable = arrayVariable;
        if (isNullSupplyingSide(join.getType(), collapseLeft)) {
            RowExpression nullRowArray = call(
                    ARRAY_CONSTRUCTOR,
                    functionResolution.arrayConstructor(ImmutableList.of(rowType)),
                    arrayType,
                    constantNull(rowType));
            unnestVariable = context.getVariableAllocator().newVariable("data", arrayType);
            Assignments.Builder coalesceAssignments = Assignments.builder();
            for (VariableReferenceExpression replicateVariable : replicateVariables) {
                coalesceAssignments.put(replicateVariable, replicateVariable);
            }
            coalesceAssignments.put(unnestVariable, new SpecialFormExpression(COALESCE, arrayType, arrayVariable, nullRowArray));
            unnestSource = new ProjectNode(
                    newJoin.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newJoin,
                    coalesceAssignments.build(),
                    LOCAL);
        }

        // 5. Re-expand the packed array(row) locally with UNNEST above the join, emitting the form
        // that matches the session's unnest semantics so it executes correctly under either:
        //   - legacy_unnest:  a single ROW column, with fields recovered via DEREFERENCE (row[i]).
        //   - non-legacy:     one flattened column per row field, mapped directly.
        // The Java engine handles both; the native (Velox) engine currently only supports the legacy
        // single-ROW form for array-of-rows UNNEST (support for the flattened form is planned).
        UnnestNode unnest;
        Assignments.Builder topAssignments = Assignments.builder();
        if (isLegacyUnnest(context.getSession())) {
            VariableReferenceExpression unnestedRow = context.getVariableAllocator().newVariable("row", rowType);
            unnest = new UnnestNode(
                    unnestSource.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    unnestSource,
                    replicateVariables,
                    ImmutableMap.of(unnestVariable, ImmutableList.of(unnestedRow)),
                    Optional.empty());

            // Rebuild each packed column as a DEREFERENCE of the unnested row by its field index
            // (row[i]); pass remaining outputs through. Final output == original join output.
            Map<VariableReferenceExpression, Integer> packedToIndex = new LinkedHashMap<>();
            for (int i = 0; i < packedColumns.size(); i++) {
                packedToIndex.put(packedColumns.get(i), i);
            }
            for (VariableReferenceExpression output : join.getOutputVariables()) {
                Integer fieldIndex = packedToIndex.get(output);
                if (fieldIndex != null) {
                    topAssignments.put(output, new SpecialFormExpression(
                            DEREFERENCE,
                            output.getType(),
                            unnestedRow,
                            constant(fieldIndex.longValue(), INTEGER)));
                }
                else {
                    topAssignments.put(output, output);
                }
            }
        }
        else {
            // Non-legacy: array(row) unnests to one flattened column per row field, in field order.
            // Name each unnested variable after its original column so EXPLAIN/debug plans stay readable.
            Map<VariableReferenceExpression, VariableReferenceExpression> packedToField = new LinkedHashMap<>();
            ImmutableList.Builder<VariableReferenceExpression> unnestedFields = ImmutableList.builder();
            for (VariableReferenceExpression packedColumn : packedColumns) {
                VariableReferenceExpression unnestedField = context.getVariableAllocator().newVariable(packedColumn.getSourceLocation(), packedColumn.getName(), packedColumn.getType());
                packedToField.put(packedColumn, unnestedField);
                unnestedFields.add(unnestedField);
            }
            unnest = new UnnestNode(
                    unnestSource.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    unnestSource,
                    replicateVariables,
                    ImmutableMap.of(unnestVariable, unnestedFields.build()),
                    Optional.empty());
            for (VariableReferenceExpression output : join.getOutputVariables()) {
                topAssignments.put(output, packedToField.getOrDefault(output, output));
            }
        }
        ProjectNode topProject = new ProjectNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                unnest,
                topAssignments.build(),
                LOCAL);

        return Result.ofPlanNode(topProject);
    }

    /**
     * Determines whether the chosen side fans out over the join keys, i.e. whether it can produce
     * more than one row per distinct join key value.
     *
     * <p>The answer comes from the grouping the side reports through {@link #deriveProperties}: a
     * side grouped on a strict superset of the join keys holds several rows per join key. Local
     * properties are hierarchical, so the columns of any prefix together form a grouping and are
     * accumulated in order before comparing; constants are skipped, since a column pinned to a
     * single value adds no grouping.
     */
    private boolean isFanoutSide(PlanNode collapseSideNode, Set<VariableReferenceExpression> joinKeys, Context context)
    {
        // An AggregationNode advertises
        // LocalProperties.grouped(groupingKeys), and ActualProperties carry that up through the
        // projections and filters that routinely sit between the aggregation and the join
        // (PropertyDerivations translates local properties across them). If the side is grouped on a
        // strict superset of the join keys, it holds several rows per join key: a fan-out.
        // The subtree is materialized out of the memo first because PropertyDerivations does not
        // understand GroupReference. This runs only when the rule is enabled (default off).
        //
        // Deliberately NOT the LogicalProperties/constraints framework: that is gated behind
        // exploit_constraints, which is untested at scale, so nothing here may depend on it.
        ActualProperties sideProperties = deriveProperties(
                resolveGroupReferences(collapseSideNode, context.getLookup()),
                context.getSession());

        // Local properties are hierarchical — grouped/sorted by the first, then by the second within
        // it, and so on — so the columns of any prefix together form a grouping. Accumulate them in
        // order: a TOP N over an aggregation, for instance, reports one SortingProperty per sort
        // column, and only their union covers the aggregation's grouping keys. Constants are skipped;
        // a column pinned to a single value adds no grouping.
        LinkedHashSet<VariableReferenceExpression> grouped = new LinkedHashSet<>();
        for (LocalProperty<VariableReferenceExpression> property : sideProperties.getLocalProperties()) {
            if (property instanceof ConstantProperty) {
                continue;
            }
            grouped.addAll(property.getColumns());
            if (grouped.containsAll(joinKeys) && grouped.size() > joinKeys.size()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Derives {@link ActualProperties} for a resolved subtree, tolerating node types the derivation
     * does not cover.
     *
     * <p>{@code PropertyDerivations} is exhaustive only over the node types the physical planner
     * sees, and its {@code visitPlan} throws for everything else — {@link UnionNode} and the CTE
     * nodes among them, because {@code AddExchanges} derives those itself. An iterative rule runs
     * long before that and meets them routinely, so an unsupported node reports unknown properties
     * and derivation continues above it: an aggregation sitting on top of a union is still visible.
     *
     * <p>This must never fail planning. The derivation is advisory — it only decides whether the
     * collapse is worth doing — and {@code IterativeOptimizer} calls {@code apply()} even for a
     * DISABLED rule when {@code verbose_optimizer_info} is on, so an escaping exception would break
     * queries that do not use this optimization at all.
     */
    private ActualProperties deriveProperties(PlanNode node, Session session)
    {
        List<ActualProperties> inputProperties = node.getSources().stream()
                .map(source -> deriveProperties(source, session))
                .collect(toImmutableList());
        try {
            return PropertyDerivations.deriveProperties(node, inputProperties, metadata, session);
        }
        catch (UnsupportedOperationException | VerifyException e) {
            return ActualProperties.builder().build();
        }
    }
}

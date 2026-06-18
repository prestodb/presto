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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
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
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.planner.PlannerUtils.createArrayAggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Collapses a fan-out equi-join whose preserved side resolves (possibly through intervening
 * {@link ProjectNode}/{@link FilterNode}s) to either an {@link AggregationNode} grouped by, or an
 * {@code INNER} {@link JoinNode} keyed on, a strict superset of that side's join keys — turning the
 * {@code 1-to-N} join into a {@code N-to-1} join plus a cheap local {@code UNNEST}.
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
 * <p>The same fan-out arises when the preserved side is itself an {@code INNER} join keyed on a
 * strict superset of the outer join key, e.g. {@code a JOIN (b JOIN c USING (k1, k2)) USING (k1)}:
 * the inner join produces {@code N} rows per {@code k1}, so the outer join on {@code k1} is
 * {@code 1-to-N}. The same {@code array_agg(row(...))} + {@code UNNEST} collapse applies, packing
 * every non-key column the preserved side produces.
 *
 * <p>The collapse transformation is independent of the preserved side's node type: given the
 * resolved side {@code S}, the outer join keys {@code J}, and {@code R = S.outputs − J}, the rewrite
 * always (1) projects {@code row(R...)} and {@code array_agg}s it grouped by {@code J} (now unique on
 * {@code J}), (2) rebuilds the outer join {@code N-to-1}, (3) {@code UNNEST}s the array into one
 * {@code ROW} column, and (4) dereferences that row back into {@code R}. Only the eligibility
 * detection differs per type.
 *
 * <p>The rewrite is losslessly semantics-preserving: {@code array_agg} packs the non-key columns
 * and {@code UNNEST} unpacks them, reproducing the same multiset of rows. The row multiplication
 * moves out of the distributed join (smaller build, unique-key join, less shuffle of duplicated
 * rows) into a streaming local {@code UNNEST}.
 *
 * <p>Because the only available unnest is {@code CROSS JOIN UNNEST} (there is no outer/left
 * unnest), the collapsed side must be the <em>preserved</em> side so that every join output row
 * carries a non-empty array: INNER (either side, build preferred), LEFT (left only), RIGHT (right
 * only). FULL outer and cross joins never fire.
 *
 * <p>This rule is gated behind {@code optimize_join_fan_out} and is disabled by default.
 */
public class CollapseFanoutJoinWithArrayAggUnnest
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    private final FunctionAndTypeManager functionAndTypeManager;

    public CollapseFanoutJoinWithArrayAggUnnest(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
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

        // Try the preserved side(s). For INNER prefer the build (right) side, then probe (left).
        if (type == JoinType.INNER || type == JoinType.RIGHT) {
            Result result = tryCollapseSide(join, false, context);
            if (!result.isEmpty()) {
                return result;
            }
        }
        if (type == JoinType.INNER || type == JoinType.LEFT) {
            Result result = tryCollapseSide(join, true, context);
            if (!result.isEmpty()) {
                return result;
            }
        }
        return Result.empty();
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

        // Eligibility: peer through project/filter down to an aggregation grouped by, or an inner
        // join keyed on, a strict superset of the outer join keys.
        if (!isCollapsibleFanout(resolved, joinKeys, context)) {
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
        VariableReferenceExpression arrayVariable = context.getVariableAllocator().newVariable("data", arrayAggregation.getCall().getType());
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

        // 4. Re-expand the packed array(row) locally with UNNEST above the join, emitting the form
        // that matches the session's unnest semantics so it executes correctly under either:
        //   - legacy_unnest:  a single ROW column, with fields recovered via DEREFERENCE (row[i]).
        //   - non-legacy:     one flattened column per row field, mapped directly.
        // The Java engine handles both; the native (Velox) engine currently only supports the legacy
        // single-ROW form for array-of-rows UNNEST (support for the flattened form is planned).
        List<VariableReferenceExpression> replicateVariables = newJoinOutputs.stream()
                .filter(variable -> !variable.equals(arrayVariable))
                .collect(toImmutableList());

        UnnestNode unnest;
        Assignments.Builder topAssignments = Assignments.builder();
        if (isLegacyUnnest(context.getSession())) {
            VariableReferenceExpression unnestedRow = context.getVariableAllocator().newVariable("row", rowType);
            unnest = new UnnestNode(
                    newJoin.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newJoin,
                    replicateVariables,
                    ImmutableMap.of(arrayVariable, ImmutableList.of(unnestedRow)),
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
                    newJoin.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newJoin,
                    replicateVariables,
                    ImmutableMap.of(arrayVariable, unnestedFields.build()),
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
     * Recursively determines whether {@code node} (already resolved or to-be-resolved) is a
     * collapsible fan-out source relative to {@code trackedKeys} — the set of outer join key
     * variables as they appear at this level of the plan. Peers through {@link ProjectNode}
     * (identity/rename only) and {@link FilterNode}, remapping the tracked keys as needed.
     *
     * <ul>
     *   <li>{@link AggregationNode}: eligible iff its grouping keys are a strict superset of the
     *       tracked keys (with the usual single-step/single-grouping-set guards).
     *   <li>{@code INNER} {@link JoinNode}: eligible iff its equi-key variables cover the tracked
     *       keys and at least one clause introduces an extra key (neither side in the tracked set),
     *       which is the strict-superset / fan-out signal.
     *   <li>{@link ProjectNode}: bail if any tracked key maps to a non-variable expression;
     *       otherwise recurse with the remapped underlying variables.
     *   <li>{@link FilterNode}: recurse with the tracked keys unchanged.
     *   <li>anything else: not collapsible.
     * </ul>
     */
    private boolean isCollapsibleFanout(PlanNode node, Set<VariableReferenceExpression> trackedKeys, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);

        if (resolved instanceof AggregationNode) {
            AggregationNode aggregation = (AggregationNode) resolved;
            // Mirror the structural guards used by other aggregation-rewriting rules.
            if (aggregation.getStep() != AggregationNode.Step.SINGLE
                    || aggregation.getGroupingSetCount() != 1
                    || aggregation.hasEmptyGroupingSet()
                    || aggregation.getGroupingKeys().isEmpty()
                    || aggregation.getGroupIdVariable().isPresent()
                    || aggregation.getHashVariable().isPresent()) {
                return false;
            }
            // The grouping keys must be a STRICT superset of the tracked keys — otherwise the side
            // is already unique on the join key and there is no fan-out to collapse.
            Set<VariableReferenceExpression> groupingKeys = new LinkedHashSet<>(aggregation.getGroupingKeys());
            return groupingKeys.containsAll(trackedKeys) && groupingKeys.size() > trackedKeys.size();
        }

        if (resolved instanceof JoinNode) {
            JoinNode innerJoin = (JoinNode) resolved;
            // Only an INNER join with equi-criteria fans out a superset key deterministically.
            if (innerJoin.getType() != JoinType.INNER || innerJoin.getCriteria().isEmpty()) {
                return false;
            }
            Set<VariableReferenceExpression> keyVariables = new LinkedHashSet<>();
            boolean hasExtraKey = false;
            for (EquiJoinClause clause : innerJoin.getCriteria()) {
                keyVariables.add(clause.getLeft());
                keyVariables.add(clause.getRight());
                if (!trackedKeys.contains(clause.getLeft()) && !trackedKeys.contains(clause.getRight())) {
                    hasExtraKey = true;
                }
            }
            // Cover the tracked keys and introduce at least one extra key (the strict-superset signal).
            return keyVariables.containsAll(trackedKeys) && hasExtraKey;
        }

        if (resolved instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) resolved;
            Assignments assignments = project.getAssignments();
            LinkedHashSet<VariableReferenceExpression> remappedKeys = new LinkedHashSet<>();
            for (VariableReferenceExpression trackedKey : trackedKeys) {
                RowExpression assignment = assignments.getMap().get(trackedKey);
                // A tracked key produced by a non-identity expression (e.g. COALESCE, arithmetic)
                // cannot be traced down — bail conservatively rather than peering through it.
                if (!(assignment instanceof VariableReferenceExpression)) {
                    return false;
                }
                remappedKeys.add((VariableReferenceExpression) assignment);
            }
            return isCollapsibleFanout(project.getSource(), remappedKeys, context);
        }

        if (resolved instanceof FilterNode) {
            // Filters do not rename variables, so the tracked keys carry through unchanged.
            return isCollapsibleFanout(((FilterNode) resolved).getSource(), trackedKeys, context);
        }

        return false;
    }
}

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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isNativeExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.isPushDownWidenCastEnabled;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Pushes widening type casts as close to the fragment's source operator as possible so the
 * source (a {@link TableScanNode} reading from storage, or a {@link RemoteSourceNode} reading
 * from an upstream fragment via Exchange) can produce the wider type directly, avoiding a
 * separate per-row CAST operator.
 *
 * <p>For a leaf fragment the source is a {@link TableScanNode}; the native (Velox) DWIO column
 * reader applies the coercion inline at column-read time. For a non-leaf fragment the source is
 * a {@link RemoteSourceNode}; the rewrite swaps {@code narrowVar} for the wide variable in the
 * RemoteSource's declared outputs while leaving the producer fragment's {@code outputLayout}
 * (the wire format) on the narrow type. The Exchange operator on the consumer side is
 * responsible for emitting wide values from narrow-typed bytes received from the upstream task.
 *
 * <h3>REPLACE-only push</h3>
 *
 * <p>Pattern:
 * <pre>
 *   ProjectNode(... CAST(narrowVar AS T) ...)
 *     &lt;zero or more transparent intermediate nodes&gt;
 *       TableScanNode(narrowVar -&gt; columnHandle, ...)         // leaf fragment
 *   or
 *       RemoteSourceNode(... narrowVar ...)                       // non-leaf fragment
 * </pre>
 * The cast is safe to push only when {@code narrowVar} has no other uses anywhere on the descent
 * path. We swap {@code narrowVar} for a wide variable in the source node and rewrite every
 * matching {@code CAST(narrowVar AS T)} subexpression in the top Project to a bare reference to
 * that wide variable. Bare-RHS assignments collapse to identities ({@code wide := wide}) and are
 * cleaned up by {@link com.facebook.presto.sql.planner.iterative.rule.InlineProjections}.
 *
 * <h3>Transparent intermediate nodes</h3>
 *
 * The descent walks through these on its way to a source operator:
 * <ul>
 *   <li>{@link FilterNode} — skipped if the predicate references {@code narrowVar}.</li>
 *   <li>{@link SortNode}, {@link TopNNode} — skipped if {@code narrowVar} is in the ordering
 *       scheme (changing the variable's type would change collation semantics).</li>
 *   <li>{@link LimitNode} — always transparent.</li>
 *   <li>Intermediate {@link ProjectNode} — must pass {@code narrowVar} through as an identity
 *       assignment and not reference it elsewhere; otherwise the descent bails.</li>
 *   <li>{@link JoinNode} — skipped if {@code narrowVar} is in any join clause / filter / hash /
 *       dynamic-filter variable.</li>
 *   <li>{@link ExchangeNode} with {@code scope = LOCAL} — the post-fragmentation LocalExchange
 *       the planner inserts between Project and RemoteSource. Skipped if {@code narrowVar}
 *       participates in partitioning / hash / ordering, or if the exchange has more than one
 *       source (e.g. union of local pipelines). REMOTE_STREAMING exchanges are not handled —
 *       PlanFragmenter has already split them into RemoteSourceNode pairs.</li>
 * </ul>
 *
 * <p>If the descent bails, the cast simply stays where it was — REPLACE never adds new plan
 * nodes. The companion ADD path that handles "{@code narrowVar} used elsewhere" by inserting a
 * synthetic Project above the scan lives behind {@code push_down_widen_cast_add_enabled}; it is
 * not part of this commit.
 *
 * <h3>Supported widening type pairs</h3>
 * <ul>
 *   <li>TINYINT → SMALLINT, INTEGER, BIGINT</li>
 *   <li>SMALLINT → INTEGER, BIGINT</li>
 *   <li>INTEGER → BIGINT</li>
 *   <li>REAL → DOUBLE</li>
 *   <li>DATE → TIMESTAMP</li>
 * </ul>
 *
 * <h3>Session gating</h3>
 *
 * Requires <em>both</em> {@code push_down_widen_cast_enabled=true} AND
 * {@code native_execution_enabled=true}. The Java scan can't do inline widening, so pushing
 * for non-native sessions would just move work around without saving anything.
 *
 * <h3>EXPLAIN vs runtime divergence</h3>
 *
 * Because this rule runs at fragmentation time (inside
 * {@link com.facebook.presto.sql.planner.PlanFragmenterUtils#finalizeSubPlan}), the
 * pre-fragmentation logical plan is not rewritten. That gives the following observability gap:
 * <ul>
 *   <li>{@code EXPLAIN <query>} — default plan type is
 *       {@link com.facebook.presto.sql.tree.ExplainType.Type#LOGICAL} (see
 *       {@code ExplainRewrite.java}), i.e. <b>pre-fragmentation</b>. The CAST Project is still
 *       visible.</li>
 *   <li>{@code EXPLAIN (TYPE DISTRIBUTED) <query>} — post-fragmentation. Rule has run, CAST is
 *       gone from the Project (folded into the scan / RemoteSource).</li>
 *   <li>{@code EXPLAIN ANALYZE <query>} — renders the executed (post-fragmentation) plan, so the
 *       CAST is gone here too.</li>
 * </ul>
 * In other words, a user who compares {@code EXPLAIN q} against {@code EXPLAIN ANALYZE q} for the
 * same query will see the CAST appear in the former and disappear in the latter. Running the
 * rule at fragmentation time is intentional — it bounds the {@code narrowVar → wideVar}
 * substitution to a single fragment so the producer's {@code PartitioningScheme.outputLayout}
 * (the wire format) stays narrow.
 */
public class PushDownWidenCast
        implements PlanOptimizer
{
    private final FunctionResolution functionResolution;
    private boolean isEnabledForTesting;

    public PushDownWidenCast(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || (isPushDownWidenCastEnabled(session) && isNativeExecutionEnabled(session));
    }

    @Override
    public PlanOptimizerResult optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");

        if (!isEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        Rewriter rewriter = new Rewriter(functionResolution, variableAllocator);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    /**
     * Walks the plan bottom-up; on each {@link ProjectNode} runs two passes in sequence:
     * <ol>
     *   <li>{@link #visitProject} → REPLACE pass: handle bare {@code wideVar := CAST(narrow AS T)}
     *       assignments by swapping {@code narrow} for {@code wideVar} all the way down to the
     *       {@link TableScanNode} via {@link #tryPushWidening}.</li>
     *   <li>{@link #applySubexpressionCastPush} → SUBEXPRESSION pass: handle nested
     *       {@code CAST(narrow AS T)} inside larger expressions. Only fires when {@code narrow}
     *       is used solely inside CAST subexpressions in this Project — the REPLACE substitution
     *       all the way to the scan would otherwise orphan references to {@code narrow}.</li>
     * </ol>
     */
    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        // (fromType -> the set of types we'll widen TO). Anything outside this map is left alone.
        private static final Map<Type, Set<Type>> WIDENING_CAST_MAP = ImmutableMap.<Type, Set<Type>>builder()
                .put(TinyintType.TINYINT, ImmutableSet.of(SmallintType.SMALLINT, IntegerType.INTEGER, BigintType.BIGINT))
                .put(SmallintType.SMALLINT, ImmutableSet.of(IntegerType.INTEGER, BigintType.BIGINT))
                .put(IntegerType.INTEGER, ImmutableSet.of(BigintType.BIGINT))
                .put(RealType.REAL, ImmutableSet.of(DoubleType.DOUBLE))
                .put(DateType.DATE, ImmutableSet.of(TimestampType.TIMESTAMP))
                .build();

        private final FunctionResolution functionResolution;
        private final VariableAllocator variableAllocator;
        private boolean planChanged;

        public Rewriter(FunctionResolution functionResolution, VariableAllocator variableAllocator)
        {
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            // variableAllocator is used by the subexpression Phase to mint fresh wideVar names
            // that won't collide with existing variables anywhere in the plan.
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            // Bottom-up: first let the visitor descend into children so nested Projects are
            // rewritten before we process this one. This means by the time we see `node`, every
            // ProjectNode below it has already been visited (and possibly transformed).
            PlanNode rewrittenNode = context.defaultRewrite(node, null);
            if (!(rewrittenNode instanceof ProjectNode)) {
                return rewrittenNode;
            }
            ProjectNode projectNode = (ProjectNode) rewrittenNode;

            // ----- Phase 1: REPLACE pass for bare-RHS CAST assignments -----
            // Looking for entries like `wideVar := CAST(narrowVar AS T)` where the WHOLE RHS is the cast.
            Set<VariableReferenceExpression> sourceOutputSet =
                    ImmutableSet.copyOf(projectNode.getSource().getOutputVariables());
            Map<VariableReferenceExpression, VariableReferenceExpression> candidates =
                    collectAllWideningCasts(projectNode, sourceOutputSet);

            // Thread `currentSource` forward across multiple candidates: each successful push
            // returns a rewritten subtree which becomes the input for the next candidate's push.
            PlanNode currentSource = projectNode.getSource();
            Map<VariableReferenceExpression, VariableReferenceExpression> pushed = new LinkedHashMap<>();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : candidates.entrySet()) {
                VariableReferenceExpression narrowVar = entry.getKey();
                VariableReferenceExpression wideVar = entry.getValue();

                // REPLACE removes narrowVar from the scan entirely. That's only safe if narrowVar
                // is not referenced anywhere else in this Project — otherwise the other references
                // would dangle. (Phase 2 below handles those harder cases via ADD instead.)
                if (isVariableUsedElsewhere(narrowVar, projectNode)) {
                    continue;
                }
                Optional<PlanNode> result = tryPushWidening(currentSource, narrowVar, wideVar);
                if (result.isPresent()) {
                    currentSource = result.get();
                    pushed.put(narrowVar, wideVar);
                }
            }
            // If REPLACE landed anywhere, build the new top Project whose CAST assignments collapse
            // to identities (`wideVar := wideVar`); InlineProjections later cleans them up.
            ProjectNode afterReplace = pushed.isEmpty()
                    ? projectNode
                    : buildWideProject(projectNode, currentSource, pushed);

            // ----- Phase 2: subexpression-CAST pass -----
            // Handles CASTs nested inside larger expressions like `f(CAST(col AS T), ...)`. Allowed
            // to use ADD semantics when narrow has other uses (REPLACE would dangle in that case).
            return applySubexpressionCastPush(afterReplace);
        }

        // =======================================================================
        // Phase 2: subexpression CAST handling
        // =======================================================================

        /**
         * Handles every widening CAST {@code CAST(narrowVar AS T)} that appears as a nested
         * subexpression in this Project's assignments (e.g. {@code abs(CAST(narrow AS BIGINT))}).
         * Only fires when {@code narrow} is used solely inside CAST subexpressions in this
         * Project; if {@code narrow} appears anywhere else (as a passthrough output, in another
         * expression, etc.), REPLACE would orphan those references and the candidate is skipped.
         *
         * <p>If Phase 1 already collapsed an assignment to {@code wide := wide} (identity), that
         * assignment no longer contains a CAST and isn't matched here.
         */
        private ProjectNode applySubexpressionCastPush(ProjectNode projectNode)
        {
            Set<VariableReferenceExpression> sourceOutputs =
                    ImmutableSet.copyOf(projectNode.getSource().getOutputVariables());

            // Step 1: discover candidates. Each unique narrowVar gets one freshly-allocated wideVar
            // (reused if the same CAST(narrow AS T) appears in multiple assignments).
            Map<VariableReferenceExpression, VariableReferenceExpression> candidates = new LinkedHashMap<>();
            for (RowExpression expr : projectNode.getAssignments().getExpressions()) {
                collectSubexpressionWideningCasts(expr, sourceOutputs, candidates);
            }
            if (candidates.isEmpty()) {
                return projectNode;
            }

            // Step 2: REPLACE each candidate whose narrowVar isn't used outside the CAST. Any
            // candidate that fails the safety check, or whose descent bails because an
            // intermediate node pins narrow, is skipped — the cast simply stays in place.
            PlanNode currentSource = projectNode.getSource();
            Map<VariableReferenceExpression, VariableReferenceExpression> pushed = new LinkedHashMap<>();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : candidates.entrySet()) {
                VariableReferenceExpression narrow = entry.getKey();
                VariableReferenceExpression wide = entry.getValue();
                if (narrowVarUsedOutsideTargetCasts(projectNode, narrow)) {
                    continue;
                }
                Optional<PlanNode> result = tryPushWidening(currentSource, narrow, wide);
                if (result.isPresent()) {
                    currentSource = result.get();
                    pushed.put(narrow, wide);
                }
            }
            if (pushed.isEmpty()) {
                return projectNode;
            }

            // Step 3: rewrite this Project's assignments — every CAST(narrow AS T) subexpression
            // becomes a bare wideVar reference. The cast itself moves down into either the scan
            // (REPLACE) or the synthetic Project above it (ADD).
            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
                newAssignments.put(entry.getKey(), rewriteSubexpressionCasts(entry.getValue(), pushed));
            }

            planChanged = true;
            return new ProjectNode(
                    projectNode.getSourceLocation(),
                    projectNode.getId(),
                    projectNode.getStatsEquivalentPlanNode(),
                    currentSource,
                    newAssignments.build(),
                    projectNode.getLocality());
        }

        /**
         * Decides whether {@code narrow} can be removed from the scan (REPLACE) or must be
         * preserved (ADD). Returns true if {@code narrow} is referenced anywhere in
         * {@code projectNode}'s assignments outside of {@code CAST(narrow AS T)} subexpressions,
         * or appears as an output variable (i.e. upstream needs it).
         *
         * <p>The visitor walks each assignment but skips the bare-variable argument of any
         * {@code CAST(narrow AS T)} call — those are the references we're about to rewrite away,
         * so they don't count as "other uses".
         */
        private boolean narrowVarUsedOutsideTargetCasts(ProjectNode projectNode, VariableReferenceExpression narrow)
        {
            // narrow is itself an output of this Project → some upstream node references it
            if (projectNode.getAssignments().getVariables().contains(narrow)) {
                return true;
            }
            boolean[] foundOutside = {false};
            DefaultRowExpressionTraversalVisitor<Void> visitor = new DefaultRowExpressionTraversalVisitor<Void>()
            {
                @Override
                public Void visitVariableReference(VariableReferenceExpression reference, Void context)
                {
                    if (reference.equals(narrow)) {
                        foundOutside[0] = true;
                    }
                    return null;
                }

                @Override
                public Void visitCall(CallExpression call, Void context)
                {
                    // CAST(narrow AS T) is a "safe" reference — the rewrite will replace it with
                    // wideVar, so the narrow usage inside this call doesn't count. Don't descend.
                    if (isWideningCast(call) && call.getArguments().get(0) instanceof VariableReferenceExpression
                            && call.getArguments().get(0).equals(narrow)) {
                        return null;
                    }
                    // Any other call: descend normally to find references to narrow inside it.
                    return super.visitCall(call, context);
                }
            };
            for (RowExpression expr : projectNode.getAssignments().getExpressions()) {
                expr.accept(visitor, null);
                if (foundOutside[0]) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Walks {@code expr} and records each widening CAST {@code CAST(narrowVar AS T)} whose
         * argument is a bare variable from the source. A fresh {@code wideVar} is allocated for
         * each unique {@code narrowVar} the first time it is seen; subsequent encounters reuse
         * the same wideVar.
         *
         * <p><b>Scoped to nested CASTs only.</b> A widening CAST that sits at the very top of an
         * assignment RHS is Phase 1's territory — collecting it here would mint a fresh wideVar
         * via the variable allocator even when the push ultimately can't fire (e.g. an Aggregation
         * blocks the descent), and that wideVar leaks into the fragment's variable set via
         * {@code variableAllocatorVariables(variableAllocator)}. Once a wideVar with the same
         * name root as a fragment-output variable lives in the allocator, downstream code that
         * canonicalizes variables by name (e.g. the {@code PartitioningScheme.outputLayout}
         * propagation in {@code PlanFragment.withRoot}) widens that fragment-output's declared
         * type — wire-format widening that breaks the per-fragment guarantee. Keep top-level
         * CASTs out of Phase 2 to avoid that side-effect.
         */
        private void collectSubexpressionWideningCasts(
                RowExpression expr,
                Set<VariableReferenceExpression> availableSourceVars,
                Map<VariableReferenceExpression, VariableReferenceExpression> narrowToWide)
        {
            DefaultRowExpressionTraversalVisitor<Void> visitor = new DefaultRowExpressionTraversalVisitor<Void>()
            {
                @Override
                public Void visitCall(CallExpression call, Void context)
                {
                    if (isWideningCast(call) && call.getArguments().get(0) instanceof VariableReferenceExpression) {
                        VariableReferenceExpression narrow = (VariableReferenceExpression) call.getArguments().get(0);
                        if (availableSourceVars.contains(narrow) && !narrowToWide.containsKey(narrow)) {
                            VariableReferenceExpression wide = variableAllocator.newVariable(
                                    narrow.getSourceLocation(), narrow.getName(), call.getType());
                            narrowToWide.put(narrow, wide);
                        }
                        return null;
                    }
                    return super.visitCall(call, context);
                }
            };
            // Walk the IMMEDIATE CHILDREN of `expr`, not `expr` itself — this excludes a top-level
            // widening CAST from being picked up here. Phase 1 owns that case; see the note above.
            if (expr instanceof CallExpression) {
                for (RowExpression arg : ((CallExpression) expr).getArguments()) {
                    arg.accept(visitor, null);
                }
            }
            else if (expr instanceof com.facebook.presto.spi.relation.SpecialFormExpression) {
                for (RowExpression arg : ((com.facebook.presto.spi.relation.SpecialFormExpression) expr).getArguments()) {
                    arg.accept(visitor, null);
                }
            }
            // VariableReference / Constant / Lambda: no descendants we care about.
        }

        /**
         * Returns {@code expr} with every {@code CAST(narrowVar AS T)} subexpression replaced by
         * {@code wideVar}, where {@code narrowVar -> wideVar} is in {@code narrowToWide}. Uses
         * {@link RowExpressionTreeRewriter} so unrelated subexpressions are returned by identity
         * (returning {@code null} from {@code rewriteCall} delegates to the tree rewriter's
         * default behavior, which recurses into arguments).
         */
        private RowExpression rewriteSubexpressionCasts(
                RowExpression expr,
                Map<VariableReferenceExpression, VariableReferenceExpression> narrowToWide)
        {
            return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Void>()
            {
                @Override
                public RowExpression rewriteCall(CallExpression node, Void ctx, RowExpressionTreeRewriter<Void> tr)
                {
                    if (isWideningCast(node) && node.getArguments().get(0) instanceof VariableReferenceExpression) {
                        VariableReferenceExpression narrow = (VariableReferenceExpression) node.getArguments().get(0);
                        VariableReferenceExpression wide = narrowToWide.get(narrow);
                        // The type check guards against rewriting a CAST whose target type doesn't
                        // match the allocated wideVar (shouldn't happen given how candidates are
                        // collected, but cheap to be defensive).
                        if (wide != null && wide.getType().equals(node.getType())) {
                            return wide;
                        }
                    }
                    return null;
                }
            }, expr);
        }

        // =======================================================================
        // Phase 1: REPLACE push — substitute narrowVar with wideVar throughout the subtree
        // =======================================================================

        /**
         * Recursive descent that pushes the substitution {@code narrowVar → wideVar} all the way
         * down to the {@link TableScanNode} that produces {@code narrowVar}. On success the scan
         * is rebuilt with {@code narrowVar} swapped for {@code wideVar} (same {@code ColumnHandle},
         * different declared type), and every intermediate node is rebuilt to reflect the
         * substitution in its outputs. Returns {@code Optional.empty()} if any node on the path
         * can't safely accept the substitution.
         *
         * <p><b>Per-node-type guards (only here, not in {@link #tryPushAddWidening}):</b>
         * <ul>
         *   <li>{@code FilterNode}: bail if {@code narrowVar} appears in the predicate — after
         *       REPLACE, the predicate would reference a variable the scan no longer produces.</li>
         *   <li>{@code SortNode}/{@code TopNNode}: bail if {@code narrowVar} is an ordering key —
         *       sorting by a type-changed variable would change collation semantics.</li>
         *   <li>{@code LimitNode}: always transparent — narrowVar isn't referenced inside.</li>
         *   <li>{@code JoinNode}: see {@link #tryPushThroughJoin} — bail if narrowVar is pinned by
         *       any join clause / filter / hash / dynamic-filter variable.</li>
         *   <li>{@code ProjectNode}: see {@link #tryPushThroughIntermediateProject} — bail unless
         *       narrowVar is an identity passthrough and not referenced elsewhere in the project.</li>
         * </ul>
         */
        private Optional<PlanNode> tryPushWidening(
                PlanNode subtree,
                VariableReferenceExpression narrowVar,
                VariableReferenceExpression wideVar)
        {
            if (subtree instanceof TableScanNode) {
                TableScanNode scan = (TableScanNode) subtree;
                if (!scan.getOutputVariables().contains(narrowVar)) {
                    return Optional.empty();
                }
                // Rebuild the scan: narrowVar key swapped for wideVar, ColumnHandle reused.
                return Optional.of(buildReplaceScan(scan, ImmutableMap.of(narrowVar, wideVar)));
            }

            if (subtree instanceof RemoteSourceNode) {
                RemoteSourceNode remote = (RemoteSourceNode) subtree;
                if (!remote.getOutputVariables().contains(narrowVar)) {
                    return Optional.empty();
                }
                // narrowVar is an ordering key of a merging exchange → swapping its type would
                // change the merge collation.
                if (remote.getOrderingScheme().isPresent()
                        && remote.getOrderingScheme().get().getOrderByVariables().contains(narrowVar)) {
                    return Optional.empty();
                }
                // Swap narrowVar for wideVar in the declared outputs (positional). The producer
                // fragment's outputLayout stays on the narrow type — the Exchange operator on the
                // consumer side widens narrow→wide as it emits pages.
                return Optional.of(buildReplaceRemoteSource(remote, narrowVar, wideVar));
            }

            if (subtree instanceof ExchangeNode) {
                ExchangeNode exchange = (ExchangeNode) subtree;
                // REMOTE_STREAMING exchanges have already been split into RemoteSourceNode by
                // PlanFragmenter; we only step through LOCAL exchanges (intra-fragment routing,
                // e.g. the LocalExchange the planner inserts between Project and RemoteSource).
                if (exchange.getScope() != ExchangeNode.Scope.LOCAL) {
                    return Optional.empty();
                }
                return tryPushThroughLocalExchange(exchange, narrowVar, wideVar);
            }

            if (subtree instanceof FilterNode) {
                FilterNode filter = (FilterNode) subtree;
                // Predicate uses narrowVar → can't REPLACE; the predicate's reference would dangle.
                if (containsVariable(narrowVar, filter.getPredicate())) {
                    return Optional.empty();
                }
                return tryPushWidening(filter.getSource(), narrowVar, wideVar)
                        .map(newSource -> filter.replaceChildren(ImmutableList.of(newSource)));
            }

            if (subtree instanceof SortNode) {
                SortNode sort = (SortNode) subtree;
                // narrowVar is an ordering key → swapping its type changes sort behavior.
                if (sort.getOrderingScheme().getOrderByVariables().contains(narrowVar)) {
                    return Optional.empty();
                }
                return tryPushWidening(sort.getSource(), narrowVar, wideVar)
                        .map(newSource -> sort.replaceChildren(ImmutableList.of(newSource)));
            }

            if (subtree instanceof TopNNode) {
                TopNNode topN = (TopNNode) subtree;
                if (topN.getOrderingScheme().getOrderByVariables().contains(narrowVar)) {
                    return Optional.empty();
                }
                return tryPushWidening(topN.getSource(), narrowVar, wideVar)
                        .map(newSource -> topN.replaceChildren(ImmutableList.of(newSource)));
            }

            if (subtree instanceof LimitNode) {
                // Limit is transparent — it doesn't reference any variable internally.
                LimitNode limit = (LimitNode) subtree;
                return tryPushWidening(limit.getSource(), narrowVar, wideVar)
                        .map(newSource -> limit.replaceChildren(ImmutableList.of(newSource)));
            }

            if (subtree instanceof JoinNode) {
                return tryPushThroughJoin((JoinNode) subtree, narrowVar, wideVar);
            }

            if (subtree instanceof ProjectNode) {
                return tryPushThroughIntermediateProject((ProjectNode) subtree, narrowVar, wideVar);
            }

            // Aggregations, window functions, semijoins, etc. — we don't know how to step through.
            return Optional.empty();
        }

        /**
         * REPLACE-style descent through an intermediate ProjectNode. For the descent to succeed
         * the intermediate Project must (a) pass narrowVar through as an identity assignment
         * (so we know what to rewrite) and (b) not reference narrowVar in any other assignment
         * (else REPLACE in the scan would orphan those references). Rewrites the identity
         * assignment {@code narrowVar := narrowVar} to {@code wideVar := wideVar}; other
         * assignments are untouched.
         */
        private Optional<PlanNode> tryPushThroughIntermediateProject(
                ProjectNode project,
                VariableReferenceExpression narrowVar,
                VariableReferenceExpression wideVar)
        {
            // Must be an identity passthrough; otherwise we can't follow it down.
            RowExpression narrowAssignment = project.getAssignments().get(narrowVar);
            if (narrowAssignment == null || !narrowAssignment.equals(narrowVar)) {
                return Optional.empty();
            }
            // narrow referenced in any other assignment → REPLACE would dangle.
            if (isVariableUsedElsewhere(narrowVar, project)) {
                return Optional.empty();
            }

            Optional<PlanNode> newSourceOpt = tryPushWidening(project.getSource(), narrowVar, wideVar);
            if (!newSourceOpt.isPresent()) {
                return Optional.empty();
            }

            // Rewrite the single narrow→narrow identity to wide→wide; copy everything else verbatim.
            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> e : project.getAssignments().entrySet()) {
                if (e.getKey().equals(narrowVar)) {
                    newAssignments.put(wideVar, wideVar);
                }
                else {
                    newAssignments.put(e);
                }
            }

            return Optional.of(new ProjectNode(
                    project.getSourceLocation(),
                    project.getId(),
                    project.getStatsEquivalentPlanNode(),
                    newSourceOpt.get(),
                    newAssignments.build(),
                    project.getLocality()));
        }

        /**
         * REPLACE-style descent through a JoinNode. Bails if narrowVar is "pinned" by the join —
         * i.e. referenced by any clause / filter / hash / dynamic-filter variable — because the
         * replacement would change the type of a variable the join semantics depend on.
         * Otherwise recurses into the single side that produces narrowVar and rebuilds the join
         * with narrowVar swapped for wideVar in its declared outputs.
         */
        private Optional<PlanNode> tryPushThroughJoin(
                JoinNode joinNode,
                VariableReferenceExpression narrowVar,
                VariableReferenceExpression wideVar)
        {
            Set<VariableReferenceExpression> joinPinnedVars = collectJoinPinnedVariables(joinNode);
            if (joinPinnedVars.contains(narrowVar)) {
                return Optional.empty();
            }

            // narrowVar must come from exactly one side (joins don't produce the same variable on
            // both sides). Recurse into that side only.
            boolean fromLeft = joinNode.getLeft().getOutputVariables().contains(narrowVar);
            boolean fromRight = joinNode.getRight().getOutputVariables().contains(narrowVar);
            if (!fromLeft && !fromRight) {
                return Optional.empty();
            }

            PlanNode newLeft = joinNode.getLeft();
            PlanNode newRight = joinNode.getRight();
            if (fromLeft) {
                Optional<PlanNode> newLeftOpt = tryPushWidening(joinNode.getLeft(), narrowVar, wideVar);
                if (!newLeftOpt.isPresent()) {
                    return Optional.empty();
                }
                newLeft = newLeftOpt.get();
            }
            else {
                Optional<PlanNode> newRightOpt = tryPushWidening(joinNode.getRight(), narrowVar, wideVar);
                if (!newRightOpt.isPresent()) {
                    return Optional.empty();
                }
                newRight = newRightOpt.get();
            }

            // REPLACE simply swaps narrowVar for wideVar in-place in the output list — that keeps
            // left-before-right ordering automatically since we're not adding a new variable.
            List<VariableReferenceExpression> newJoinOutputs = joinNode.getOutputVariables().stream()
                    .map(v -> v.equals(narrowVar) ? wideVar : v)
                    .collect(toImmutableList());

            return Optional.of(new JoinNode(
                    joinNode.getSourceLocation(),
                    joinNode.getId(),
                    joinNode.getStatsEquivalentPlanNode(),
                    joinNode.getType(),
                    newLeft,
                    newRight,
                    joinNode.getCriteria(),
                    newJoinOutputs,
                    joinNode.getFilter(),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType(),
                    joinNode.getDynamicFilters()));
        }

        // =======================================================================
        // Shared helpers
        // =======================================================================

        /**
         * Returns every variable that the join "pins" — i.e. depends on by name AND type — so we
         * know not to REPLACE any of them. Pinned variables are: equi-join clause sides, every
         * variable referenced in the (optional) join filter, dynamic-filter probe variables, and
         * the optional hash variables on either side.
         */
        private static Set<VariableReferenceExpression> collectJoinPinnedVariables(JoinNode joinNode)
        {
            Set<VariableReferenceExpression> pinned = new HashSet<>();

            for (EquiJoinClause clause : joinNode.getCriteria()) {
                pinned.add(clause.getLeft());
                pinned.add(clause.getRight());
            }

            joinNode.getFilter().ifPresent(filter ->
                    filter.accept(new DefaultRowExpressionTraversalVisitor<Void>()
                    {
                        @Override
                        public Void visitVariableReference(VariableReferenceExpression reference, Void context)
                        {
                            pinned.add(reference);
                            return null;
                        }
                    }, null));

            pinned.addAll(joinNode.getDynamicFilters().values());
            joinNode.getLeftHashVariable().ifPresent(pinned::add);
            joinNode.getRightHashVariable().ifPresent(pinned::add);

            return pinned;
        }

        /**
         * Collects whole-RHS widening-cast candidates from {@code projectNode}: every assignment
         * of the form {@code wideVar := CAST(narrowVar AS T)} where {@code narrowVar} is a
         * variable available in the source and {@code (narrow.type → T)} is a supported widening
         * pair. The "narrow used elsewhere" safety check happens later in {@link #visitProject},
         * not here — this method just enumerates candidates.
         */
        private Map<VariableReferenceExpression, VariableReferenceExpression> collectAllWideningCasts(
                ProjectNode projectNode,
                Set<VariableReferenceExpression> availableSourceVars)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> narrowToWide = new LinkedHashMap<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
                VariableReferenceExpression wideVar = entry.getKey();
                RowExpression expr = entry.getValue();

                if (!isWideningCast(expr)) {
                    continue;
                }
                RowExpression castInput = ((CallExpression) expr).getArguments().get(0);
                if (!(castInput instanceof VariableReferenceExpression)) {
                    // Only handle the simple case where the cast's input is a bare variable.
                    // Things like CAST(foo(col) AS T) are out of scope.
                    continue;
                }
                VariableReferenceExpression narrowVar = (VariableReferenceExpression) castInput;
                if (!availableSourceVars.contains(narrowVar)) {
                    continue;
                }
                narrowToWide.put(narrowVar, wideVar);
            }
            return narrowToWide;
        }

        /**
         * Rebuilds a {@link TableScanNode} with each narrow variable in {@code narrowToWide}
         * swapped for its wide counterpart. The {@link ColumnHandle} stays the same — only the
         * variable identity (name + declared type) changes. The native scan reads the column
         * from disk in its narrow type and produces the wider variable value inline.
         */
        private static TableScanNode buildReplaceScan(
                TableScanNode tableScan,
                Map<VariableReferenceExpression, VariableReferenceExpression> narrowToWide)
        {
            List<VariableReferenceExpression> newOutputVariables = tableScan.getOutputVariables().stream()
                    .map(v -> narrowToWide.getOrDefault(v, v))
                    .collect(toImmutableList());

            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> newAssignments = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : tableScan.getAssignments().entrySet()) {
                VariableReferenceExpression narrowVar = entry.getKey();
                if (narrowToWide.containsKey(narrowVar)) {
                    // Re-key the (narrow → handle) entry under wideVar — handle (the on-disk
                    // column) is unchanged.
                    newAssignments.put(narrowToWide.get(narrowVar), entry.getValue());
                }
                else {
                    newAssignments.put(entry);
                }
            }

            return new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    tableScan.getTable(),
                    newOutputVariables,
                    newAssignments.build(),
                    tableScan.getTableConstraints(),
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());
        }

        /**
         * REPLACE-style descent through a {@link ExchangeNode.Scope#LOCAL} exchange. The local
         * exchange just routes data between local pipelines (single source in the common case);
         * it preserves types. Bails on multi-source local exchanges (e.g. union sources) and on
         * cases where {@code narrowVar} participates in partitioning / hash / ordering — changing
         * its type would change the routing semantics. Recursively pushes the widening into the
         * single source and rebuilds the exchange with {@code narrowVar} swapped for
         * {@code wideVar} positionally in {@code partitioningScheme.outputLayout} and in
         * {@code inputs[0]}.
         */
        private Optional<PlanNode> tryPushThroughLocalExchange(
                ExchangeNode exchange,
                VariableReferenceExpression narrowVar,
                VariableReferenceExpression wideVar)
        {
            // Multi-source case (e.g., union of local pipelines) would require pushing into every
            // source — possible in principle but not needed for the LocalExchange→RemoteSource
            // pattern that motivates this case. Bail.
            if (exchange.getSources().size() != 1) {
                return Optional.empty();
            }
            PartitioningScheme scheme = exchange.getPartitioningScheme();
            List<VariableReferenceExpression> outputs = scheme.getOutputLayout();
            int p = outputs.indexOf(narrowVar);
            if (p < 0) {
                return Optional.empty();
            }
            // narrowVar drives partitioning / hashing / ordering — changing its type would
            // change the routing or sort key.
            if (scheme.getPartitioning().getVariableReferences().contains(narrowVar)) {
                return Optional.empty();
            }
            if (scheme.getHashColumn().isPresent() && scheme.getHashColumn().get().equals(narrowVar)) {
                return Optional.empty();
            }
            if (exchange.getOrderingScheme().isPresent()
                    && exchange.getOrderingScheme().get().getOrderByVariables().contains(narrowVar)) {
                return Optional.empty();
            }

            // Push into the single source. Use the source-side variable at position p — usually
            // equal to narrowVar (LocalExchange is renaming-free), but pulled positionally to
            // be safe.
            VariableReferenceExpression sourceNarrowVar = exchange.getInputs().get(0).get(p);
            Optional<PlanNode> newSource = tryPushWidening(exchange.getSources().get(0), sourceNarrowVar, wideVar);
            if (!newSource.isPresent()) {
                return Optional.empty();
            }

            // Rebuild: narrowVar → wideVar at position p in both outputs and inputs[0].
            List<VariableReferenceExpression> newOutputs = new java.util.ArrayList<>(outputs);
            newOutputs.set(p, wideVar);
            List<VariableReferenceExpression> newInput0 = new java.util.ArrayList<>(exchange.getInputs().get(0));
            newInput0.set(p, wideVar);
            PartitioningScheme newScheme = new PartitioningScheme(
                    scheme.getPartitioning(),
                    ImmutableList.copyOf(newOutputs),
                    scheme.getHashColumn(),
                    scheme.isReplicateNullsAndAny(),
                    scheme.isScaleWriters(),
                    scheme.getEncoding(),
                    scheme.getBucketToPartition());
            return Optional.of(new ExchangeNode(
                    exchange.getSourceLocation(),
                    exchange.getId(),
                    exchange.getStatsEquivalentPlanNode(),
                    exchange.getType(),
                    exchange.getScope(),
                    newScheme,
                    ImmutableList.of(newSource.get()),
                    ImmutableList.of(ImmutableList.copyOf(newInput0)),
                    exchange.isEnsureSourceOrdering(),
                    exchange.getOrderingScheme()));
        }

        /**
         * Rebuilds a {@link RemoteSourceNode} with {@code narrowVar} replaced by {@code wideVar}
         * in the declared outputs. All other RemoteSource state (source fragment ids, ordering
         * scheme, exchange type, encoding, transport type) is preserved — the producer fragment's
         * {@code outputLayout} (the wire format) stays on the narrow type. The Exchange operator
         * on the consumer side widens narrow→wide as it emits pages.
         */
        private static RemoteSourceNode buildReplaceRemoteSource(
                RemoteSourceNode remote,
                VariableReferenceExpression narrowVar,
                VariableReferenceExpression wideVar)
        {
            List<VariableReferenceExpression> newOutputs = remote.getOutputVariables().stream()
                    .map(v -> v.equals(narrowVar) ? wideVar : v)
                    .collect(toImmutableList());

            return new RemoteSourceNode(
                    remote.getSourceLocation(),
                    remote.getId(),
                    remote.getStatsEquivalentPlanNode(),
                    remote.getSourceFragmentIds(),
                    newOutputs,
                    remote.isEnsureSourceOrdering(),
                    remote.getOrderingScheme(),
                    remote.getExchangeType(),
                    remote.getEncoding(),
                    remote.getTransportType());
        }

        /**
         * Builds the new top-level {@code ProjectNode} after a successful REPLACE push. Each
         * pushed-down {@code CAST(narrowVar AS T)} assignment becomes the identity
         * {@code wideVar := wideVar}; the wide variable now arrives pre-typed from the rewritten
         * scan, so the cast is no longer needed. The identity assignment is dead weight in the
         * plan but easy to clean up — {@link com.facebook.presto.sql.planner.iterative.rule.InlineProjections}
         * removes it in the next iterative pass.
         */
        private ProjectNode buildWideProject(
                ProjectNode projectNode,
                PlanNode newSource,
                Map<VariableReferenceExpression, VariableReferenceExpression> pushed)
        {
            Assignments.Builder newProjectAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
                VariableReferenceExpression outputVar = entry.getKey();
                RowExpression expr = entry.getValue();

                if (isWideningCast(expr)) {
                    RowExpression castInput = ((CallExpression) expr).getArguments().get(0);
                    if (castInput instanceof VariableReferenceExpression
                            && pushed.containsKey((VariableReferenceExpression) castInput)) {
                        // outputVar IS wideVar (we keyed candidates by output → input). After the
                        // push, the scan produces wideVar directly, so this collapses to identity.
                        newProjectAssignments.put(outputVar, outputVar);
                        continue;
                    }
                }
                newProjectAssignments.put(outputVar, expr);
            }

            planChanged = true;
            return new ProjectNode(
                    projectNode.getSourceLocation(),
                    projectNode.getId(),
                    projectNode.getStatsEquivalentPlanNode(),
                    newSource,
                    newProjectAssignments.build(),
                    projectNode.getLocality());
        }

        /** True iff {@code expr} is {@code CAST(arg AS T)} for some supported widening pair. */
        private boolean isWideningCast(RowExpression expr)
        {
            if (!(expr instanceof CallExpression)) {
                return false;
            }
            CallExpression call = (CallExpression) expr;
            if (!functionResolution.isCastFunction(call.getFunctionHandle())) {
                return false;
            }
            if (call.getArguments().size() != 1) {
                return false;
            }
            Type fromType = call.getArguments().get(0).getType();
            Type toType = call.getType();
            Set<Type> wideningTargets = WIDENING_CAST_MAP.get(fromType);
            return wideningTargets != null && wideningTargets.contains(toType);
        }

        /**
         * True iff {@code narrowVar} appears in MORE than one assignment of the project. Counts
         * by assignment (not occurrence), so a single assignment like {@code CAST(narrow AS T)}
         * yields count=1 — the expected use that the rule is about to rewrite. count&gt;1 means
         * narrow is referenced beyond that one spot and REPLACE would dangle.
         */
        private boolean isVariableUsedElsewhere(
                VariableReferenceExpression narrowVar,
                ProjectNode projectNode)
        {
            int usageCount = 0;
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
                if (containsVariable(narrowVar, entry.getValue())) {
                    usageCount++;
                }
            }
            return usageCount > 1;
        }

        /** Visitor-based check: does {@code expression} (anywhere in its tree) reference {@code variable}? */
        private static boolean containsVariable(VariableReferenceExpression variable, RowExpression expression)
        {
            boolean[] found = {false};
            expression.accept(new DefaultRowExpressionTraversalVisitor<Void>()
            {
                @Override
                public Void visitVariableReference(VariableReferenceExpression reference, Void context)
                {
                    if (reference.equals(variable)) {
                        found[0] = true;
                    }
                    return null;
                }
            }, null);
            return found[0];
        }
    }

    /**
     * Returns true if {@code fromType → toType} is one of the widening pairs this optimizer knows
     * how to push (the public mirror of the private {@code WIDENING_CAST_MAP} inside
     * {@link Rewriter}).
     *
     * <p>Static so connector / scan operator code can ask the same question — e.g. a Hive page
     * source deciding whether a Java-side scan path can honor a wider declared type.
     */
    public static boolean isWideningTypePair(Type fromType, Type toType)
    {
        if (fromType.equals(TinyintType.TINYINT)) {
            return toType.equals(SmallintType.SMALLINT)
                    || toType.equals(IntegerType.INTEGER)
                    || toType.equals(BigintType.BIGINT);
        }
        if (fromType.equals(SmallintType.SMALLINT)) {
            return toType.equals(IntegerType.INTEGER) || toType.equals(BigintType.BIGINT);
        }
        if (fromType.equals(IntegerType.INTEGER)) {
            return toType.equals(BigintType.BIGINT);
        }
        if (fromType.equals(RealType.REAL)) {
            return toType.equals(DoubleType.DOUBLE);
        }
        if (fromType.equals(DateType.DATE)) {
            return toType.equals(TimestampType.TIMESTAMP);
        }
        return false;
    }
}

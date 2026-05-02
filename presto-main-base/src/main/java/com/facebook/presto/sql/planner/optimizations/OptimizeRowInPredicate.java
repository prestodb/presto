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
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isOptimizeRowInPredicate;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites ROW constructor IN / NOT IN predicates so the domain translator can extract
 * per-column constraints. Runs once over the plan before {@code PickTableLayout} so the
 * added simple predicates surface to the connector for partition pruning.
 *
 * <p>ROW IN:
 * <pre>
 *   ROW(c1, c2) IN (ROW('a', 1), ROW('b', 2))
 *   →
 *   (c1 IN ('a', 'b') AND c2 IN (1, 2))
 *   AND ((c1 = 'a' AND c2 = 1) OR (c1 = 'b' AND c2 = 2))
 * </pre>
 *
 * <p>ROW NOT IN:
 * <pre>
 *   ROW(c1, c2) NOT IN (ROW('a', 1), ROW('b', 2))
 *   →
 *   (c1 NOT IN ('a', 'b') OR c2 NOT IN (1, 2)
 *    OR ROW(c1, c2) NOT IN (ROW('a', 1), ROW('b', 2)))
 * </pre>
 *
 * <p>Both transformations are sound: the added simple predicates are implied by the original
 * ROW predicate, and the original is preserved (as the OR-of-ANDs in the IN case, or as the
 * trailing disjunct in the NOT IN case) for correctness.
 *
 * <p>Only fires when the filter sits directly on a TableScan (optionally through ProjectNodes) —
 * the per-column predicates only pay off when they reach a connector that can use them.
 */
public class OptimizeRowInPredicate
        implements PlanOptimizer
{
    private final FunctionResolution functionResolution;

    public OptimizeRowInPredicate(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!isOptimizeRowInPredicate(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        Rewriter rewriter = new Rewriter(functionResolution, idAllocator);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final FunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private boolean planChanged;

        Rewriter(FunctionResolution functionResolution, PlanNodeIdAllocator idAllocator)
        {
            this.functionResolution = functionResolution;
            this.idAllocator = idAllocator;
        }

        boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            // Only rewrite filters that sit directly on a TableScan (optionally through ProjectNodes).
            // The whole point is to expose per-column predicates to the connector for partition pruning,
            // so applying it elsewhere just bloats the predicate without any payoff.
            RowExpression predicate = node.getPredicate();
            RowExpression rewritten = isFilterOnScan(rewrittenSource) ? rewritePredicate(predicate) : predicate;

            if (rewrittenSource == node.getSource() && predicate.equals(rewritten)) {
                return node;
            }

            planChanged = true;
            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), rewrittenSource, rewritten);
        }

        private static boolean isFilterOnScan(PlanNode source)
        {
            PlanNode current = source;
            while (current instanceof ProjectNode) {
                current = ((ProjectNode) current).getSource();
            }
            return current instanceof TableScanNode;
        }

        /**
         * Walks the entire predicate tree (through AND, OR, CASE, function calls, etc.), rewriting
         * any eligible ROW IN / ROW NOT IN expression encountered.
         */
        private RowExpression rewritePredicate(RowExpression predicate)
        {
            return RowExpressionTreeRewriter.rewriteWith(new InnerRewriter(), predicate);
        }

        private class InnerRewriter
                extends RowExpressionRewriter<Void>
        {
            @Override
            public RowExpression rewriteSpecialForm(SpecialFormExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                if (node.getForm() == IN) {
                    Optional<RowExpression> rewritten = tryRewriteRowIn(node);
                    if (rewritten.isPresent()) {
                        return rewritten.get();
                    }
                }
                return null;
            }

            @Override
            public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                if (functionResolution.isNotFunction(node.getFunctionHandle()) && node.getArguments().size() == 1) {
                    RowExpression argument = node.getArguments().get(0);
                    if (argument instanceof SpecialFormExpression && ((SpecialFormExpression) argument).getForm() == IN) {
                        Optional<RowExpression> rewritten = tryRewriteRowNotIn((SpecialFormExpression) argument);
                        if (rewritten.isPresent()) {
                            return rewritten.get();
                        }
                    }
                }
                return null;
            }
        }

        /**
         * Extracts the ROW field variables from the IN expression's first argument, plus the list
         * of candidate ROW_CONSTRUCTOR rows. Returns empty if the pattern does not match.
         */
        private static Optional<RowFields> extractRowFields(SpecialFormExpression inExpr)
        {
            List<RowExpression> args = inExpr.getArguments();
            if (args.size() < 2) {
                return Optional.empty();
            }

            RowExpression target = args.get(0);
            if (!(target instanceof SpecialFormExpression)) {
                return Optional.empty();
            }

            SpecialFormExpression targetRow = (SpecialFormExpression) target;
            if (targetRow.getForm() != ROW_CONSTRUCTOR) {
                return Optional.empty();
            }

            List<RowExpression> rowFields = targetRow.getArguments();
            if (rowFields.isEmpty()) {
                return Optional.empty();
            }

            List<VariableReferenceExpression> fieldVars = new ArrayList<>(rowFields.size());
            for (RowExpression field : rowFields) {
                if (!(field instanceof VariableReferenceExpression)) {
                    return Optional.empty();
                }
                fieldVars.add((VariableReferenceExpression) field);
            }

            int arity = rowFields.size();
            List<SpecialFormExpression> candidateRows = new ArrayList<>(args.size() - 1);
            for (int i = 1; i < args.size(); i++) {
                if (!(args.get(i) instanceof SpecialFormExpression)) {
                    return Optional.empty();
                }
                SpecialFormExpression candidate = (SpecialFormExpression) args.get(i);
                if (candidate.getForm() != ROW_CONSTRUCTOR || candidate.getArguments().size() != arity) {
                    return Optional.empty();
                }
                candidateRows.add(candidate);
            }

            return Optional.of(new RowFields(fieldVars, candidateRows));
        }

        /**
         * Rewrites a single ROW IN expression. Returns empty if the pattern does not match.
         */
        private Optional<RowExpression> tryRewriteRowIn(SpecialFormExpression inExpr)
        {
            Optional<RowFields> maybeFields = extractRowFields(inExpr);
            if (!maybeFields.isPresent()) {
                return Optional.empty();
            }
            RowFields rowFields = maybeFields.get();

            int arity = rowFields.fieldVars.size();

            // Build: (col1 = v1_1 AND col2 = v1_2) OR (col1 = v2_1 AND col2 = v2_2) OR ...
            ImmutableList.Builder<RowExpression> disjuncts = ImmutableList.builder();
            for (SpecialFormExpression candidate : rowFields.candidateRows) {
                ImmutableList.Builder<RowExpression> conjuncts = ImmutableList.builder();
                for (int fieldIdx = 0; fieldIdx < arity; fieldIdx++) {
                    VariableReferenceExpression leftVar = rowFields.fieldVars.get(fieldIdx);
                    RowExpression rightVal = candidate.getArguments().get(fieldIdx);

                    conjuncts.add(new CallExpression(
                            EQUAL.name(),
                            functionResolution.comparisonFunction(EQUAL, leftVar.getType(), rightVal.getType()),
                            BOOLEAN,
                            ImmutableList.of(leftVar, rightVal)));
                }
                disjuncts.add(and(conjuncts.build()));
            }

            RowExpression orExpression = or(disjuncts.build());

            // Build per-column IN predicates: col1 IN (v1_1, v2_1, ...) AND col2 IN (v1_2, v2_2, ...) AND ...
            ImmutableList.Builder<RowExpression> columnInPredicates = ImmutableList.builder();
            for (int fieldIdx = 0; fieldIdx < arity; fieldIdx++) {
                columnInPredicates.add(buildColumnInPredicate(rowFields, fieldIdx));
            }

            return Optional.of(and(ImmutableList.<RowExpression>builder()
                    .addAll(columnInPredicates.build())
                    .add(orExpression)
                    .build()));
        }

        /**
         * Rewrites a single ROW NOT IN expression (passed in as the inner IN, the caller has stripped
         * the surrounding NOT). Returns empty if the pattern does not match.
         */
        private Optional<RowExpression> tryRewriteRowNotIn(SpecialFormExpression inExpr)
        {
            Optional<RowFields> maybeFields = extractRowFields(inExpr);
            if (!maybeFields.isPresent()) {
                return Optional.empty();
            }
            RowFields rowFields = maybeFields.get();

            int arity = rowFields.fieldVars.size();

            // Build: c1 NOT IN (...) OR c2 NOT IN (...) OR ... OR original ROW NOT IN
            ImmutableList.Builder<RowExpression> orDisjuncts = ImmutableList.builder();
            for (int fieldIdx = 0; fieldIdx < arity; fieldIdx++) {
                orDisjuncts.add(new CallExpression(
                        "not",
                        functionResolution.notFunction(),
                        BOOLEAN,
                        ImmutableList.of(buildColumnInPredicate(rowFields, fieldIdx))));
            }

            orDisjuncts.add(new CallExpression(
                    "not",
                    functionResolution.notFunction(),
                    BOOLEAN,
                    ImmutableList.of(inExpr)));

            return Optional.of(or(orDisjuncts.build()));
        }

        private static SpecialFormExpression buildColumnInPredicate(RowFields rowFields, int fieldIdx)
        {
            VariableReferenceExpression fieldVar = rowFields.fieldVars.get(fieldIdx);

            ImmutableList.Builder<RowExpression> columnValues = ImmutableList.builder();
            for (SpecialFormExpression candidate : rowFields.candidateRows) {
                columnValues.add(candidate.getArguments().get(fieldIdx));
            }

            return new SpecialFormExpression(
                    IN,
                    BOOLEAN,
                    ImmutableList.<RowExpression>builder()
                            .add(fieldVar)
                            .addAll(columnValues.build())
                            .build());
        }

        private static final class RowFields
        {
            final List<VariableReferenceExpression> fieldVars;
            final List<SpecialFormExpression> candidateRows;

            RowFields(List<VariableReferenceExpression> fieldVars, List<SpecialFormExpression> candidateRows)
            {
                this.fieldVars = fieldVars;
                this.candidateRows = candidateRows;
            }
        }
    }
}

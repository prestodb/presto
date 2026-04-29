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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.isRewriteRowConstructorInToDisjunction;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites predicates of the form:
 * <pre>
 *   ROW(col1, col2) IN (ROW('a', 1), ROW('b', 2), ...)
 * </pre>
 * into:
 * <pre>
 *   (col1 = 'a' AND col2 = 1)
 *   OR (col1 = 'b' AND col2 = 2)
 *   OR ...
 * </pre>
 *
 * This transformation enables the domain translator to extract per-column
 * constraints from the flattened AND/OR chain, which is impossible when the
 * predicate uses ROW-level IN comparisons. This benefits partition pruning,
 * predicate pushdown, and general filter evaluation.
 */
public class RewriteRowConstructorInToDisjunction
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FunctionResolution functionResolution;

    public RewriteRowConstructorInToDisjunction(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        if (!isRewriteRowConstructorInToDisjunction(context.getSession())) {
            return Result.empty();
        }

        RowExpression predicate = filterNode.getPredicate();
        RowExpression rewritten = rewritePredicate(predicate);

        if (predicate.equals(rewritten)) {
            return Result.empty();
        }

        return Result.ofPlanNode(new FilterNode(
                filterNode.getSourceLocation(),
                filterNode.getId(),
                filterNode.getSource(),
                rewritten));
    }

    /**
     * Walks the predicate tree looking for rewritable ROW IN expressions.
     * Handles AND conjuncts at the top level and rewrites each eligible IN independently.
     */
    private RowExpression rewritePredicate(RowExpression predicate)
    {
        if (predicate instanceof SpecialFormExpression && ((SpecialFormExpression) predicate).getForm() == IN) {
            RowExpression rewritten = tryRewriteRowIn((SpecialFormExpression) predicate);
            if (rewritten != null) {
                return rewritten;
            }
            return predicate;
        }

        List<RowExpression> conjuncts = extractConjuncts(predicate);
        if (conjuncts.size() <= 1) {
            return predicate;
        }

        boolean anyChanged = false;
        ImmutableList.Builder<RowExpression> newConjuncts = ImmutableList.builder();
        for (RowExpression conjunct : conjuncts) {
            RowExpression rewritten = rewritePredicate(conjunct);
            if (!rewritten.equals(conjunct)) {
                anyChanged = true;
            }
            newConjuncts.add(rewritten);
        }
        if (anyChanged) {
            return and(newConjuncts.build());
        }
        return predicate;
    }

    /**
     * Attempts to rewrite a single SpecialFormExpression(IN, ...) where the first argument
     * is a ROW_CONSTRUCTOR of variable references and all candidates are ROW_CONSTRUCTORs
     * of matching arity.
     *
     * Returns the rewritten expression, or null if the pattern does not match.
     */
    private RowExpression tryRewriteRowIn(SpecialFormExpression inExpr)
    {
        List<RowExpression> args = inExpr.getArguments();
        if (args.size() < 2) {
            return null;
        }

        RowExpression target = args.get(0);
        if (!(target instanceof SpecialFormExpression)) {
            return null;
        }

        SpecialFormExpression targetRow = (SpecialFormExpression) target;
        if (targetRow.getForm() != ROW_CONSTRUCTOR) {
            return null;
        }

        List<RowExpression> rowFields = targetRow.getArguments();
        if (rowFields.isEmpty()) {
            return null;
        }

        // All fields must be VariableReferenceExpressions
        List<VariableReferenceExpression> fieldVars = new ArrayList<>(rowFields.size());
        for (RowExpression field : rowFields) {
            if (!(field instanceof VariableReferenceExpression)) {
                return null;
            }
            fieldVars.add((VariableReferenceExpression) field);
        }

        // All candidate values must be ROW_CONSTRUCTORs with matching arity
        int arity = rowFields.size();
        List<SpecialFormExpression> candidateRows = new ArrayList<>(args.size() - 1);
        for (int i = 1; i < args.size(); i++) {
            if (!(args.get(i) instanceof SpecialFormExpression)) {
                return null;
            }
            SpecialFormExpression candidate = (SpecialFormExpression) args.get(i);
            if (candidate.getForm() != ROW_CONSTRUCTOR || candidate.getArguments().size() != arity) {
                return null;
            }
            candidateRows.add(candidate);
        }

        // Build: (col1 = v1_1 AND col2 = v1_2) OR (col1 = v2_1 AND col2 = v2_2) OR ...
        ImmutableList.Builder<RowExpression> disjuncts = ImmutableList.builder();
        for (SpecialFormExpression candidate : candidateRows) {
            ImmutableList.Builder<RowExpression> conjuncts = ImmutableList.builder();
            for (int fieldIdx = 0; fieldIdx < arity; fieldIdx++) {
                VariableReferenceExpression leftVar = fieldVars.get(fieldIdx);
                RowExpression rightVal = candidate.getArguments().get(fieldIdx);

                conjuncts.add(new CallExpression(
                        EQUAL.name(),
                        functionResolution.comparisonFunction(EQUAL, leftVar.getType(), rightVal.getType()),
                        BOOLEAN,
                        ImmutableList.of(leftVar, rightVal)));
            }
            disjuncts.add(and(conjuncts.build()));
        }

        return or(disjuncts.build());
    }
}

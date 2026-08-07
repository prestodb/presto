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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getRewriteSemiJoinAgainstValuesToFilterMaxSize;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.AggregationNode.isDistinct;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.coalesceNullToFalse;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.facebook.presto.sql.relational.Expressions.specialForm;

/**
 * Collapses a semi/anti-join against an inline literal {@link ValuesNode} into an {@code IN} / {@code NOT IN}
 * predicate directly on the source, so scan-level pushdowns (partition pruning) can fire just as they do for
 * the literal {@code WHERE x NOT IN (1, 2, 3)} form.
 * <pre>
 *     - Filter[ not(coalesce(semiJoinOutput, false)) ]      - Filter[ not(k IN (1, 2, 3)) or k is null ]
 *         - SemiJoin[ k = filterKey ]                  =>        - source
 *             - source
 *             - Values[ (1), (2), (3) ]
 * </pre>
 * The positive form {@code coalesce(semiJoinOutput, false)} collapses to {@code k IN (1, 2, 3)}.
 */
public class RewriteSemiJoinAgainstValuesToFilter
        implements Rule<FilterNode>
{
    private static final Capture<SemiJoinNode> SEMI_JOIN = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(semiJoin().capturedAs(SEMI_JOIN)));

    private final FunctionAndTypeManager functionAndTypeManager;

    public RewriteSemiJoinAgainstValuesToFilter(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getRewriteSemiJoinAgainstValuesToFilterMaxSize(session) > 0;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        SemiJoinNode semiJoin = captures.get(SEMI_JOIN);

        PlanNode filteringSource = context.getLookup().resolve(semiJoin.getFilteringSource());
        // Deduplication doesn't change membership, so see through a no-op DISTINCT to the literal list.
        if (filteringSource instanceof AggregationNode && isDistinct((AggregationNode) filteringSource)) {
            filteringSource = context.getLookup().resolve(((AggregationNode) filteringSource).getSource());
        }
        if (!(filteringSource instanceof ValuesNode)) {
            return Result.empty();
        }
        ValuesNode values = (ValuesNode) filteringSource;
        if (values.getOutputVariables().size() != 1 || !values.getOutputVariables().get(0).equals(semiJoin.getFilteringSourceJoinVariable())) {
            return Result.empty();
        }
        // A large inlined IN bloats the plan and the connector compacts an oversized domain; keep the semi join instead.
        if (values.getRows().size() > getRewriteSemiJoinAgainstValuesToFilterMaxSize(context.getSession())) {
            return Result.empty();
        }

        ImmutableList.Builder<RowExpression> allLiterals = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> nonNullLiterals = ImmutableList.builder();
        for (List<RowExpression> row : values.getRows()) {
            RowExpression cell = row.get(0);
            if (!(cell instanceof ConstantExpression)) {
                return Result.empty();
            }
            allLiterals.add(cell);
            if (!((ConstantExpression) cell).isNull()) {
                nonNullLiterals.add(cell);
            }
        }
        List<RowExpression> literals = nonNullLiterals.build();
        boolean hasNullLiteral = literals.size() < values.getRows().size();

        VariableReferenceExpression semiJoinOutput = semiJoin.getSemiJoinOutput();
        RowExpression coalesced = coalesceNullToFalse(semiJoinOutput);
        RowExpression notCoalesced = not(functionAndTypeManager, coalesced);
        RowExpression notBare = not(functionAndTypeManager, semiJoinOutput);

        // Exactly one conjunct must test semiJoinOutput's truthiness; it picks the rewrite, the rest pass through.
        Rewrite rewrite = null;
        ImmutableList.Builder<RowExpression> remaining = ImmutableList.builder();
        for (RowExpression conjunct : extractConjuncts(filterNode.getPredicate())) {
            Rewrite match;
            if (conjunct.equals(semiJoinOutput) || conjunct.equals(coalesced)) {
                match = Rewrite.IN_LIST;
            }
            else if (conjunct.equals(notCoalesced)) {
                match = Rewrite.NOT_IN_OR_NULL;
            }
            else if (conjunct.equals(notBare)) {
                match = Rewrite.NOT_IN;
            }
            else {
                remaining.add(conjunct);
                continue;
            }
            if (rewrite != null) {
                return Result.empty();
            }
            rewrite = match;
        }
        if (rewrite == null) {
            return Result.empty();
        }

        List<RowExpression> remainingConjuncts = remaining.build();
        // semiJoinOutput must be used only by that conjunct, or dropping the SemiJoin would dangle the reference.
        if (remainingConjuncts.stream().anyMatch(conjunct -> extractAll(conjunct).contains(semiJoinOutput))) {
            return Result.empty();
        }

        VariableReferenceExpression sourceKey = semiJoin.getSourceJoinVariable();
        RowExpression collapsed = collapse(rewrite, hasNullLiteral, sourceKey, literals);

        RowExpression predicate = and(ImmutableList.<RowExpression>builder().addAll(remainingConjuncts).add(collapsed).build());
        FilterNode newFilter = new FilterNode(filterNode.getSourceLocation(), context.getIdAllocator().getNextId(), semiJoin.getSource(), predicate);

        // Preserve the filter's output schema: re-derive semiJoinOutput as the equivalent IN (dead in the anti-join shape, pruned later).
        List<RowExpression> fullList = allLiterals.build();
        RowExpression semiJoinOutputValue = fullList.isEmpty() ? constant(false, BOOLEAN) : in(sourceKey, fullList);
        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression output : filterNode.getOutputVariables()) {
            assignments.put(output, output.equals(semiJoinOutput) ? semiJoinOutputValue : output);
        }
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newFilter, assignments.build()));
    }

    // semiJoinOutput == `sourceKey IN values` in 3VL; L is the NULL-stripped list (empty-L branches fold to a constant).
    // The two negatives are the two standard anti-join null regimes (see facebookincubator/velox#2527, which Presto
    // native must agree with): NOT_IN_OR_NULL is the regular anti-join / NOT EXISTS form (ignores build nulls, keeps
    // null source rows); NOT_IN is the null-aware anti-join / SQL NOT IN form (drops null source rows, matches nothing
    // when the list held a NULL). The doc's extra-filter (correlated subquery) caveat can't arise here: SemiJoinNode
    // carries no filter and the Values are constant, so membership never depends on the outer row.
    private RowExpression collapse(Rewrite rewrite, boolean hasNullLiteral, VariableReferenceExpression sourceKey, List<RowExpression> literals)
    {
        switch (rewrite) {
            case IN_LIST:
                return literals.isEmpty() ? constant(false, BOOLEAN) : in(sourceKey, literals);
            case NOT_IN_OR_NULL:
                return literals.isEmpty()
                        ? constant(true, BOOLEAN)
                        : or(not(functionAndTypeManager, in(sourceKey, literals)), specialForm(IS_NULL, BOOLEAN, sourceKey));
            case NOT_IN:
                if (hasNullLiteral) {
                    return constant(false, BOOLEAN);
                }
                return literals.isEmpty() ? constant(true, BOOLEAN) : not(functionAndTypeManager, in(sourceKey, literals));
            default:
                throw new IllegalArgumentException("Unexpected rewrite: " + rewrite);
        }
    }

    private enum Rewrite
    {
        IN_LIST,
        NOT_IN_OR_NULL,
        NOT_IN
    }

    private static RowExpression in(VariableReferenceExpression sourceKey, List<RowExpression> literals)
    {
        return specialForm(IN, BOOLEAN, ImmutableList.<RowExpression>builder().add(sourceKey).addAll(literals).build());
    }
}

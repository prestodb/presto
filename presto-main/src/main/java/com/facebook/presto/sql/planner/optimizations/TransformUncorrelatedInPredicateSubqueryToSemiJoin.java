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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.ExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * This optimizers looks for InPredicate expressions with subqueries, finds matching uncorrelated Apply nodes
 * and then replace Apply nodes with SemiJoin nodes and updates InPredicates.
 * <p/>
 * Plan before optimizer:
 * <pre>
 * Filter(a IN b):
 *   Apply
 *     - correlation: []  // empty
 *     - input: some plan A producing symbol a
 *     - subquery: some plan B producing symbol b
 * </pre>
 * <p/>
 * Plan after optimizer:
 * <pre>
 * Filter(semijoinresult):
 *   SemiJoin
 *     - source: plan A
 *     - filteringSource: symbol a
 *     - sourceJoinSymbol: plan B
 *     - filteringSourceJoinSymbol: symbol b
 *     - semiJoinOutput: semijoinresult
 * </pre>
 */
public class TransformUncorrelatedInPredicateSubqueryToSemiJoin
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new InPredicateRewriter(idAllocator, symbolAllocator), plan, null);
    }

    /**
     * For each node which contains InPredicate this rewriter calls {@link InsertSemiJoinRewriter} rewriter, then
     * InPredicate is replaced by semi join symbol returned from the used nested rewriter.
     */
    private static class InPredicateRewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final List<Map<InPredicate, Expression>> inPredicateMappings = new ArrayList<>();

        public InPredicateRewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            InPredicateRewriteResult inPredicateRewriteResult = rewriteInPredicates(
                    context.defaultRewrite(node, context.get()),
                    ImmutableList.of(node.getPredicate()));
            inPredicateMappings.add(inPredicateRewriteResult.inPredicateMapping);

            return new FilterNode(
                    inPredicateRewriteResult.node.getId(),
                    getOnlyElement(inPredicateRewriteResult.node.getSources()),
                    replaceInPredicates(node.getPredicate()));
        }

        private Expression replaceInPredicates(Expression expression)
        {
            for (Map<InPredicate, Expression> inPredicateMapping : inPredicateMappings) {
                expression = replaceExpression(expression, inPredicateMapping);
            }
            return expression;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            InPredicateRewriteResult inPredicateRewriteResult = rewriteInPredicates(
                    context.defaultRewrite(node, context.get()),
                    node.getAssignments().values());

            inPredicateMappings.add(inPredicateRewriteResult.inPredicateMapping);

            return new ProjectNode(inPredicateRewriteResult.node.getId(),
                    getOnlyElement(inPredicateRewriteResult.node.getSources()),
                    replaceInPredicateInAssignments(node));
        }

        private InPredicateRewriteResult rewriteInPredicates(PlanNode node, Collection<Expression> expressions)
        {
            List<InPredicate> inPredicates = extractInPredicates(expressions);
            ImmutableMap.Builder<InPredicate, Expression> inPredicateMapping = ImmutableMap.builder();
            PlanNode rewrittenNode = node;
            for (InPredicate inPredicate : inPredicates) {
                InsertSemiJoinRewriter rewriter = new InsertSemiJoinRewriter(idAllocator, symbolAllocator, inPredicate);
                rewrittenNode = rewriteWith(rewriter, rewrittenNode, null);
                inPredicateMapping.putAll(rewriter.getInPredicateMapping());
            }
            return new InPredicateRewriteResult(rewrittenNode, inPredicateMapping.build());
        }

        private static class InPredicateRewriteResult
        {
            private final PlanNode node;
            private final Map<InPredicate, Expression> inPredicateMapping;

            private InPredicateRewriteResult(PlanNode node, Map<InPredicate, Expression> inPredicateMapping)
            {
                this.node = requireNonNull(node, "node is null");
                this.inPredicateMapping = requireNonNull(inPredicateMapping, "inPredicateMapping is null");
            }
        }

        private Map<Symbol, Expression> replaceInPredicateInAssignments(ProjectNode node)
        {
            ImmutableMap.Builder<Symbol, Expression> assignmentsBuilder = ImmutableMap.builder();
            Map<Symbol, Expression> assignments = node.getAssignments();
            for (Symbol symbol : assignments.keySet()) {
                assignmentsBuilder.put(symbol, replaceInPredicates(assignments.get(symbol)));
            }
            return assignmentsBuilder.build();
        }

        private List<InPredicate> extractInPredicates(Collection<Expression> expressions)
        {
            ImmutableList.Builder<InPredicate> inPredicates = ImmutableList.builder();
            for (Expression expression : expressions) {
                new DefaultTraversalVisitor<Void, Void>()
                {
                    @Override
                    protected Void visitInPredicate(InPredicate node, Void context)
                    {
                        if (node.getValueList() instanceof SymbolReference) {
                            inPredicates.add(node);
                        }
                        return null;
                    }
                }.process(expression, null);
            }
            return inPredicates.build();
        }
    }

    /**
     * For given InPredicate (in context) it finds matching Apply node (which produces InPredicate value in apply's input,
     * and valueList in apply's subquery) and replace it with a SemiJoin node.
     * Between InPredicate's plan node and Apply node there could be several projections of InPredicate symbols, so they
     * have to be considered.
     */
    private static class InsertSemiJoinRewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private InPredicate originalInPredicate;
        private InPredicate inPredicate;
        private Optional<Symbol> semiJoinSymbol = Optional.empty();

        public InsertSemiJoinRewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, InPredicate inPredicate)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.originalInPredicate = requireNonNull(inPredicate, "inPredicate is null");
            this.inPredicate = originalInPredicate;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            inPredicate = (InPredicate) replaceExpression(inPredicate, mapAssignmentSymbolsToExpression(node.getAssignments()));

            ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node, context.get());
            if (semiJoinSymbol.isPresent()) {
                return appendIdentityProjection(rewrittenNode, semiJoinSymbol.get());
            }
            else {
                return rewrittenNode;
            }
        }

        private Map<Expression, Expression> mapAssignmentSymbolsToExpression(Map<Symbol, Expression> assignments)
        {
            return assignments.entrySet().stream()
                    .collect(toImmutableMap(e -> e.getKey().toSymbolReference(), Entry::getValue));
        }

        private ProjectNode appendIdentityProjection(ProjectNode node, Symbol symbol)
        {
            if (node.getOutputSymbols().contains(symbol)) {
                return node;
            }
            else if (node.getSource().getOutputSymbols().contains(symbol)) {
                ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builder();
                builder.putAll(node.getAssignments());
                builder.put(symbol, symbol.toSymbolReference());
                return new ProjectNode(node.getId(), node.getSource(), builder.build());
            }
            else {
                return node;
            }
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            Symbol value = Symbol.from(inPredicate.getValue());
            Symbol valueList = Symbol.from(inPredicate.getValueList());
            if (node.getCorrelation().isEmpty() && inPredicateMatchesApply(node, value, valueList)) {
                checkState(!semiJoinSymbol.isPresent(), "Semi join symbol is already set");
                semiJoinSymbol = Optional.of(symbolAllocator.newSymbol("semijoin_result", BOOLEAN));
                return new SemiJoinNode(idAllocator.getNextId(),
                        node.getInput(),
                        node.getSubquery(),
                        value,
                        valueList,
                        semiJoinSymbol.get(),
                        Optional.empty(),
                        Optional.empty()
                );
            }
            return context.defaultRewrite(node, context.get());
        }

        private boolean inPredicateMatchesApply(ApplyNode node, Symbol value, Symbol valueList)
        {
            return node.getInput().getOutputSymbols().contains(value) && node.getSubquery().getOutputSymbols().contains(valueList);
        }

        public Map<InPredicate, Expression> getInPredicateMapping()
        {
            if (!semiJoinSymbol.isPresent()) {
                return ImmutableMap.of();
            }
            return ImmutableMap.of(originalInPredicate, semiJoinSymbol.get().toSymbolReference());
        }
    }
}

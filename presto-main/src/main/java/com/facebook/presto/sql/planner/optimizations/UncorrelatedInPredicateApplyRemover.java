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
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.ExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class UncorrelatedInPredicateApplyRemover
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new InPredicateRewriter(idAllocator, symbolAllocator), plan, null);
    }

    /**
     * Finds InPredicate and calls {@link InsertSemiJoinRewriter} rewriter for each, then replace expressions that
     * InPredicate is replaced by semi join symbol returned from the used nested rewriter.
     */
    private class InPredicateRewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        public InPredicateRewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            node = (FilterNode) super.visitFilter(node, context);

            List<InPredicate> inPredicates = collectInPredicates(ImmutableList.of(node.getPredicate()));
            ImmutableMap.Builder<InPredicate, Expression> inPredicateMapping = ImmutableMap.builder();
            node = rewriteInPredicates(node, inPredicates, inPredicateMapping);
            Expression expression = replaceExpression(node.getPredicate(), inPredicateMapping.build());
            return new FilterNode(node.getId(), getOnlyElement(node.getSources()), expression);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            node = (ProjectNode) super.visitProject(node, context);

            List<InPredicate> inPredicates = collectInPredicates(node.getAssignments().values());
            ImmutableMap.Builder<InPredicate, Expression> inPredicateMapping = ImmutableMap.builder();
            node = rewriteInPredicates(node, inPredicates, inPredicateMapping);
            return new ProjectNode(node.getId(),
                    getOnlyElement(node.getSources()),
                    rewriteAssignments(node, inPredicateMapping.build()));
        }

        private <T extends PlanNode> T rewriteInPredicates(PlanNode node, List<InPredicate> inPredicates, ImmutableMap.Builder<InPredicate, Expression> inPredicateMapping)
        {
            for (InPredicate inPredicate : inPredicates) {
                InsertSemiJoinContext insertSemiJoinContext = new InsertSemiJoinContext(inPredicate);
                node = rewriteWith(new InsertSemiJoinRewriter(idAllocator, symbolAllocator), node, insertSemiJoinContext);
                inPredicateMapping.putAll(insertSemiJoinContext.getInPredicateMapping());
            }
            return (T) node;
        }

        private ImmutableMap<Symbol, Expression> rewriteAssignments(ProjectNode node, ImmutableMap<InPredicate, Expression> inPredicateMapping)
        {
            ImmutableMap.Builder<Symbol, Expression> assignmentsBuilder = ImmutableMap.builder();
            Map<Symbol, Expression> assignments = node.getAssignments();
            for (Symbol symbol : assignments.keySet()) {
                assignmentsBuilder.put(symbol, replaceExpression(assignments.get(symbol), inPredicateMapping));
            }
            return assignmentsBuilder.build();
        }

        private List<InPredicate> collectInPredicates(Collection<Expression> expressions)
        {
            ImmutableList.Builder<InPredicate> listBuilder = ImmutableList.builder();
            for (Expression expression : expressions) {
                new DefaultTraversalVisitor<Void, Void>()
                {
                    @Override
                    protected Void visitInPredicate(InPredicate node, Void context)
                    {
                        listBuilder.add(node);
                        return null;
                    }
                }.process(expression, null);
            }
            return listBuilder.build();
        }
    }

    /**
     * For given InPredicate (in context) it finds matching Apply node (which produces in predicate value in apply's input,
     * and valueList in apply's subquery) and replace it with a SemiJoin node.
     * Between InPredicate's plan node and Apply node there could be several projections of InPredicate symbols, so they
     * have to be considered.
     */
    private class InsertSemiJoinRewriter
            extends SimplePlanRewriter<InsertSemiJoinContext>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        public InsertSemiJoinRewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<InsertSemiJoinContext> context)
        {
            InPredicate inPredicate = context.get().getInPredicate();
            InPredicate unprojectedInPredicate = unprojectInPredicate(node, inPredicate);
            if (!inPredicate.equals(unprojectedInPredicate)) {
                context.get().updateInPreciate(unprojectedInPredicate);
            }
            node = (ProjectNode) super.visitProject(node, context);
            Optional<Symbol> inPredicateSymbol = context.get().getSemiJoinResultSymbol();
            if (inPredicateSymbol.isPresent()) {
                return addSymbolFromSourceToOutput(node, inPredicateSymbol.get());
            }
            else {
                return node;
            }
        }

        private InPredicate unprojectInPredicate(ProjectNode node, InPredicate inPredicate)
        {
            Expression value = inPredicate.getValue();
            Expression valueList = inPredicate.getValueList();

            for (Symbol assignmentKey : node.getAssignments().keySet()) {
                Expression assignmentValue = node.getAssignments().get(assignmentKey);
                Expression assigmentKeyExpression = assignmentKey.toQualifiedNameReference();
                if (value.equals(assigmentKeyExpression)) {
                    value = assignmentValue;
                }
                if (valueList.equals(assigmentKeyExpression)) {
                    valueList = assignmentValue;
                }
            }
            return new InPredicate(value, valueList);
        }

        private ProjectNode addSymbolFromSourceToOutput(ProjectNode node, Symbol symbol)
        {
            if (node.getOutputSymbols().contains(symbol)) {
                return node;
            }
            else if (node.getSource().getOutputSymbols().contains(symbol)) {
                ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builder();
                builder.putAll(node.getAssignments());
                builder.put(symbol, symbol.toQualifiedNameReference());
                return new ProjectNode(node.getId(), node.getSource(), builder.build());
            }
            else {
                return node;
            }
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<InsertSemiJoinContext> context)
        {
            InPredicate inPredicate = context.get().getInPredicate();
            if (inPredicate.getValueList() instanceof QualifiedNameReference) {
                Symbol value = asSymbol(inPredicate.getValue(), node.getInput());
                Symbol valueList = asSymbol(inPredicate.getValueList(), node.getSubquery());
                if (inPredicateMatchesApply(node, value, valueList)) {
                    Symbol semijoinResult = symbolAllocator.newSymbol("semijoin_result", BOOLEAN);
                    context.get().setSemiJoinResultSymbol(semijoinResult);
                    return new SemiJoinNode(idAllocator.getNextId(),
                            node.getInput(),
                            node.getSubquery(),
                            value,
                            valueList,
                            semijoinResult,
                            Optional.empty(),
                            Optional.empty()
                    );
                }
            }
            return super.visitApply(node, context);
        }

        private boolean inPredicateMatchesApply(ApplyNode node, Symbol value, Symbol valueList)
        {
            return node.getInput().getOutputSymbols().contains(value) && node.getSubquery().getOutputSymbols().contains(valueList);
        }

        private Symbol asSymbol(Expression expression, PlanNode node)
        {
            checkState(expression instanceof QualifiedNameReference);
            return Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
        }
    }

    private class InsertSemiJoinContext
    {
        private final InPredicate oryginalInPredicate;
        private InPredicate actualInPredicate;
        private Optional<Symbol> semiJoinResultSymbol = Optional.empty();

        public InsertSemiJoinContext(InPredicate inPredicate)
        {
            this.oryginalInPredicate = requireNonNull(inPredicate, "inPredicate is null");
            this.actualInPredicate = inPredicate;
        }

        public void updateInPreciate(InPredicate inPredicate)
        {
            this.actualInPredicate = requireNonNull(inPredicate, "inPredicate is null");
        }

        public InPredicate getInPredicate()
        {
            return actualInPredicate;
        }

        public void setSemiJoinResultSymbol(Symbol symbol)
        {
            checkState(!semiJoinResultSymbol.isPresent(), "Semi join result symbol is already set");
            this.semiJoinResultSymbol = Optional.of(symbol);
        }

        public Map<InPredicate, Expression> getInPredicateMapping()
        {
            if (!semiJoinResultSymbol.isPresent()) {
                return ImmutableMap.of();
            }
            return ImmutableMap.of(oryginalInPredicate, semiJoinResultSymbol.get().toQualifiedNameReference());
        }

        public Optional<Symbol> getSemiJoinResultSymbol()
        {
            return semiJoinResultSymbol;
        }
    }
}

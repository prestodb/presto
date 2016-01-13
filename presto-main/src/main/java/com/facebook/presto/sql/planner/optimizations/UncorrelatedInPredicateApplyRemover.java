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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.ExpressionNodeInliner.inlineExpression;
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
            node = (FilterNode) rewriteInPredicates(node, inPredicates, inPredicateMapping);
            Expression expression = inlineExpression(node.getPredicate(), inPredicateMapping.build());
            return new FilterNode(node.getId(), getOnlyElement(node.getSources()), expression);
        }

        private PlanNode rewriteInPredicates(PlanNode node, List<InPredicate> inPredicates, ImmutableMap.Builder<InPredicate, Expression> inPredicateMapping)
        {
            for (InPredicate inPredicate : inPredicates) {
                InsertSemiJoinContext insertSemiJoin = new InsertSemiJoinContext(inPredicate);
                node = rewriteWith(new InsertSemiJoinRewriter(idAllocator, symbolAllocator), node, insertSemiJoin);
                inPredicateMapping.putAll(insertSemiJoin.getInPredicateMapping());
            }
            return node;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            node = (ProjectNode) super.visitProject(node, context);

            List<InPredicate> inPredicates = collectInPredicates(node.getExpressions());
            ImmutableMap.Builder<InPredicate, Expression> inPredicateMapping = ImmutableMap.builder();
            node = (ProjectNode) rewriteInPredicates(node, inPredicates, inPredicateMapping);
            return new ProjectNode(node.getId(),
                    getOnlyElement(node.getSources()),
                    rewriteAssignments(node, inPredicateMapping.build()));
        }

        private ImmutableMap<Symbol, Expression> rewriteAssignments(ProjectNode node, ImmutableMap<InPredicate, Expression> inPredicateMapping)
        {
            ImmutableMap.Builder<Symbol, Expression> assignmentsBuilder = ImmutableMap.builder();
            for (Symbol symbol : node.getAssignments().keySet()) {
                assignmentsBuilder.put(symbol, inlineExpression(node.getAssignments().get(symbol), inPredicateMapping));
            }
            return assignmentsBuilder.build();
        }

        private List<InPredicate> collectInPredicates(List<Expression> expressions)
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
            InPredicate inPredicate = context.get().peekInPredicate();
            InPredicate unprojectedInPredicate = unprojectInPredicate(node, inPredicate);
            if (!inPredicate.equals(unprojectedInPredicate)) {
                context.get().pushInPredicate(unprojectedInPredicate);
            }
            node = (ProjectNode) super.visitProject(node, context);
            Optional<Symbol> inPredicateSymbol = context.get().getInPredicateSymbol();
            if (inPredicateSymbol.isPresent()) {
                return tryAppendProjection(node, inPredicateSymbol.get());
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
                inPredicate = new InPredicate(value, valueList);
            }
            return inPredicate;
        }

        private ProjectNode tryAppendProjection(ProjectNode node, Symbol symbol)
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
            InPredicate inPredicate = context.get().peekInPredicate();
            if (inPredicate.getValueList() instanceof QualifiedNameReference) {
                Symbol value = asSymbol(inPredicate.getValue());
                Symbol valueList = asSymbol(inPredicate.getValueList());
                if (node.getInput().getOutputSymbols().contains(value) && node.getSubquery().getOutputSymbols().contains(valueList)) {
                    Symbol semijoinResult = symbolAllocator.newSymbol("semijoin_result", BOOLEAN);
                    context.get().setInPredicateSymbol(semijoinResult);
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
                else {
                    return new ApplyNode(node.getId(),
                            context.rewrite(node.getInput(), context.get()),
                            node.getSubquery(),
                            node.getCorrelation());
                }
            }
            else {
                return super.visitApply(node, context);
            }
        }

        private Symbol asSymbol(Expression expression)
        {
            checkState(expression instanceof QualifiedNameReference);
            return Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
        }
    }

    private class InsertSemiJoinContext
    {
        private LinkedList<InPredicate> inPredicates = new LinkedList<>();
        private Optional<Symbol> inPredicateSymbol = Optional.empty();

        public InsertSemiJoinContext(InPredicate inPredicate)
        {
            pushInPredicate(inPredicate);
        }

        public void pushInPredicate(InPredicate inPredicate)
        {
            this.inPredicates.addLast(inPredicate);
        }

        public InPredicate peekInPredicate()
        {
            checkState(!inPredicates.isEmpty(), "InPredicate is not set");
            return inPredicates.getLast();
        }

        public void setInPredicateSymbol(Symbol symbol)
        {
            this.inPredicateSymbol = Optional.of(symbol);
        }

        public Map<InPredicate, Expression> getInPredicateMapping()
        {
            if (!inPredicateSymbol.isPresent()) {
                return ImmutableMap.of();
            }
            ImmutableMap.Builder<InPredicate, Expression> builder = ImmutableMap.builder();
            for (InPredicate inPredicate : inPredicates) {
                builder.put(inPredicate, inPredicateSymbol.get().toQualifiedNameReference());
            }
            return builder.build();
        }

        public Optional<Symbol> getInPredicateSymbol()
        {
            return inPredicateSymbol;
        }

        public void reset()
        {
            inPredicates.clear();
            inPredicateSymbol = Optional.empty();
        }
    }
}

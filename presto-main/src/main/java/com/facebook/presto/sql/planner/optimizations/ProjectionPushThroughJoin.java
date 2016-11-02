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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Pushdown Projections through Join
 *
 * Rewrite if and only if
 *  an expression's dependency fields are all from one join child, and do not exist in the other join child
 *
 * Pushdown to the join child that contains all dependency fields
 * Add ProjectNode between join child and JoinNode
 */
public class ProjectionPushThroughJoin
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!SystemSessionProperties.isPushProjectionThroughJoin(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PushdownContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<PushdownContext> context)
        {
            return buildNode(node, node.getOutputSymbols(), context);
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<PushdownContext> context)
        {
            return buildNode(node, node.getOutputSymbols(), context);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<PushdownContext> context)
        {
            if (node.getSource() instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) node.getSource();
                List<Symbol> leftOutputSymbols = joinNode.getLeft().getOutputSymbols();
                List<Symbol> rightOutputSymbols = joinNode.getRight().getOutputSymbols();
                Map<Symbol, Expression> leftAssignments = new HashMap<>();
                Map<Symbol, Expression> rightAssignments = new HashMap<>();

                // pushdown an expression through join
                // if and only if the expression's dependency fields are all from one join child
                // and do not exist in the other join child
                boolean pushdownAll = true;
                ImmutableMap.Builder<Symbol, Expression> assignmentBuilder = ImmutableMap.builder();
                for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                    Symbol key = entry.getKey();
                    Expression expression = entry.getValue();
                    if (expression instanceof SymbolReference) {
                        if (((SymbolReference) expression).equals(key.toSymbolReference())) {
                            assignmentBuilder.put(key, expression);
                            continue;
                        }
                    }
                    if (isPushDownExpression(expression)) {
                        Set<Symbol> fields = DependencyExtractor.extractUnique(expression);
                        if (fields.isEmpty()) {
                            assignmentBuilder.put(key, expression);
                            continue;
                        }
                        if (leftOutputSymbols.containsAll(fields) && !rightOutputSymbols.stream().anyMatch(s -> fields.contains(s))) {
                            assignmentBuilder.put(key, new SymbolReference(key.getName()));
                            leftAssignments.put(key, expression);
                            continue;
                        }
                        if (rightOutputSymbols.containsAll(fields) && !leftOutputSymbols.stream().anyMatch(s -> fields.contains(s))) {
                            assignmentBuilder.put(key, new SymbolReference(key.getName()));
                            rightAssignments.put(key, expression);
                            continue;
                        }
                    }
                    pushdownAll = false;
                    assignmentBuilder.put(key, expression);
                }

                if (!leftAssignments.isEmpty() || !rightAssignments.isEmpty()) {
                    if (pushdownAll) {
                        return context.rewrite(node.getSource(), new PushdownContext(leftAssignments, rightAssignments));
                    }
                    return new ProjectNode(idAllocator.getNextId(), context.rewrite(node.getSource(), new PushdownContext(leftAssignments, rightAssignments)), assignmentBuilder.build());
                }
                return context.defaultRewrite(node);
            }
            return buildNode(node, node.getSource().getOutputSymbols(), context);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<PushdownContext> context)
        {
            PushdownContext assignment = context.get();
            if (assignment == null) {
                return context.defaultRewrite(node);
            }

            PlanNode left = node.getLeft();
            PlanNode right = node.getRight();
            Map<Symbol, Expression> leftAssignments = assignment.getLeftAssignment();
            Map<Symbol, Expression> rightAssignments = assignment.getRightAssignment();
            ImmutableMap.Builder<Symbol, Expression> builder = ImmutableMap.builder();
            builder.putAll(leftAssignments);
            builder.putAll(rightAssignments);

            if (!leftAssignments.isEmpty()) {
                if (!canPushDown(leftAssignments, node)) {
                    return new ProjectNode(idAllocator.getNextId(), node, buildContext(builder.build(), node.getOutputSymbols()));
                }
                List<Symbol> leftSymbols = node.getCriteria().stream().map(EquiJoinClause::getLeft).collect(toList());
                left = context.rewrite(node.getLeft(), new PushdownContext(buildContext(leftAssignments, leftSymbols), new HashMap<>()));
            }

            if (!rightAssignments.isEmpty()) {
                if (!canPushDown(rightAssignments, node)) {
                    return new ProjectNode(idAllocator.getNextId(), node, buildContext(builder.build(), node.getOutputSymbols()));
                }
                List<Symbol> rightSymbols = node.getCriteria().stream().map(EquiJoinClause::getRight).collect(toList());
                right = context.rewrite(node.getRight(), new PushdownContext(new HashMap<>(), buildContext(rightAssignments, rightSymbols)));
            }
            return new JoinNode(idAllocator.getNextId(), node.getType(), left, right, node.getCriteria(), node.getFilter(), node.getLeftHashSymbol(), node.getRightHashSymbol());
        }

        private boolean isPushDownExpression(Expression expression)
        {
            return expression instanceof ArithmeticBinaryExpression || expression instanceof ArithmeticUnaryExpression || expression instanceof ComparisonExpression;
        }

        private boolean canPushDown(Map<Symbol, Expression> assignments, JoinNode node)
        {
            ImmutableSet.Builder<Symbol> symbolBuilder = ImmutableSet.builder();
            for (Expression expression : assignments.values()) {
                symbolBuilder.addAll(DependencyExtractor.extractUnique(expression));
            }
            ImmutableSet<Symbol> expressionSymbols = symbolBuilder.build();
            return node.getLeft().getOutputSymbols().containsAll(expressionSymbols) || node.getRight().getOutputSymbols().containsAll(expressionSymbols);
        }

        private Map<Symbol, Expression> buildContext(Map<Symbol, Expression> assignments, List<Symbol> outputs)
        {
            Map<Symbol, Expression> result = new HashMap<>();
            result.putAll(assignments);
            for (Symbol symbol : outputs) {
                if (!result.containsKey(symbol)) {
                    result.put(symbol, symbol.toSymbolReference());
                }
            }
            return result;
        }

        private PlanNode buildNode(PlanNode node, List<Symbol> symbols, RewriteContext<PushdownContext> context)
        {
            PushdownContext assignment = context.get();
            if (assignment == null) {
                return context.defaultRewrite(node);
            }
            Map<Symbol, Expression> leftAssignments = assignment.getLeftAssignment();
            Map<Symbol, Expression> rightAssignments = assignment.getRightAssignment();
            if (!leftAssignments.isEmpty()) {
                return new ProjectNode(idAllocator.getNextId(), node, buildContext(leftAssignments, symbols));
            }
            if (!rightAssignments.isEmpty()) {
                return new ProjectNode(idAllocator.getNextId(), node, buildContext(rightAssignments, symbols));
            }
            return context.defaultRewrite(node);
        }
    }

    private static class PushdownContext
    {
        private final Map<Symbol, Expression> leftAssignments;
        private final Map<Symbol, Expression> rightAssignments;

        public PushdownContext(Map<Symbol, Expression> leftAssignments, Map<Symbol, Expression> rightAssignments)
        {
            this.leftAssignments = leftAssignments;
            this.rightAssignments = rightAssignments;
        }

        public Map<Symbol, Expression> getLeftAssignment()
        {
            return leftAssignments;
        }

        public Map<Symbol, Expression> getRightAssignment()
        {
            return rightAssignments;
        }
    }
}

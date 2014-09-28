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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.RowNumberLimitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;

public class WindowFilterPushDown
        extends PlanOptimizer
{
    private static final Signature ROW_NUMBER_SIGNATURE = new Signature("row_number", StandardTypes.BIGINT, ImmutableList.<String>of());

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class WindowContext
    {
        private final Optional<WindowNode> windowNode;
        private final Expression expression;

        private WindowContext(Optional<WindowNode> windowNode, Expression expression)
        {
            this.windowNode = checkNotNull(windowNode, "windowNode is null");
            this.expression = checkNotNull(expression, "expression is null");
        }

        public Optional<WindowNode> getWindowNode()
        {
            return windowNode;
        }

        public Expression getExpression()
        {
            return expression;
        }
    }

    private static class Rewriter
            extends PlanNodeRewriter<WindowContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode rewriteWindow(WindowNode node, WindowContext context, PlanRewriter<WindowContext> planRewriter)
        {
            if (context != null && canOptimizeWindowFunction(node, context)) {
                int limit = extractLimitOptional(node, context.getExpression()).get();
                if (node.getPartitionBy().isEmpty()) {
                    WindowContext windowContext = new WindowContext(Optional.of(node), new LongLiteral(String.valueOf(limit)));
                    PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), windowContext);
                    return new WindowNode(idAllocator.getNextId(),
                            rewrittenSource,
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            node.getWindowFunctions(),
                            node.getSignatures());
                }
                if (node.getOrderBy().isEmpty()) {
                    PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
                    return new RowNumberLimitNode(idAllocator.getNextId(),
                            rewrittenSource,
                            node.getPartitionBy(),
                            getOnlyElement(node.getWindowFunctions().keySet()),
                            limit);
                }
                PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), null);
                return new TopNRowNumberNode(idAllocator.getNextId(),
                        rewrittenSource,
                        node.getPartitionBy(),
                        node.getOrderBy(),
                        node.getOrderings(),
                        getOnlyElement(node.getWindowFunctions().keySet()),
                        limit);
            }
            return planRewriter.defaultRewrite(node, null);
        }

        private static boolean canOptimizeWindowFunction(WindowNode node, WindowContext context)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            if (!isRowNumberSignature(node.getSignatures().get(rowNumberSymbol))) {
                return false;
            }

            if (!filterContainsWindowFunctions(node, context.getExpression())) {
                return false;
            }

            Optional<Integer> limit = extractLimitOptional(node, context.getExpression());
            return limit.isPresent() && limit.get() < Integer.MAX_VALUE;
        }

        private static boolean filterContainsWindowFunctions(WindowNode node, Expression filterPredicate)
        {
            Set<Symbol> windowFunctionSymbols = node.getWindowFunctions().keySet();
            Sets.SetView<Symbol> commonSymbols = Sets.intersection(DependencyExtractor.extractUnique(filterPredicate), windowFunctionSymbols);
            return !commonSymbols.isEmpty();
        }

        @Override
        public PlanNode rewriteLimit(LimitNode node, WindowContext context, PlanRewriter<WindowContext> planRewriter)
        {
            if (context != null && context.getExpression() instanceof LongLiteral &&
                    ((LongLiteral) context.getExpression()).getValue() < node.getCount()) {
                PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), context);
                if (rewrittenSource != node.getSource()) {
                    return rewrittenSource;
                }
            }
            return planRewriter.defaultRewrite(node, null);
        }

        @Override
        public PlanNode rewriteTopN(TopNNode node, WindowContext context, PlanRewriter<WindowContext> planRewriter)
        {
            return planRewriter.defaultRewrite(node, null);
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, WindowContext context, PlanRewriter<WindowContext> planRewriter)
        {
            if (context != null && context.getExpression() instanceof LongLiteral && context.getWindowNode().isPresent()) {
                PlanNode rewrittenNode = planRewriter.rewrite(node, null);
                WindowNode windowNode = context.getWindowNode().get();
                if (windowNode.getOrderBy().isEmpty()) {
                    return new LimitNode(idAllocator.getNextId(), rewrittenNode, ((LongLiteral) context.getExpression()).getValue());
                }
                return new TopNNode(
                        idAllocator.getNextId(),
                        rewrittenNode,
                        ((LongLiteral) context.getExpression()).getValue(),
                        windowNode.getOrderBy(),
                        windowNode.getOrderings(),
                        false);
            }
            return planRewriter.defaultRewrite(node, null);
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, WindowContext context, PlanRewriter<WindowContext> planRewriter)
        {
            WindowContext sourceContext = new WindowContext(Optional.<WindowNode>absent(), node.getPredicate());
            PlanNode rewrittenSource = planRewriter.rewrite(node.getSource(), sourceContext);
            if (rewrittenSource != node.getSource()) {
                return rewrittenSource;
            }
            return planRewriter.defaultRewrite(node, null);
        }
    }

    private static Optional<Integer> extractLimitOptional(WindowNode node, Expression filterPredicate)
    {
        Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
        return WindowLimitExtractor.extract(filterPredicate, rowNumberSymbol);
    }

    @VisibleForTesting
    static boolean isRowNumberSignature(Signature signature)
    {
        return signature.equals(ROW_NUMBER_SIGNATURE);
    }

    public static final class WindowLimitExtractor
    {
        private WindowLimitExtractor() {}

        public static Optional<Integer> extract(Expression expression, Symbol rowNumberSymbol)
        {
            Visitor visitor = new Visitor();
            Long limit = visitor.process(expression, rowNumberSymbol);
            if (limit == null || limit >= Integer.MAX_VALUE) {
                return Optional.absent();
            }

            return Optional.of(limit.intValue());
        }

        private static class Visitor
                extends DefaultExpressionTraversalVisitor<Long, Symbol>
        {
            @Override
            protected Long visitComparisonExpression(ComparisonExpression node, Symbol rowNumberSymbol)
            {
                QualifiedNameReference reference = extractReference(node);
                Literal literal = extractLiteral(node);
                if (!Symbol.fromQualifiedName(reference.getName()).equals(rowNumberSymbol)) {
                    return null;
                }

                if (node.getLeft() instanceof QualifiedNameReference && node.getRight() instanceof Literal) {
                    if (node.getType() == ComparisonExpression.Type.LESS_THAN_OR_EQUAL) {
                        return extractValue(literal);
                    }
                    if (node.getType() == ComparisonExpression.Type.LESS_THAN) {
                        return extractValue(literal) - 1;
                    }
                }
                else if (node.getLeft() instanceof Literal && node.getRight() instanceof QualifiedNameReference) {
                    if (node.getType() == ComparisonExpression.Type.GREATER_THAN_OR_EQUAL) {
                        return extractValue(literal);
                    }
                    if (node.getType() == ComparisonExpression.Type.GREATER_THAN) {
                        return extractValue(literal) - 1;
                    }
                }
                return null;
            }
        }

        private static QualifiedNameReference extractReference(ComparisonExpression expression)
        {
            if (expression.getLeft() instanceof QualifiedNameReference) {
                return (QualifiedNameReference) expression.getLeft();
            }
            if (expression.getRight() instanceof QualifiedNameReference) {
                return (QualifiedNameReference) expression.getRight();
            }
            throw new IllegalArgumentException("Comparison does not have a child of type QualifiedNameReference");
        }

        private static Literal extractLiteral(ComparisonExpression expression)
        {
            if (expression.getLeft() instanceof Literal) {
                return (Literal) expression.getLeft();
            }
            if (expression.getRight() instanceof Literal) {
                return (Literal) expression.getRight();
            }
            throw new IllegalArgumentException("Comparison does not have a child of type Literal");
        }

        private static long extractValue(Literal literal)
        {
            if (literal instanceof DoubleLiteral) {
                return (long) ((DoubleLiteral) literal).getValue();
            }
            if (literal instanceof LongLiteral) {
                return ((LongLiteral) literal).getValue();
            }
            throw new IllegalArgumentException("Row number compared to non numeric literal");
        }
    }
}

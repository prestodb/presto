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

import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.Rule.Context;
import com.facebook.presto.sql.planner.iterative.Rule.Result;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.sort;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.unnest;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class PushDownDereferences
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PushDownDereferences(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new PushDownDereferenceThroughJoin(metadata, sqlParser),
                new PushDownDereferenceThroughSort(metadata, sqlParser),
                new PushDownDereferenceThroughUnnest(metadata, sqlParser),
                new PushDownDereferenceThroughProject(metadata, sqlParser));
    }

    private abstract class DereferencePushDownRule<N extends PlanNode>
            implements Rule<ProjectNode>
    {
        private final Capture<N> targetCapture = newCapture();
        private final Pattern<N> targetPattern;

        protected final Metadata metadata;
        protected final SqlParser sqlParser;

        protected DereferencePushDownRule(Metadata metadata, SqlParser sqlParser, Pattern<N> targetPattern)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.targetPattern = requireNonNull(targetPattern, "targetPattern is null");
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project().with(source().matching(targetPattern.capturedAs(targetCapture)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            N child = captures.get(targetCapture);
            Map<DereferenceExpression, Symbol> expressions = getDereferenceSymbolMap(node.getAssignments().getExpressions(), context, metadata, sqlParser);
            Assignments assignments = node.getAssignments().rewrite(new DereferenceReplacer(expressions));

            Result result = pushDownDereferences(context, child, expressions, assignments);
            if (result.isEmpty()) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), result.getTransformedPlan().get(), assignments));
        }

        protected abstract Result pushDownDereferences(Context context, N targetNode, Map<DereferenceExpression, Symbol> expressions, Assignments assignments);

        protected boolean validPushDown(DereferenceExpression dereference)
        {
            Expression base = dereference.getBase();
            return (base instanceof SymbolReference) || (base instanceof DereferenceExpression);
        }
    }

    /** Transforms:
     * <pre>
     *  Project(a_x := a.msg.x)
     *    Join(a_y = b_y) => [a]
     *      Project(a_y := a.msg.y)
     *          Source(a)
     *      Project(b_y := b.msg.y)
     *          Source(b)
     *  </pre>
     * to:
     * <pre>
     *  Project(a_x := a_x)
     *    Join(a_y = b_y) => [a_x]
     *      Project(a_x := a.msg.x, a_y := a.msg.y)
     *        Source(a)
     *      Project(b_y := b.msg.y)
     *        Source(b)
     * </pre>
     */
    public class PushDownDereferenceThroughJoin
            extends DereferencePushDownRule<JoinNode>
    {
        public PushDownDereferenceThroughJoin(Metadata metadata, SqlParser sqlParser)
        {
            super(metadata, sqlParser, join());
        }

        @Override
        protected Result pushDownDereferences(Context context, JoinNode joinNode, Map<DereferenceExpression, Symbol> expressions, Assignments assignments)
        {
            List<Symbol> outputSymbols = joinNode.getOutputSymbols();
            Map<Symbol, Expression> projectExpressions = expressions.entrySet().stream()
                    .filter(entry -> validPushDown(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));

            ImmutableMap.Builder<DereferenceExpression, Symbol> dereferenceSymbolsBuilder = ImmutableMap.builder();
            dereferenceSymbolsBuilder.putAll(expressions);
            if (joinNode.getFilter().isPresent()) {
                Map<DereferenceExpression, Symbol> predicateSymbols = getDereferenceSymbolMap(ImmutableList.of(joinNode.getFilter().get()), context, metadata, sqlParser).entrySet().stream()
                        .filter(entry -> !projectExpressions.values().contains(entry.getKey()))
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                dereferenceSymbolsBuilder.putAll(predicateSymbols);
            }
            Map<DereferenceExpression, Symbol> dereferenceSymbols = dereferenceSymbolsBuilder.build();

            Map<Symbol, Expression> dereferences = dereferenceSymbols.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));

            ImmutableMap.Builder<Symbol, Expression> pushdownExpressionsBuilder = ImmutableMap.builder();
            pushdownExpressionsBuilder.putAll(dereferences);
            Map<Symbol, Expression> remainingProjectExpressions = projectExpressions.entrySet().stream()
                    .filter(entry -> !dereferences.keySet().contains(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            pushdownExpressionsBuilder.putAll(remainingProjectExpressions);

            Map<Symbol, Expression> pushdownExpressions = pushdownExpressionsBuilder.build();
            if (pushdownExpressions.isEmpty()) {
                return Result.empty();
            }

            Map<Symbol, Symbol> symbolsMap = pushdownExpressions.entrySet().stream()
                    .collect(toImmutableMap(entry -> getOnlyElement(extractAll(entry.getValue())), Map.Entry::getKey));

            PlanNode left = joinNode.getLeft();
            PlanNode right = joinNode.getRight();

            Assignments.Builder leftBuilder = Assignments.builder();
            leftBuilder.putIdentities(left.getOutputSymbols().stream()
                    .filter(symbol -> !symbolsMap.containsKey(symbol))
                    .collect(toImmutableList()));

            Assignments.Builder rightBuilder = Assignments.builder();
            rightBuilder.putIdentities(right.getOutputSymbols().stream()
                    .filter(symbol -> !symbolsMap.containsKey(symbol))
                    .collect(toImmutableList()));

            for (Map.Entry<Symbol, Expression> entry : pushdownExpressions.entrySet()) {
                Symbol outputSymbol = getOnlyElement(extractAll(entry.getValue()));
                if (left.getOutputSymbols().contains(outputSymbol)) {
                    leftBuilder.put(entry.getKey(), entry.getValue());
                }
                if (right.getOutputSymbols().contains(outputSymbol)) {
                    rightBuilder.put(entry.getKey(), entry.getValue());
                }
            }
            ProjectNode leftChild = new ProjectNode(context.getIdAllocator().getNextId(), left, leftBuilder.build());
            ProjectNode rightChild = new ProjectNode(context.getIdAllocator().getNextId(), right, rightBuilder.build());

            return Result.ofPlanNode(
                    new JoinNode(
                        context.getIdAllocator().getNextId(),
                        joinNode.getType(),
                        leftChild,
                        rightChild,
                        joinNode.getCriteria(),
                        ImmutableList.<Symbol>builder()
                                .addAll(leftChild.getOutputSymbols())
                                .addAll(rightChild.getOutputSymbols())
                                .build(),
                        joinNode.getFilter().map(expression -> replaceDereferences(expression, dereferenceSymbols)),
                        joinNode.getLeftHashSymbol(),
                        joinNode.getRightHashSymbol(),
                        joinNode.getDistributionType()));
        }
    }

    /** Transforms:
     * <pre>
     *  Project(a_x := a.msg.x)
     *    Project(a_y := key)
     *          Source(a)
     *  </pre>
     * to:
     * <pre>
     *  Project(a_y := key, a_z = a.msg.x)
     *    Source(a)
     * </pre>
     */
    public class PushDownDereferenceThroughProject
            extends DereferencePushDownRule<ProjectNode>
    {
        public PushDownDereferenceThroughProject(Metadata metadata, SqlParser sqlParser)
        {
            super(metadata, sqlParser, project());
        }

        @Override
        protected Result pushDownDereferences(Context context, ProjectNode projectNode, Map<DereferenceExpression, Symbol> expressions, Assignments assignments)
        {
            List<Symbol> outputSymbols = projectNode.getOutputSymbols();
            Map<Symbol, Expression> pushdownExpressions = expressions.entrySet().stream()
                    .filter(entry -> validPushDown(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));

            if (pushdownExpressions.isEmpty()) {
                return Result.empty();
            }

            Map<Symbol, Symbol> symbolsMap = pushdownExpressions.entrySet().stream()
                    .collect(toImmutableMap(entry -> getOnlyElement(extractAll(entry.getValue())), Map.Entry::getKey));

            Assignments.Builder sourceBuilder = Assignments.builder();
            for (Map.Entry<Symbol, Expression> entry : projectNode.getAssignments().entrySet()) {
                if (symbolsMap.containsKey(entry.getKey())) {
                    Symbol targetSymbol = symbolsMap.get(entry.getKey());
                    DereferenceExpression targetDereference = (DereferenceExpression) pushdownExpressions.get(targetSymbol);
                    DereferenceExpression dereference = new DereferenceExpression(entry.getValue(), targetDereference.getField());
                    sourceBuilder.put(targetSymbol, dereference);
                }
                else {
                    sourceBuilder.put(entry.getKey(), entry.getValue());
                }
            }
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), projectNode.getSource(), sourceBuilder.build()));
        }
    }

    /**
     * Transforms:
     * <pre>
     *  Project(a_x := a.msg.x)
     *    Sort
     *      Source(a)
     *  </pre>
     * to:
     * <pre>
     *  Sort
     *    Project(a_y := a.msg.x)
     *      Source(a)
     *  </pre>
     */
    public class PushDownDereferenceThroughSort
            extends DereferencePushDownRule<SortNode>
    {
        public PushDownDereferenceThroughSort(Metadata metadata, SqlParser sqlParser)
        {
            super(metadata, sqlParser, sort());
        }

        @Override
        protected Result pushDownDereferences(Context context, SortNode sortNode, Map<DereferenceExpression, Symbol> expressions, Assignments assignments)
        {
            List<Symbol> outputSymbols = sortNode.getOutputSymbols();
            Map<Symbol, Expression> pushdownExpressions = expressions.entrySet().stream()
                    .filter(entry -> validPushDown(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));

            if (pushdownExpressions.isEmpty()) {
                return Result.empty();
            }

            Assignments newAssignments = Assignments.builder()
                    .putAll(pushdownExpressions)
                    .putIdentities(outputSymbols)
                    .build();
            ProjectNode source = new ProjectNode(context.getIdAllocator().getNextId(), sortNode.getSource(), newAssignments);
            SortNode result = new SortNode(context.getIdAllocator().getNextId(), source, sortNode.getOrderingScheme());
            return Result.ofPlanNode(result);
        }
    }

    /**
     * Transforms:
     * <pre>
     *  Project(a_x := a.msg.x)
     *    Unnest
     *      Source(a)
     *  </pre>
     * to:
     * <pre>
     *  Unnest
     *    Project(a_y := a.msg.x)
     *      Source(a)
     *  </pre>
     */
    public class PushDownDereferenceThroughUnnest
            extends DereferencePushDownRule<UnnestNode>
    {
        public PushDownDereferenceThroughUnnest(Metadata metadata, SqlParser sqlParser)
        {
            super(metadata, sqlParser, unnest());
        }

        @Override
        protected Result pushDownDereferences(Context context, UnnestNode unnestNode, Map<DereferenceExpression, Symbol> expressions, Assignments assignments)
        {
            List<Symbol> outputSymbols = unnestNode.getOutputSymbols();
            Map<Symbol, Expression> pushdownExpressions = expressions.entrySet().stream()
                    .filter(entry -> validPushDown(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));

            if (pushdownExpressions.isEmpty()) {
                return Result.empty();
            }

            ImmutableMap.Builder<Symbol, Symbol> symbolsMapBuilder = ImmutableMap.builder();
            for (Map.Entry<DereferenceExpression, Symbol> entry : expressions.entrySet()) {
                Expression expression = entry.getKey().getBase();
                if (expression instanceof SymbolReference) {
                    symbolsMapBuilder.put(Symbol.from(expression), entry.getValue());
                }
            }
            Map<Symbol, Symbol> symbolsMap = symbolsMapBuilder.build();

            List<Symbol> sourceSymbols = unnestNode.getSource().getOutputSymbols().stream()
                    .filter(symbol -> !symbolsMap.containsKey(symbol))
                    .collect(toImmutableList());

            List<Symbol> relicateSymbols = unnestNode.getReplicateSymbols().stream()
                    .map(symbol -> replaceSymbol(symbolsMap, symbol))
                    .collect(toImmutableList());

            Assignments newAssignments = Assignments.builder()
                    .putAll(pushdownExpressions)
                    .putIdentities(sourceSymbols)
                    .build();
            ProjectNode source = new ProjectNode(context.getIdAllocator().getNextId(), unnestNode.getSource(), newAssignments);
            UnnestNode result = new UnnestNode(context.getIdAllocator().getNextId(), source, relicateSymbols, unnestNode.getUnnestSymbols(), unnestNode.getOrdinalitySymbol());
            return Result.ofPlanNode(result);
        }
    }

    private static Symbol replaceSymbol(Map<Symbol, Symbol> symbolsMap, Symbol symbol)
    {
        if (symbolsMap.containsKey(symbol)) {
            return symbolsMap.get(symbol);
        }
        return symbol;
    }

    private static Expression replaceDereferences(Expression expression, Map<DereferenceExpression, Symbol> replacements)
    {
        return ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(replacements), expression);
    }

    private static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<DereferenceExpression, Symbol> expressions;

        DereferenceReplacer(Map<DereferenceExpression, Symbol> expressions)
        {
            this.expressions = requireNonNull(expressions, "expressions is null");
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (expressions.containsKey(node)) {
                return expressions.get(node).toSymbolReference();
            }
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static List<DereferenceExpression> extractDereferenceExpressions(Expression expression)
    {
        ImmutableList.Builder<DereferenceExpression> builder = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<DereferenceExpression>>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<DereferenceExpression> context)
            {
                context.add(node);
                process(node.getBase(), context);
                return null;
            }
        }.process(expression, builder);
        return builder.build();
    }

    private static Map<DereferenceExpression, Symbol> getDereferenceSymbolMap(Collection<Expression> expressions, Context context, Metadata metadata, SqlParser sqlParser)
    {
        Set<DereferenceExpression> dereferences = expressions.stream()
                .flatMap(expression -> extractDereferenceExpressions(expression).stream())
                .collect(toImmutableSet());

        return dereferences.stream()
                .filter(expression -> !baseExists(expression, dereferences))
                .collect(toImmutableMap(Function.identity(), expression -> newSymbol(expression, context, metadata, sqlParser)));
    }

    private static Symbol newSymbol(Expression expression, Context context, Metadata metadata, SqlParser sqlParser)
    {
        Type type = getExpressionTypes(context.getSession(), metadata, sqlParser, context.getSymbolAllocator().getTypes(), expression, emptyList(), WarningCollector.NOOP).get(NodeRef.of(expression));
        verify(type != null);
        return context.getSymbolAllocator().newSymbol(expression, type);
    }

    private static boolean baseExists(DereferenceExpression expression, Set<DereferenceExpression> dereferences)
    {
        Expression base = expression.getBase();
        while (base instanceof DereferenceExpression) {
            if (dereferences.contains(base)) {
                return true;
            }
            base = ((DereferenceExpression) base).getBase();
        }
        return false;
    }
}

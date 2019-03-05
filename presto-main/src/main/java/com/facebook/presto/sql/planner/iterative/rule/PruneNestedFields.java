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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.NestedField;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.Rule.Context;
import com.facebook.presto.sql.planner.iterative.Rule.Result;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractAll;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class PruneNestedFields
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PruneNestedFields(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new PruneProjectFilter(metadata, sqlParser),
                new PruneProjectTableScan(metadata, sqlParser));
    }

    @VisibleForTesting
    public static final class PruneProjectFilter
            implements Rule<ProjectNode>
    {
        private static final Capture<FilterNode> FILTER = newCapture();
        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(filter().capturedAs(FILTER)));

        private final Metadata metadata;
        private final SqlParser sqlParser;

        public PruneProjectFilter(Metadata metadata, SqlParser sqlParser)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            //TODO: add session property
            return true;
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            FilterNode filterNode = captures.get(FILTER);
            Map<Expression, Symbol> expressions = getDereferenceSymbolMap(node, context, metadata, sqlParser);
            List<Symbol> symbols = filterNode.getOutputSymbols().stream().collect(toList());
            Map<Expression, Symbol> pushdownExpressions = expressions.entrySet().stream()
                    .filter(entry -> symbols.contains(getOnlyElement(extractAll(entry.getKey()))))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            Set<Expression> dereferences = pushdownExpressions.keySet();

            if (dereferences.isEmpty()) {
                return Result.empty();
            }

            Map<Symbol, Expression> pushdownDereferences = pushdownExpressions.entrySet().stream().collect(toMap(Map.Entry::getValue, Map.Entry::getKey));
            List<Expression> predicates = extractDereference(filterNode.getPredicate());
            Map<Symbol, Expression> predicateDereferences = predicates.stream()
                    .distinct()
                    .filter(expression -> !expressions.containsKey(expression))
                    .collect(toMap(expression -> getSymbol(expression, context, metadata, sqlParser), Function.identity()));
            Map<Expression, Symbol> predicateExpressions = predicateDereferences.entrySet().stream()
                    .collect(toMap(Map.Entry::getValue, Map.Entry::getKey));

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                assignmentsBuilder.put(entry.getKey(), ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressions), entry.getValue()));
            }
            Assignments assignments = assignmentsBuilder.build();

            List<Symbol> outputs = filterNode.getOutputSymbols().stream()
                    .filter(symbol -> assignments.getMap().containsKey(symbol))
                    .collect(toList());

            Assignments.Builder pushdownBuilder = Assignments.builder();
            pushdownBuilder.putAll(pushdownDereferences).putAll(predicateDereferences).putIdentities(outputs);
            ProjectNode child = new ProjectNode(context.getIdAllocator().getNextId(), filterNode.getSource(), pushdownBuilder.build());

            ImmutableMap.Builder<Expression, Symbol> constraintBuilder = ImmutableMap.builder();
            constraintBuilder.putAll(expressions);
            constraintBuilder.putAll(predicateExpressions);

            Expression predicate = ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(constraintBuilder.build()), filterNode.getPredicate());
            FilterNode target = new FilterNode(context.getIdAllocator().getNextId(), child, predicate);

            ProjectNode result = new ProjectNode(context.getIdAllocator().getNextId(), target, assignments);
            return Result.ofPlanNode(result);
        }
    }

    @VisibleForTesting
    public static final class PruneProjectTableScan
            implements Rule<ProjectNode>
    {
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
        private static final Pattern<ProjectNode> PATTERN = project()
                .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

        private final Metadata metadata;
        private final SqlParser sqlParser;

        public PruneProjectTableScan(Metadata metadata, SqlParser sqlParser)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        }

        @Override
        public boolean isEnabled(Session session)
        {
            //TODO: add session property
            return true;
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            TableScanNode tableScanNode = captures.get(TABLE_SCAN);
            Map<Expression, Symbol> expressions = getDereferenceSymbolMap(node, context, metadata, sqlParser);
            List<Symbol> symbols = tableScanNode.getOutputSymbols().stream().collect(toList());
            Map<Expression, Symbol> pushdownExpressions = expressions.entrySet().stream()
                    .filter(entry -> symbols.contains(getOnlyElement(extractAll(entry.getKey()))))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            Set<Expression> dereferences = pushdownExpressions.keySet();

            if (dereferences.isEmpty()) {
                return Result.empty();
            }

            NestedFieldTranslator nestedColumnTranslator = new NestedFieldTranslator(tableScanNode.getAssignments(), tableScanNode.getTable(), context.getSession());
            Map<Expression, NestedField> nestedColumns = dereferences.stream().collect(Collectors.toMap(Function.identity(), nestedColumnTranslator::toNestedField));

            Map<NestedField, ColumnHandle> nestedColumnHandles = metadata.getNestedColumnHandles(context.getSession(), tableScanNode.getTable(), nestedColumns.values()).entrySet().stream()
                    .filter(entry -> !nestedColumnTranslator.columnHandleExists(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (nestedColumnHandles.isEmpty()) {
                return Result.empty();
            }

            if (tableScanNode.getAssignments().values().containsAll(nestedColumnHandles.values())) {
                return Result.empty();
            }
            ImmutableMap.Builder<Symbol, ColumnHandle> columnHandleBuilder = ImmutableMap.builder();
            columnHandleBuilder.putAll(tableScanNode.getAssignments());

            ImmutableMap.Builder<Expression, Symbol> symbolExpressionBuilder = ImmutableMap.builder();
            for (Map.Entry<NestedField, ColumnHandle> entry : nestedColumnHandles.entrySet()) {
                NestedField nestedColumn = entry.getKey();
                Expression expression = nestedColumnTranslator.toExpression(nestedColumn);
                Symbol symbol = context.getSymbolAllocator().newSymbol(nestedColumn.getName(), getExpressionType(expression, context, metadata, sqlParser));
                symbolExpressionBuilder.put(expression, symbol);
                columnHandleBuilder.put(symbol, entry.getValue());
            }
            ImmutableMap<Symbol, ColumnHandle> nestedColumnsMap = columnHandleBuilder.build();

            TableScanNode source = new TableScanNode(context.getIdAllocator().getNextId(), tableScanNode.getTable(), ImmutableList.copyOf(nestedColumnsMap.keySet()), nestedColumnsMap, tableScanNode.getLayout(), tableScanNode.getCurrentConstraint(), tableScanNode.getEnforcedConstraint());

            Rewriter rewriter = new Rewriter(symbolExpressionBuilder.build());
            Map<Symbol, Expression> assignments = node.getAssignments().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> ExpressionTreeRewriter.rewriteWith(rewriter, entry.getValue())));
            ProjectNode target = new ProjectNode(context.getIdAllocator().getNextId(), source, Assignments.copyOf(assignments));
            return Result.ofPlanNode(target);
        }

        private class NestedFieldTranslator
        {
            private final Map<Symbol, String> symbolToColumnName;
            private final Map<String, Symbol> columnNameToSymbol;

            NestedFieldTranslator(Map<Symbol, ColumnHandle> columnHandleMap, TableHandle tableHandle, Session session)
            {
                BiMap<Symbol, String> symbolToColumnName = HashBiMap.create(columnHandleMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> metadata.getColumnMetadata(session, tableHandle, entry.getValue()).getName())));
                this.symbolToColumnName = symbolToColumnName;
                this.columnNameToSymbol = symbolToColumnName.inverse();
            }

            boolean columnHandleExists(NestedField nestedColumn)
            {
                return columnNameToSymbol.containsKey(nestedColumn.getName());
            }

            NestedField toNestedField(Expression expression)
            {
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                new DefaultExpressionTraversalVisitor<Void, Void>()
                {
                    @Override
                    protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
                    {
                        return null;
                    }

                    @Override
                    protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
                    {
                        process(node.getBase(), context);
                        builder.add(node.getField().getValue());
                        return null;
                    }

                    @Override
                    protected Void visitSymbolReference(SymbolReference node, Void context)
                    {
                        Symbol baseName = Symbol.from(node);
                        checkArgument(symbolToColumnName.containsKey(baseName), "base [%s] doesn't exist in assignments [%s]", baseName, symbolToColumnName);
                        builder.add(symbolToColumnName.get(baseName));
                        return null;
                    }
                }.process(expression, null);

                List<String> names = builder.build();
                return new NestedField(names);
            }

            Expression toExpression(NestedField nestedColumn)
            {
                Expression result = null;
                for (String part : nestedColumn.getFields()) {
                    if (result == null) {
                        checkArgument(columnNameToSymbol.containsKey(part), "element %s doesn't exist in map %s", part, columnNameToSymbol);
                        result = columnNameToSymbol.get(part).toSymbolReference();
                    }
                    else {
                        result = new DereferenceExpression(result, new Identifier(part));
                    }
                }
                return result;
            }
        }
    }

    private static class Rewriter
            extends ExpressionRewriter<Void>
    {
        private final Map<Expression, Symbol> map;

        Rewriter(Map<Expression, Symbol> map)
        {
            this.map = map;
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node)) {
                return map.get(node).toSymbolReference();
            }
            return treeRewriter.defaultRewrite(node, context);
        }

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node)) {
                return map.get(node).toSymbolReference();
            }
            return super.rewriteSymbolReference(node, context, treeRewriter);
        }
    }

    private static Type getExpressionType(Expression expression, Context context, Metadata metadata, SqlParser sqlParser)
    {
        Type type = getExpressionTypes(context.getSession(), metadata, sqlParser, context.getSymbolAllocator().getTypes(), expression, emptyList(), WarningCollector.NOOP)
                .get(NodeRef.of(expression));
        verify(type != null);
        return type;
    }

    private static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<Expression, Symbol> expressions;

        DereferenceReplacer(Map<Expression, Symbol> expressions)
        {
            this.expressions = expressions;
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

    private static List<Expression> extractDereference(Expression expression)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Expression>>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<Expression> context)
            {
                context.add(node);
                return null;
            }
        }.process(expression, builder);
        return builder.build();
    }

    private static Map<Expression, Symbol> getDereferenceSymbolMap(ProjectNode node, Context context, Metadata metadata, SqlParser sqlParser)
    {
        List<Expression> expressions = extractExpressionsNonRecursive(node).stream()
                .flatMap(expression -> extractDereference(expression).stream())
                .map(PruneNestedFields::processSubscriptDereference)
                .filter(Objects::nonNull)
                .collect(toList());

        return expressions.stream()
                .filter(expression -> !prefixExist(expression, expressions))
                .filter(expression -> expression instanceof DereferenceExpression)
                .distinct()
                .collect(toMap(Function.identity(), expression -> getSymbol(expression, context, metadata, sqlParser)));
    }

    private static Symbol getSymbol(Expression expression, Context context, Metadata metadata, SqlParser sqlParser)
    {
        return context.getSymbolAllocator().newSymbol(expression, getExpressionType(expression, context, metadata, sqlParser));
    }

    private static boolean prefixExist(Expression expression, final List<Expression> dereferences)
    {
        int[] count = {0};
        new DefaultExpressionTraversalVisitor<Void, int[]>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, int[] count)
            {
                if (dereferences.contains(node)) {
                    count[0] = count[0] + 1;
                }
                process(node.getBase(), count);
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, int[] count)
            {
                if (dereferences.contains(node)) {
                    count[0] = count[0] + 1;
                }
                return null;
            }
        }.process(expression, count);

        return count[0] > 1;
    }

    private static Expression processSubscriptDereference(Expression expression)
    {
        checkArgument(expression instanceof DereferenceExpression, "Expression: " + expression.toString() + " is not DereferenceExpression");
        SubscriptExpression[] subscriptExpression = new SubscriptExpression[1];
        boolean[] isDereferenceOrSubscript = {true};

        new DefaultExpressionTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitSubscriptExpression(SubscriptExpression node, Void context)
            {
                subscriptExpression[0] = node;
                process(node.getBase(), context);
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
            {
                if (!(node.getBase() instanceof SymbolReference || node.getBase() instanceof DereferenceExpression || node.getBase() instanceof SubscriptExpression)) {
                    isDereferenceOrSubscript[0] = false;
                }
                process(node.getBase(), context);
                return null;
            }
        }.process(expression, null);

        if (isDereferenceOrSubscript[0]) {
            return subscriptExpression[0] == null ? expression : subscriptExpression[0].getBase();
        }
        return null;
    }
}

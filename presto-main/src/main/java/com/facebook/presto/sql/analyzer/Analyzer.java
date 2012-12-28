package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.AnalyzedExpression.rewrittenExpressionGetter;
import static com.facebook.presto.sql.analyzer.AnalyzedOrdering.expressionGetter;
import static com.facebook.presto.sql.analyzer.AnalyzedOrdering.nodeGetter;
import static com.facebook.presto.sql.analyzer.Field.nameGetter;
import static com.facebook.presto.sql.tree.SortItem.sortKeyGetter;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public class Analyzer
{
    private final Metadata metadata;
    private final Session session;

    public Analyzer(Session session, Metadata metadata)
    {
        this.session = session;
        this.metadata = metadata;
    }

    public AnalysisResult analyze(Node node)
    {
        return analyze(node, new AnalysisContext(session));
    }

    private AnalysisResult analyze(Node node, AnalysisContext context)
    {
        StatementAnalyzer analyzer = new StatementAnalyzer(metadata);
        return analyzer.process(node, context);
    }

    private static class StatementAnalyzer
            extends AstVisitor<AnalysisResult, AnalysisContext>
    {
        private final Metadata metadata;

        private StatementAnalyzer(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        protected AnalysisResult visitQuery(Query query, AnalysisContext context)
        {
            Preconditions.checkArgument(query.getHaving() == null, "not yet implemented: HAVING");
            Preconditions.checkArgument(query.getFrom().size() == 1, "not yet implemented: multiple FROM relations");

            // prevent symbol allocator from picking symbols named the same as output aliases, since both share the same namespace for reference resolution
            for (Expression expression : query.getSelect().getSelectItems()) {
                if (expression instanceof AliasedExpression) {
                    context.getSymbolAllocator().blacklist(((AliasedExpression) expression).getAlias());
                }
            }

            // analyze FROM clause
            Relation relation = Iterables.getOnlyElement(query.getFrom());
            TupleDescriptor sourceDescriptor = new RelationAnalyzer(metadata, context.getSession()).process(relation, context);

            AnalyzedExpression predicate = null;
            if (query.getWhere() != null) {
                predicate = analyzePredicate(query.getWhere(), sourceDescriptor);
            }

            List<AnalyzedExpression> groupBy = analyzeGroupBy(query.getGroupBy(), sourceDescriptor, context.getSymbols());
            Set<AnalyzedFunction> aggregations = analyzeAggregations(query.getGroupBy(), query.getSelect(), query.getOrderBy(), sourceDescriptor, context.getSymbols());
            List<AnalyzedOrdering> orderBy = analyzeOrderBy(query.getOrderBy(), sourceDescriptor);
            AnalyzedOutput output = analyzeOutput(query.getSelect(), context.getSymbolAllocator(), sourceDescriptor);

            if (query.getSelect().isDistinct()) {
                analyzeDistinct(output, orderBy);
            }

            Long limit = null;
            if (query.getLimit() != null) {
                limit = Long.parseLong(query.getLimit());
            }

            return AnalysisResult.newInstance(context, query.getSelect().isDistinct(), output, predicate, groupBy, aggregations, limit, orderBy);
        }

        private void analyzeDistinct(AnalyzedOutput output, List<AnalyzedOrdering> orderBy)
        {
            Set<Expression> outputExpressions = ImmutableSet.copyOf(Iterables.transform(output.getExpressions().values(), rewrittenExpressionGetter()));

            // get all order by expression that are not in the select clause
            List<AnalyzedOrdering> missingOrderings = IterableTransformer.on(orderBy)
                    .select(compose(not(in(outputExpressions)), Functions.compose(rewrittenExpressionGetter(), expressionGetter())))
                    .list();

            if (!missingOrderings.isEmpty()) {
                List<String> expressions = IterableTransformer.on(missingOrderings)
                        .transform(nodeGetter())
                        .transform(SortItem.sortKeyGetter())
                        .transform(ExpressionFormatter.expressionFormatterFunction())
                        .list();

                throw new SemanticException(missingOrderings.get(0).getNode(), "Expressions must appear in select list for SELECT DISTINCT, ORDER BY: %s", expressions);
            }
        }

        private List<AnalyzedOrdering> analyzeOrderBy(List<SortItem> orderBy, TupleDescriptor descriptor)
        {
            ImmutableList.Builder<AnalyzedOrdering> builder = ImmutableList.builder();
            for (SortItem sortItem : orderBy) {
                if (sortItem.getNullOrdering() != SortItem.NullOrdering.UNDEFINED) {
                    throw new SemanticException(sortItem, "Custom null ordering not yet supported");
                }

                AnalyzedExpression expression = new ExpressionAnalyzer(metadata, descriptor.getSymbols()).analyze(sortItem.getSortKey(), descriptor);
                builder.add(new AnalyzedOrdering(expression, sortItem.getOrdering(), sortItem));
            }

            return builder.build();
        }

        private AnalyzedExpression analyzePredicate(Expression predicate, TupleDescriptor sourceDescriptor)
        {
            AnalyzedExpression analyzedExpression = new ExpressionAnalyzer(metadata, sourceDescriptor.getSymbols()).analyze(predicate, sourceDescriptor);
            Type expressionType = analyzedExpression.getType();
            if (expressionType != Type.BOOLEAN && expressionType != Type.NULL) {
                throw new SemanticException(predicate, "WHERE clause must evaluate to a boolean: actual type %s", expressionType);
            }
            return analyzedExpression;
        }

        /**
         * Analyzes output expressions from select clause and expands wildcard selectors (e.g., SELECT * or SELECT T.*)
         */
        private AnalyzedOutput analyzeOutput(Select select, SymbolAllocator allocator, TupleDescriptor descriptor)
        {
            ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
            ImmutableList.Builder<Optional<String>> names = ImmutableList.builder();
            for (Expression expression : select.getSelectItems()) {
                if (expression instanceof AllColumns) {
                    // expand * and T.*
                    Optional<QualifiedName> starPrefix = ((AllColumns) expression).getPrefix();
                    for (Field field : descriptor.getFields()) {
                        Optional<QualifiedName> prefix = field.getPrefix();
                        // Check if the prefix of the field name (i.e., the table name or relation alias) have a suffix matching the prefix of the wildcard
                        // e.g., SELECT T.* FROM S.T should resolve correctly
                        if (!starPrefix.isPresent() || prefix.isPresent() && prefix.get().hasSuffix(starPrefix.get())) {
                            names.add(field.getAttribute());
                            expressions.add(new QualifiedNameReference(field.getSymbol().toQualifiedName()));
                        }
                    }
                }
                else {
                    Optional<String> alias = Optional.absent();
                    if (expression instanceof AliasedExpression) {
                        AliasedExpression aliased = (AliasedExpression) expression;

                        alias = Optional.of(aliased.getAlias());
                        expression = aliased.getExpression();
                    }
                    else if (expression instanceof QualifiedNameReference) {
                        alias = Optional.of(((QualifiedNameReference) expression).getName().getSuffix());
                    }

                    names.add(alias);
                    expressions.add(expression);
                }
            }


            BiMap<Symbol, AnalyzedExpression> assignments = HashBiMap.create();

            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (Expression expression : expressions.build()) {
                AnalyzedExpression analysis = new ExpressionAnalyzer(metadata, allocator.getTypes()).analyze(expression, descriptor);

                Symbol symbol;
                if (assignments.containsValue(analysis)) {
                    symbol = assignments.inverse().get(analysis);
                }
                else {
                    symbol = allocator.newSymbol(analysis.getRewrittenExpression(), analysis.getType());
                    assignments.put(symbol, analysis);
                }

                symbols.add(symbol);
                types.add(analysis.getType());
            }

            return new AnalyzedOutput(new TupleDescriptor(names.build(), symbols.build(), types.build()), assignments);
        }

        private List<AnalyzedExpression> analyzeGroupBy(List<Expression> groupBy, TupleDescriptor descriptor, Map<Symbol, Type> symbols)
        {
            ImmutableList.Builder<AnalyzedExpression> builder = ImmutableList.builder();
            for (Expression expression : groupBy) {
                builder.add(new ExpressionAnalyzer(metadata, symbols).analyze(expression, descriptor));
            }
            return builder.build();
        }

        private Set<AnalyzedFunction> analyzeAggregations(List<Expression> groupBy, Select select, List<SortItem> orderBy, TupleDescriptor descriptor, Map<Symbol, Type> symbols)
        {
            if (!groupBy.isEmpty() && Iterables.any(select.getSelectItems(), instanceOf(AllColumns.class))) {
                throw new SemanticException(select, "Wildcard selector not supported when GROUP BY is present"); // TODO: add support for SELECT T.*, count() ... GROUP BY T.* (maybe?)
            }

            List<Expression> scalarTerms = new ArrayList<>();
            ImmutableSet.Builder<AnalyzedFunction> aggregateTermsBuilder = ImmutableSet.builder();
            // analyze select and order by terms
            for (Expression term : concat(transform(select.getSelectItems(), unaliasFunction()), transform(orderBy, sortKeyGetter()))) {
                // TODO: this doesn't currently handle queries like 'SELECT k + sum(v) FROM T GROUP BY k' correctly
                AggregateAnalyzer analyzer = new AggregateAnalyzer(metadata, descriptor, symbols);

                List<AnalyzedFunction> aggregations = analyzer.analyze(term);
                if (aggregations.isEmpty()) {
                    scalarTerms.add(term);
                }
                else {
                    aggregateTermsBuilder.addAll(aggregations);
                }
            }

            Set<AnalyzedFunction> aggregateTerms = aggregateTermsBuilder.build();

            if (!groupBy.isEmpty()) {
                Iterable<Expression> notInGroupBy = Iterables.filter(scalarTerms, not(in(groupBy)));
                if (!Iterables.isEmpty(notInGroupBy)) {
                    throw new SemanticException(select, "Expressions must appear in GROUP BY clause or be used in an aggregate function: %s", Iterables.transform(notInGroupBy, ExpressionFormatter.expressionFormatterFunction()));
                }
            }
            else {
                // if this is an aggregation query and some terms are not aggregates and there's no group by clause...
                if (!scalarTerms.isEmpty() && !aggregateTerms.isEmpty()) {
                    throw new SemanticException(select, "Mixing of aggregate and non-aggregate columns is illegal if there is no GROUP BY clause: %s", Iterables.transform(scalarTerms, ExpressionFormatter.expressionFormatterFunction()));
                }
            }

            return aggregateTerms;
        }
    }

    /**
     * Resolves and extracts aggregate functions from an expression and analyzes them (infer types and replace QualifiedNames with symbols)
     */
    private static class AggregateAnalyzer
            extends DefaultTraversalVisitor<Void, FunctionCall>
    {
        private final Metadata metadata;
        private final TupleDescriptor descriptor;
        private final Map<Symbol, Type> symbols;

        private List<AnalyzedFunction> aggregations;

        public AggregateAnalyzer(Metadata metadata, TupleDescriptor descriptor, Map<Symbol, Type> symbols)
        {
            this.metadata = metadata;
            this.descriptor = descriptor;
            this.symbols = symbols;
        }

        public List<AnalyzedFunction> analyze(Expression expression)
        {
            aggregations = new ArrayList<>();
            process(expression, null);

            return aggregations;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, FunctionCall enclosingAggregate)
        {
            ImmutableList.Builder<AnalyzedExpression> argumentsAnalysis = ImmutableList.builder();
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                AnalyzedExpression analysis = new ExpressionAnalyzer(metadata, symbols).analyze(expression, descriptor);
                argumentsAnalysis.add(analysis);
                argumentTypes.add(analysis.getType());
            }

            FunctionInfo info = metadata.getFunction(node.getName(), Lists.transform(argumentTypes.build(), Type.toRaw()));

            if (info != null && info.isAggregate()) {
                if (enclosingAggregate != null) {
                    throw new SemanticException(node, "Cannot nest aggregate functions: %s", ExpressionFormatter.toString(enclosingAggregate));
                }

                FunctionCall rewritten = TreeRewriter.rewriteWith(new NameToSymbolRewriter(descriptor), node);
                aggregations.add(new AnalyzedFunction(info, argumentsAnalysis.build(), rewritten));
                return super.visitFunctionCall(node, node); // visit children
            }

            return super.visitFunctionCall(node, enclosingAggregate);
        }
    }

    private static Function<Expression, Expression> unaliasFunction()
    {
        return new Function<Expression, Expression>()
        {
            @Override
            public Expression apply(Expression input)
            {
                if (input instanceof AliasedExpression) {
                    return ((AliasedExpression) input).getExpression();
                }

                return input;
            }
        };
    }

    private static class RelationAnalyzer
            extends DefaultTraversalVisitor<TupleDescriptor, AnalysisContext>
    {
        private final Metadata metadata;
        private final Session session;

        private RelationAnalyzer(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        protected TupleDescriptor visitTable(Table table, AnalysisContext context)
        {
            QualifiedName name = table.getName();

            if (name.getParts().size() > 3) {
                throw new SemanticException(table, "Too many dots in table name: %s", name);
            }

            List<String> parts = Lists.reverse(name.getParts());
            String tableName = parts.get(0);
            String schemaName = (parts.size() > 1) ? parts.get(1) : session.getCurrentSchema();
            String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCurrentCatalog();

            TableMetadata tableMetadata = metadata.getTable(catalogName, schemaName, tableName);
            if (tableMetadata == null) {
                throw new SemanticException(table, "Table %s does not exist", name);
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                QualifiedName prefix = QualifiedName.of(tableMetadata.getCatalogName(), tableMetadata.getSchemaName(), tableMetadata.getTableName());

                Symbol symbol = context.getSymbolAllocator().newSymbol(column.getName(), Type.fromRaw(column.getType()));

                Preconditions.checkArgument(column.getColumnHandle().isPresent(), "Column doesn't have a handle");
                fields.add(new Field(Optional.of(prefix), Optional.of(column.getName()), column.getColumnHandle(), symbol, Type.fromRaw(column.getType())));
            }

            TupleDescriptor descriptor = new TupleDescriptor(fields.build());
            context.registerTable(table, descriptor, tableMetadata);
            return descriptor;
        }

        @Override
        protected TupleDescriptor visitAliasedRelation(AliasedRelation relation, AnalysisContext context)
        {
            if (relation.getColumnNames() != null && !relation.getColumnNames().isEmpty()) {
                throw new UnsupportedOperationException("not yet implemented: column mappings in relation alias");
            }

            TupleDescriptor child = process(relation.getRelation(), context);

            ImmutableList.Builder<Field> builder = ImmutableList.builder();
            for (Field field : child.getFields()) {
                builder.add(new Field(Optional.of(QualifiedName.of(relation.getAlias())), field.getAttribute(), field.getColumn(), field.getSymbol(), field.getType()));
            }

            return new TupleDescriptor(builder.build());
        }

        @Override
        protected TupleDescriptor visitSubquery(Subquery node, AnalysisContext context)
        {
            // Analyze the subquery recursively
            AnalysisResult analysis = new Analyzer(context.getSession(), metadata).analyze(node.getQuery(), new AnalysisContext(context.getSession(), context.getSymbolAllocator()));

            context.registerInlineView(node, analysis);

            return analysis.getOutputDescriptor();
        }

        @Override
        protected TupleDescriptor visitJoin(Join node, AnalysisContext context)
        {
            if (node.getType() != Join.Type.INNER) {
                throw new SemanticException(node, "Only inner joins are supported");
            }

            TupleDescriptor left = process(node.getLeft(), context);
            TupleDescriptor right = process(node.getRight(), context);

            TupleDescriptor descriptor = new TupleDescriptor(ImmutableList.copyOf(Iterables.concat(left.getFields(), right.getFields())));

            AnalyzedExpression analyzedCriteria;

            JoinCriteria criteria = node.getCriteria();
            if (criteria instanceof NaturalJoin) {
                throw new SemanticException(node, "Natural join not supported");
            }
            else if (criteria instanceof JoinUsing) {
                List<String> columns = ((JoinUsing) criteria).getColumns();
                if (columns.size() > 1) {
                    throw new SemanticException(node, "Only single-column join criteria supported at this time");
                }

                String column = Iterables.getOnlyElement(columns);
                Field leftField = resolveSingleField(node, left, column);
                Field rightField = resolveSingleField(node, right, column);

                Expression expression = new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(leftField.getSymbol().toQualifiedName()),
                        new QualifiedNameReference(rightField.getSymbol().toQualifiedName()));

                analyzedCriteria = new ExpressionAnalyzer(metadata, descriptor.getSymbols())
                        .analyze(expression, descriptor);
            }
            else if (criteria instanceof JoinOn) {
                analyzedCriteria = new ExpressionAnalyzer(metadata, descriptor.getSymbols())
                        .analyze(((JoinOn) criteria).getExpression(), descriptor);

                Expression expression = analyzedCriteria.getRewrittenExpression();
                if (!(expression instanceof ComparisonExpression)) {
                    throw new SemanticException(node, "Non-equi joins not supported");
                }

                ComparisonExpression comparison = (ComparisonExpression) expression;
                if (comparison.getType() != ComparisonExpression.Type.EQUAL || !(comparison.getLeft() instanceof QualifiedNameReference) || !(comparison.getRight() instanceof QualifiedNameReference)) {
                    throw new SemanticException(node, "Non-equi joins not supported");
                }
            }
            else {
                throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
            }

            context.registerJoin(node, analyzedCriteria);

            return descriptor;
        }

        private Field resolveSingleField(Node node, TupleDescriptor descriptor, String column)
        {
            List<Field> matches = descriptor.resolve(QualifiedName.of(column));
            if (matches.isEmpty()) {
                throw new SemanticException(node, "Attribute '%s' cannot be resolved", column);
            }
            else if (matches.size() > 1) {
                throw new SemanticException(node, "Attribute '%s' is ambiguous. Possible matches: %s", column, Iterables.transform(matches, nameGetter()));
            }

            return Iterables.getOnlyElement(matches);
        }
    }

}

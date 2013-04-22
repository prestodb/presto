package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Set;

class TupleAnalyzer
        extends DefaultTraversalVisitor<Multimap<Optional<QualifiedName>, TupleDescriptor>, AnalysisContext>
{
    private final Analysis analysis;
    private final Session session;
    private final Metadata metadata;

    public TupleAnalyzer(Analysis analysis, Session session, Metadata metadata)
    {
        Preconditions.checkNotNull(analysis, "analysis is null");
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(metadata, "metadata is null");

        this.analysis = analysis;
        this.session = session;
        this.metadata = metadata;
    }

    @Override
    protected Multimap<Optional<QualifiedName>, TupleDescriptor> visitTable(Table table, AnalysisContext context)
    {
        if (!table.getName().getPrefix().isPresent()) {
            String name = table.getName().getSuffix();

            Query query = context.getNamedQuery(name);
            if (query != null) {
                analysis.registerNamedQuery(table, query);
                return ImmutableMultimap.of(Optional.of(QualifiedName.of(name)), analysis.getOutputDescriptor(query));
            }
        }

        QualifiedTableName name = MetadataUtil.createQualifiedTableName(session, table.getName());

        TableMetadata tableMetadata = metadata.getTable(name);
        if (tableMetadata == null) {
            throw new SemanticException(table, "Table %s does not exist", name);
        }

        TableHandle tableHandle = tableMetadata.getTableHandle().get();

        // TODO: discover columns lazily based on where they are needed (to support datasources that can't enumerate all tables)
        int index = 0;
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Field field = new Field(table.getName(), Optional.of(column.getName()), Type.fromRaw(column.getType()), index++);
            fields.add(field);
            analysis.setColumn(field, column.getColumnHandle().get());
        }

        analysis.registerTable(table, tableHandle);
        analysis.setOutputDescriptor(table, new TupleDescriptor(fields.build()));

        return ImmutableMultimap.<Optional<QualifiedName>, TupleDescriptor>builder()
                .put(Optional.of(table.getName()), new TupleDescriptor(fields.build()))
                .build();
    }

    @Override
    protected Multimap<Optional<QualifiedName>, TupleDescriptor> visitAliasedRelation(AliasedRelation relation, AnalysisContext context)
    {
        Multimap<Optional<QualifiedName>, TupleDescriptor> children = process(relation.getRelation(), context);

        ImmutableList.Builder<Field> builder = ImmutableList.builder();

        if (relation.getColumnNames() != null) {
            int totalColumns = 0;
            for (TupleDescriptor descriptor2 : children.values()) {
                totalColumns += descriptor2.getFields().size();
            }

            if (totalColumns != relation.getColumnNames().size()) {
                throw new SemanticException(relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
            }
        }

        int index = 0;
        for (TupleDescriptor descriptor : children.values()) {
            for (Field field : descriptor.getFields()) {
                Optional<String> columnAlias = field.getName();
                if (relation.getColumnNames() != null) {
                    columnAlias = Optional.of(relation.getColumnNames().get(index));
                }
                builder.add(new Field(QualifiedName.of(relation.getAlias()), columnAlias, field.getType(), index));
                index++;
            }
        }

        TupleDescriptor descriptor = new TupleDescriptor(builder.build());

        analysis.setOutputDescriptor(relation, descriptor);

        return ImmutableMultimap.<Optional<QualifiedName>, TupleDescriptor>builder()
                .put(Optional.of(QualifiedName.of(relation.getAlias())), descriptor)
                .build();
    }

    @Override
    protected Multimap<Optional<QualifiedName>, TupleDescriptor> visitSubquery(Subquery node, AnalysisContext context)
    {
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, session);
        TupleDescriptor descriptor = analyzer.process(node.getQuery(), context);

        analysis.setOutputDescriptor(node, descriptor);

        return ImmutableMultimap.<Optional<QualifiedName>, TupleDescriptor>builder()
                .put(Optional.<QualifiedName>absent(), descriptor)
                .build();
    }

    @Override
    protected Multimap<Optional<QualifiedName>, TupleDescriptor> visitJoin(Join node, AnalysisContext context)
    {
        if (node.getType() != Join.Type.INNER) {
            throw new SemanticException(node, "Only inner joins are supported");
        }

        JoinCriteria criteria = node.getCriteria();
        if (criteria instanceof NaturalJoin) {
            throw new SemanticException(node, "Natural join not supported");
        }

        Multimap<Optional<QualifiedName>, TupleDescriptor> left = process(node.getLeft(), context);
        Multimap<Optional<QualifiedName>, TupleDescriptor> right = process(node.getRight(), context);

        Multimap<Optional<QualifiedName>, TupleDescriptor> descriptors = ImmutableMultimap.<Optional<QualifiedName>, TupleDescriptor>builder()
                .putAll(left)
                .putAll(right)
                .build();

        if (criteria instanceof JoinUsing) {
            // TODO: implement proper "using" semantics with respect to output columns
            List<String> columns = ((JoinUsing) criteria).getColumns();

            Scope leftScope = new Scope(left);
            Scope rightScope = new Scope(right);

            ImmutableList.Builder<EquiJoinClause> builder = ImmutableList.builder();
            for (String column : columns) {
                Expression leftExpression = new QualifiedNameReference(QualifiedName.of(column));
                Expression rightExpression = new QualifiedNameReference(QualifiedName.of(column));

                Analyzer.analyzeExpression(metadata, leftScope, analysis, leftExpression);
                Analyzer.analyzeExpression(metadata, rightScope, analysis, rightExpression);

                builder.add(new EquiJoinClause(leftExpression, rightExpression));
            }

            analysis.setEquijoinCriteria(node, builder.build());
            return descriptors;
        }
        else if (criteria instanceof JoinOn) {
            Scope scope = new Scope(descriptors);
            Scope leftScope = new Scope(left);
            Scope rightScope = new Scope(right);

            Expression expression = ((JoinOn) criteria).getExpression();

            Analyzer.analyzeExpression(metadata, scope, analysis, expression);

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, expression, "JOIN");

            Object optimizedExpression = ExpressionInterpreter.expressionOptimizer(new NoOpSymbolResolver(), metadata, session).process(expression, null);

            if (!(optimizedExpression instanceof Expression)) {
                throw new SemanticException(node, "Joins on constant expressions (i.e., cross joins) not supported");
            }

            // analyze the optimized expression to record the types of all subexpressions
            Analyzer.analyzeExpression(metadata, scope, analysis, (Expression) optimizedExpression);

            ImmutableList.Builder<EquiJoinClause> clauses = ImmutableList.builder();
            for (Expression conjunct : ExpressionUtils.extractConjuncts((Expression) optimizedExpression)) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    throw new SemanticException(node, "Non-equi joins not supported: %s", ExpressionFormatter.toString(conjunct));
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                if (comparison.getType() != ComparisonExpression.Type.EQUAL) {
                    throw new SemanticException(node, "Non-equi joins not supported: %s", ExpressionFormatter.toString(conjunct));
                }

                Set<QualifiedName> firstDependencies = DependencyExtractor.extract(comparison.getLeft());
                Set<QualifiedName> secondDependencies = DependencyExtractor.extract(comparison.getRight());

                Expression leftExpression;
                Expression rightExpression;
                if (Iterables.all(firstDependencies, leftScope.canResolvePredicate()) && Iterables.all(secondDependencies, rightScope.canResolvePredicate())) {
                    leftExpression = comparison.getLeft();
                    rightExpression = comparison.getRight();
                }
                else if (Iterables.all(firstDependencies, rightScope.canResolvePredicate()) && Iterables.all(secondDependencies, leftScope.canResolvePredicate())) {
                    leftExpression = comparison.getRight();
                    rightExpression = comparison.getLeft();
                }
                else {
                    // must have a complex expression that involves both tuples on one side of the comparison expression (e.g., coalesce(left.x, right.x) = 1)
                    throw new SemanticException(node, "Non-equi joins not supported: %s", ExpressionFormatter.toString(conjunct));
                }

                clauses.add(new EquiJoinClause(leftExpression, rightExpression));
            }

            analysis.setEquijoinCriteria(node, clauses.build());
            return descriptors;
        }

        throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
    }

    public static class DependencyExtractor
    {
        public static Set<QualifiedName> extract(Expression expression)
        {
            ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();

            Visitor visitor = new Visitor();
            visitor.process(expression, builder);

            return builder.build();
        }

        private static class Visitor
                extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
        {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableSet.Builder<QualifiedName> builder)
            {
                builder.add(node.getName());
                return null;
            }
        }

    }

    private static class NoOpSymbolResolver
            implements SymbolResolver
    {
        @Override
        public Object getValue(Symbol symbol)
        {
            return new QualifiedNameReference(symbol.toQualifiedName());
        }
    }
}

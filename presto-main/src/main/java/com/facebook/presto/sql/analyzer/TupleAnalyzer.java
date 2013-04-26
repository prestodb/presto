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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;

class TupleAnalyzer
        extends DefaultTraversalVisitor<TupleDescriptor, AnalysisContext>
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
    protected TupleDescriptor visitTable(Table table, AnalysisContext context)
    {
        if (!table.getName().getPrefix().isPresent()) {
            // is this a reference to a WITH query?
            String name = table.getName().getSuffix();

            Query query = context.getNamedQuery(name);
            if (query != null) {
                analysis.registerNamedQuery(table, query);

                // re-alias the fields with the name assigned to the query in the WITH declaration
                TupleDescriptor queryDescriptor = analysis.getOutputDescriptor(query);
                ImmutableList.Builder<Field> fields = ImmutableList.builder();
                for (Field field : queryDescriptor.getFields()) {
                    fields.add(Field.newQualified(QualifiedName.of(name), field.getName(), field.getType()));
                }

                TupleDescriptor descriptor = new TupleDescriptor(fields.build());
                analysis.setOutputDescriptor(table, descriptor);
                return descriptor;
            }
        }

        QualifiedTableName name = MetadataUtil.createQualifiedTableName(session, table.getName());

        TableMetadata tableMetadata = metadata.getTable(name);
        if (tableMetadata == null) {
            throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
        }

        TableHandle tableHandle = tableMetadata.getTableHandle().get();

        // TODO: discover columns lazily based on where they are needed (to support datasources that can't enumerate all tables)
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Field field = Field.newQualified(table.getName(), Optional.of(column.getName()), Type.fromRaw(column.getType()));
            fields.add(field);
            analysis.setColumn(field, column.getColumnHandle().get());
        }

        analysis.registerTable(table, tableHandle);

        TupleDescriptor descriptor = new TupleDescriptor(fields.build());
        analysis.setOutputDescriptor(table, descriptor);
        return descriptor;
    }

    @Override
    protected TupleDescriptor visitAliasedRelation(AliasedRelation relation, AnalysisContext context)
    {
        TupleDescriptor child = process(relation.getRelation(), context);

        ImmutableList.Builder<Field> builder = ImmutableList.builder();

        if (relation.getColumnNames() != null) {
            int totalColumns = child.getFields().size();
            if (totalColumns != relation.getColumnNames().size()) {
                throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
            }
        }

        for (int i = 0; i < child.getFields().size(); i++) {
            Field field = child.getFields().get(i);

            Optional<String> columnAlias = field.getName();
            if (relation.getColumnNames() != null) {
                columnAlias = Optional.of(relation.getColumnNames().get(i));
            }
            builder.add(Field.newQualified(QualifiedName.of(relation.getAlias()), columnAlias, field.getType()));
        }

        TupleDescriptor descriptor = new TupleDescriptor(builder.build());

        analysis.setOutputDescriptor(relation, descriptor);
        return descriptor;
    }

    @Override
    protected TupleDescriptor visitSubquery(Subquery node, AnalysisContext context)
    {
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, session);
        TupleDescriptor descriptor = analyzer.process(node.getQuery(), context);

        analysis.setOutputDescriptor(node, descriptor);

        return descriptor;
    }

    @Override
    protected TupleDescriptor visitJoin(Join node, AnalysisContext context)
    {
        if (node.getType() != Join.Type.INNER) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only inner joins are supported");
        }

        JoinCriteria criteria = node.getCriteria();
        if (criteria instanceof NaturalJoin) {
            throw new SemanticException(NOT_SUPPORTED, node, "Natural join not supported");
        }

        TupleDescriptor left = process(node.getLeft(), context);
        TupleDescriptor right = process(node.getRight(), context);

        Sets.SetView<QualifiedName> duplicateAliases = Sets.intersection(left.getRelationAliases(), right.getRelationAliases());
        if (!duplicateAliases.isEmpty()) {
            throw new SemanticException(DUPLICATE_RELATION, node, "Relations appear more than once: %s", duplicateAliases);
        }

        // compute output descriptor (all fields from left followed by all fields from right)
        List<Field> outputFields = ImmutableList.<Field>builder()
                .addAll(left.getFields())
                .addAll(right.getFields())
                .build();

        TupleDescriptor output = new TupleDescriptor(outputFields);

        if (criteria instanceof JoinUsing) {
            // TODO: implement proper "using" semantics with respect to output columns
            List<String> columns = ((JoinUsing) criteria).getColumns();

            ImmutableList.Builder<EquiJoinClause> builder = ImmutableList.builder();
            for (String column : columns) {
                Expression leftExpression = new QualifiedNameReference(QualifiedName.of(column));
                Expression rightExpression = new QualifiedNameReference(QualifiedName.of(column));

                Analyzer.analyzeExpression(metadata, left, analysis, leftExpression);
                Analyzer.analyzeExpression(metadata, right, analysis, rightExpression);

                builder.add(new EquiJoinClause(leftExpression, rightExpression));
            }

            analysis.setEquijoinCriteria(node, builder.build());
        }
        else if (criteria instanceof JoinOn) {
            Expression expression = ((JoinOn) criteria).getExpression();

            // ensure all names can be resolved, types match, etc (we don't need to record resolved names, subexpression types, etc. because
            // we do it further down when after we determine which subexpressions apply to left vs right tuple)
            ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
            analyzer.analyze(expression, output);

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, expression, "JOIN");

            Object optimizedExpression = ExpressionInterpreter.expressionOptimizer(new NoOpSymbolResolver(), metadata, session).process(expression, null);

            if (!(optimizedExpression instanceof Expression)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Joins on constant expressions (i.e., cross joins) not supported");
            }

            ImmutableList.Builder<EquiJoinClause> clauses = ImmutableList.builder();
            for (Expression conjunct : ExpressionUtils.extractConjuncts((Expression) optimizedExpression)) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Non-equi joins not supported: %s", conjunct);
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                if (comparison.getType() != ComparisonExpression.Type.EQUAL) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Non-equi joins not supported: %s", conjunct);
                }

                Set<QualifiedName> firstDependencies = DependencyExtractor.extract(comparison.getLeft());
                Set<QualifiedName> secondDependencies = DependencyExtractor.extract(comparison.getRight());

                Expression leftExpression;
                Expression rightExpression;
                if (Iterables.all(firstDependencies, left.canResolvePredicate()) && Iterables.all(secondDependencies, right.canResolvePredicate())) {
                    leftExpression = comparison.getLeft();
                    rightExpression = comparison.getRight();
                }
                else if (Iterables.all(firstDependencies, right.canResolvePredicate()) && Iterables.all(secondDependencies, left.canResolvePredicate())) {
                    leftExpression = comparison.getRight();
                    rightExpression = comparison.getLeft();
                }
                else {
                    // must have a complex expression that involves both tuples on one side of the comparison expression (e.g., coalesce(left.x, right.x) = 1)
                    throw new SemanticException(NOT_SUPPORTED, node, "Non-equi joins not supported: %s", conjunct);
                }

                // analyze the clauses to record the types of all subexpressions and resolve names against the left/right underlying tuples
                Analyzer.analyzeExpression(metadata, left, analysis, leftExpression);
                Analyzer.analyzeExpression(metadata, right, analysis, rightExpression);

                clauses.add(new EquiJoinClause(leftExpression, rightExpression));
            }

            analysis.setEquijoinCriteria(node, clauses.build());
        }
        else {
            throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
        }

        analysis.setOutputDescriptor(node, output);
        return output;
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

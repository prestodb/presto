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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.Analyzer.ExpressionAnalysis;
import static com.facebook.presto.sql.analyzer.Field.typeGetter;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_WINDOW_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.tree.FunctionCall.distinctPredicate;
import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Iterables.transform;

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

        Optional<TableHandle> tableHandle = metadata.getTableHandle(name);
        if (!tableHandle.isPresent()) {
            if (!metadata.getCatalogNames().containsKey(name.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, table, "Catalog %s does not exist", name.getCatalogName());
            }
            if (!metadata.listSchemaNames(name.getCatalogName()).contains(name.getSchemaName())) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema %s does not exist", name.getSchemaName());
            }
            throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
        }
        TableMetadata tableMetadata = metadata.getTableMetadata(tableHandle.get());
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(tableHandle.get());

        // TODO: discover columns lazily based on where they are needed (to support datasources that can't enumerate all tables)
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Field field = Field.newQualified(table.getName(), Optional.of(column.getName()), Type.fromRaw(column.getType()));
            fields.add(field);
            analysis.setColumn(field, columns.get(column.getName()));
        }

        analysis.registerTable(table, tableHandle.get());

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
    protected TupleDescriptor visitSampledRelation(final SampledRelation relation, AnalysisContext context)
    {

        if (relation.getColumnsToStratifyOn().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, relation, "STRATIFY ON is not yet implemented");
        }

        // We use the optimizer to be able to produce a semantic exception if columns are referenced in the expression.
        // We can't do this with the interpreter yet because it's designed for the execution stage and has the wrong shape.
        // So, for now, we punt on supporting non-deterministic functions.
        ExpressionInterpreter samplePercentageEval = ExpressionInterpreter.expressionOptimizer(relation.getSamplePercentage(), metadata, session);

        Object samplePercentageObject = samplePercentageEval.optimize(new SymbolResolver()
        {
            @Override
            public Object getValue(Symbol symbol)
            {
                throw new SemanticException(NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage cannot contain column references");
            }
        });

        if (!(samplePercentageObject instanceof Number)) {
            throw new SemanticException(SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE, relation.getSamplePercentage(), "Sample percentage should evaluate to a numeric expression");
        }

        double samplePercentageValue = ((Number) samplePercentageObject).doubleValue();

        if (samplePercentageValue < 0.0 || samplePercentageValue > 100.0) {
            throw new SemanticException(SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE, relation.getSamplePercentage(), "Sample percentage must be between 0 and 100");
        }

        TupleDescriptor descriptor = process(relation.getRelation(), context);

        analysis.setOutputDescriptor(relation, descriptor);
        analysis.setSampleRatio(relation, samplePercentageValue / 100);

        return descriptor;
    }

    @Override
    protected TupleDescriptor visitTableSubquery(TableSubquery node, AnalysisContext context)
    {
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, session, Optional.<QueryExplainer>absent());
        TupleDescriptor descriptor = analyzer.process(node.getQuery(), context);

        analysis.setOutputDescriptor(node, descriptor);

        return descriptor;
    }

    @Override
    protected TupleDescriptor visitQuerySpecification(QuerySpecification node, AnalysisContext parentContext)
    {
        // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
        // to pass down to analyzeFrom

        AnalysisContext context = new AnalysisContext(parentContext);

        TupleDescriptor tupleDescriptor = analyzeFrom(node, context);

        analyzeWhere(node, tupleDescriptor, context);

        List<FieldOrExpression> outputExpressions = analyzeSelect(node, tupleDescriptor, context);
        List<FieldOrExpression> groupByExpressions = analyzeGroupBy(node, tupleDescriptor, context, outputExpressions);
        List<FieldOrExpression> orderByExpressions = analyzeOrderBy(node, tupleDescriptor, context, outputExpressions);
        analyzeHaving(node, tupleDescriptor, context);

        analyzeAggregations(node, tupleDescriptor, groupByExpressions, outputExpressions, orderByExpressions);
        analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

        TupleDescriptor descriptor = computeOutputDescriptor(node, tupleDescriptor);
        analysis.setOutputDescriptor(node, descriptor);

        return descriptor;
    }

    @Override
    protected TupleDescriptor visitUnion(Union node, AnalysisContext context)
    {
        Preconditions.checkState(node.getRelations().size() >= 2);

        TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata);

        // Use the first descriptor as the output descriptor for the UNION
        TupleDescriptor outputDescriptor = analyzer.process(node.getRelations().get(0), context);

        for (Relation relation : Iterables.skip(node.getRelations(), 1)) {
            TupleDescriptor descriptor = analyzer.process(relation, context);
            if (!elementsEqual(transform(outputDescriptor.getFields(), typeGetter()), transform(descriptor.getFields(), typeGetter()))) {
                throw new SemanticException(MISMATCHED_SET_COLUMN_TYPES, node, "Union query terms have mismatched columns");
            }
        }

        analysis.setOutputDescriptor(node, outputDescriptor);
        return outputDescriptor;
    }

    @Override
    protected TupleDescriptor visitIntersect(Intersect node, AnalysisContext context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "INTERSECT not yet implemented");
    }

    @Override
    protected TupleDescriptor visitExcept(Except node, AnalysisContext context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "EXCEPT not yet implemented");
    }

    @Override
    protected TupleDescriptor visitJoin(Join node, AnalysisContext context)
    {
        if (!EnumSet.of(Join.Type.INNER, Join.Type.LEFT, Join.Type.RIGHT).contains(node.getType())) {
            throw new SemanticException(NOT_SUPPORTED, node, "Only inner, left, and right joins are supported");
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

                ExpressionAnalysis leftExpressionAnalysis = Analyzer.analyzeExpression(session, metadata, left, analysis, context, leftExpression);
                ExpressionAnalysis rightExpressionAnalysis = Analyzer.analyzeExpression(session, metadata, right, analysis, context, rightExpression);
                Preconditions.checkState(leftExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");
                Preconditions.checkState(rightExpressionAnalysis.getSubqueryInPredicates().isEmpty(), "INVARIANT");

                builder.add(new EquiJoinClause(leftExpression, rightExpression));
            }

            analysis.setEquijoinCriteria(node, builder.build());
        }
        else if (criteria instanceof JoinOn) {
            Expression expression = ((JoinOn) criteria).getExpression();

            // ensure all names can be resolved, types match, etc (we don't need to record resolved names, subexpression types, etc. because
            // we do it further down when after we determine which subexpressions apply to left vs right tuple)
            ExpressionAnalyzer analyzer = new ExpressionAnalyzer(analysis, session, metadata);
            analyzer.analyze(expression, output, context);

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, expression, "JOIN");

            Object optimizedExpression = ExpressionInterpreter.expressionOptimizer(expression, metadata, session).optimize(NoOpSymbolResolver.INSTANCE);

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
                ExpressionAnalysis leftExpressionAnalysis = Analyzer.analyzeExpression(session, metadata, left, analysis, context, leftExpression);
                ExpressionAnalysis rightExpressionAnalysis = Analyzer.analyzeExpression(session, metadata, right, analysis, context, rightExpression);
                analysis.addJoinInPredicates(node, new Analysis.JoinInPredicates(leftExpressionAnalysis.getSubqueryInPredicates(), rightExpressionAnalysis.getSubqueryInPredicates()));

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

    private void analyzeWindowFunctions(QuerySpecification node, List<FieldOrExpression> outputExpressions, List<FieldOrExpression> orderByExpressions)
    {
        WindowFunctionExtractor extractor = new WindowFunctionExtractor();

        for (FieldOrExpression fieldOrExpression : Iterables.concat(outputExpressions, orderByExpressions)) {
            if (fieldOrExpression.isExpression()) {
                extractor.process(fieldOrExpression.getExpression(), null);
            }
        }

        List<FunctionCall> windowFunctions = extractor.getWindowFunctions();

        for (FunctionCall windowFunction : windowFunctions) {
            Window window = windowFunction.getWindow().get();

            WindowFunctionExtractor nestedExtractor = new WindowFunctionExtractor();
            for (Expression argument : windowFunction.getArguments()) {
                nestedExtractor.process(argument, null);
            }

            for (Expression expression : window.getPartitionBy()) {
                nestedExtractor.process(expression, null);
            }

            for (SortItem sortItem : window.getOrderBy()) {
                nestedExtractor.process(sortItem.getSortKey(), null);
            }

            if (window.getFrame().isPresent()) {
                nestedExtractor.process(window.getFrame().get(), null);
            }

            if (!nestedExtractor.getWindowFunctions().isEmpty()) {
                throw new SemanticException(NESTED_WINDOW, node, "Cannot nest window functions inside window function '%s': %s",
                        windowFunction,
                        extractor.getWindowFunctions());
            }

            if (windowFunction.isDistinct()) {
                throw new SemanticException(NOT_SUPPORTED, node, "DISTINCT in window function parameters not yet supported: %s", windowFunction);
            }

            if (window.getFrame().isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Window frames not yet supported");
            }

            List<Type> argumentTypes = Lists.transform(windowFunction.getArguments(), new Function<Expression, Type>()
            {
                @Override
                public Type apply(Expression input)
                {
                    return analysis.getType(input);
                }
            });

            FunctionInfo info = metadata.getFunction(windowFunction.getName(), argumentTypes);
            if (!info.isWindow()) {
                throw new SemanticException(MUST_BE_WINDOW_FUNCTION, node, "Not a window function: %s", windowFunction.getName());
            }
        }

        analysis.setWindowFunctions(node, windowFunctions);
    }

    private void analyzeHaving(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        if (node.getHaving().isPresent()) {
            Expression predicate = node.getHaving().get();

            ExpressionAnalysis expressionAnalysis = Analyzer.analyzeExpression(session, metadata, tupleDescriptor, analysis, context, predicate);
            analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());

            if (expressionAnalysis.getType() != Type.BOOLEAN && expressionAnalysis.getType() != Type.NULL) {
                throw new SemanticException(TYPE_MISMATCH, predicate, "HAVING clause must evaluate to a boolean: actual type %s", expressionAnalysis.getType());
            }

            analysis.setHaving(node, predicate);
        }
    }

    private List<FieldOrExpression> analyzeOrderBy(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context, List<FieldOrExpression> outputExpressions)
    {
        List<SortItem> items = node.getOrderBy();

        ImmutableList.Builder<FieldOrExpression> orderByExpressionsBuilder = ImmutableList.builder();

        if (!items.isEmpty()) {
            // Compute aliased output terms so we can resolve order by expressions against them first
            ImmutableMultimap.Builder<QualifiedName, Expression> byAliasBuilder = ImmutableMultimap.builder();
            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof SingleColumn) {
                    Optional<String> alias = ((SingleColumn) item).getAlias();
                    if (alias.isPresent()) {
                        byAliasBuilder.put(QualifiedName.of(alias.get()), ((SingleColumn) item).getExpression()); // TODO: need to know if alias was quoted
                    }
                }
            }
            Multimap<QualifiedName, Expression> byAlias = byAliasBuilder.build();

            for (SortItem item : items) {
                Expression expression = item.getSortKey();

                FieldOrExpression orderByExpression = null;
                if (expression instanceof QualifiedNameReference && !((QualifiedNameReference) expression).getName().getPrefix().isPresent()) {
                    // if this is a simple name reference, try to resolve against output columns

                    QualifiedName name = ((QualifiedNameReference) expression).getName();
                    Collection<Expression> expressions = byAlias.get(name);
                    if (expressions.size() > 1) {
                        throw new SemanticException(AMBIGUOUS_ATTRIBUTE, expression, "'%s' in ORDER BY is ambiguous", name.getSuffix());
                    }
                    else if (expressions.size() == 1) {
                        orderByExpression = new FieldOrExpression(Iterables.getOnlyElement(expressions));
                    }

                    // otherwise, couldn't resolve name against output aliases, so fall through...
                }
                else if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > outputExpressions.size()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    orderByExpression = outputExpressions.get((int) (ordinal - 1));
                }

                // otherwise, just use the expression as is
                if (orderByExpression == null) {
                    orderByExpression = new FieldOrExpression(expression);
                }

                if (orderByExpression.isExpression()) {
                    ExpressionAnalysis expressionAnalysis = Analyzer.analyzeExpression(session, metadata, tupleDescriptor, analysis, context, orderByExpression.getExpression());
                    analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());
                }

                orderByExpressionsBuilder.add(orderByExpression);
            }
        }

        List<FieldOrExpression> orderByExpressions = orderByExpressionsBuilder.build();
        analysis.setOrderByExpressions(node, orderByExpressions);

        if (node.getSelect().isDistinct() && !outputExpressions.containsAll(orderByExpressions)) {
            throw new SemanticException(ORDER_BY_MUST_BE_IN_SELECT, node.getSelect(), "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        }
        return orderByExpressions;
    }

    private List<FieldOrExpression> analyzeGroupBy(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context, List<FieldOrExpression> outputExpressions)
    {
        ImmutableList.Builder<FieldOrExpression> groupByExpressionsBuilder = ImmutableList.builder();
        if (!node.getGroupBy().isEmpty()) {
            // Translate group by expressions that reference ordinals
            for (Expression expression : node.getGroupBy()) {
                // first, see if this is an ordinal
                FieldOrExpression groupByExpression;

                if (expression instanceof LongLiteral) {
                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > outputExpressions.size()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "GROUP BY position %s is not in select list", ordinal);
                    }

                    groupByExpression = outputExpressions.get((int) (ordinal - 1));
                }
                else {
                    ExpressionAnalysis expressionAnalysis = Analyzer.analyzeExpression(session, metadata, tupleDescriptor, analysis, context, expression);
                    analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());
                    groupByExpression = new FieldOrExpression(expression);
                }

                if (groupByExpression.isExpression()) {
                    Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, groupByExpression.getExpression(), "GROUP BY");
                }

                groupByExpressionsBuilder.add(groupByExpression);
            }
        }

        List<FieldOrExpression> groupByExpressions = groupByExpressionsBuilder.build();
        analysis.setGroupByExpressions(node, groupByExpressions);
        return groupByExpressions;
    }

    private TupleDescriptor computeOutputDescriptor(QuerySpecification node, TupleDescriptor inputTupleDescriptor)
    {
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                for (Field field : inputTupleDescriptor.resolveFieldsWithPrefix(starPrefix)) {
                    outputFields.add(Field.newUnqualified(field.getName(), field.getType()));
                }
            }
            else if (item instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) item;

                Optional<String> alias = column.getAlias();
                if (!alias.isPresent() && column.getExpression() instanceof QualifiedNameReference) {
                    alias = Optional.of(((QualifiedNameReference) column.getExpression()).getName().getSuffix());
                }

                outputFields.add(Field.newUnqualified(alias, analysis.getType(column.getExpression()))); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
            }
        }

        return new TupleDescriptor(outputFields.build());
    }

    private List<FieldOrExpression> analyzeSelect(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        ImmutableList.Builder<FieldOrExpression> outputExpressionBuilder = ImmutableList.builder();

        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                List<Integer> fields = tupleDescriptor.resolveFieldIndexesWithPrefix(starPrefix);
                if (fields.isEmpty()) {
                    if (starPrefix.isPresent()) {
                        throw new SemanticException(MISSING_TABLE, item, "Table '%s' not found", starPrefix.get());
                    }
                    else {
                        throw new SemanticException(WILDCARD_WITHOUT_FROM, item, "SELECT * not allowed in queries without FROM clause");
                    }
                }

                for (int fieldIndex : fields) {
                    outputExpressionBuilder.add(new FieldOrExpression(fieldIndex));
                }
            }
            else if (item instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) item;
                ExpressionAnalysis expressionAnalysis = Analyzer.analyzeExpression(session, metadata, tupleDescriptor, analysis, context, column.getExpression());
                analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());
                outputExpressionBuilder.add(new FieldOrExpression(column.getExpression()));
            }
            else {
                throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
            }
        }

        ImmutableList<FieldOrExpression> result = outputExpressionBuilder.build();
        analysis.setOutputExpressions(node, result);

        return result;
    }

    private void analyzeWhere(QuerySpecification node, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        if (node.getWhere().isPresent()) {
            Expression predicate = node.getWhere().get();

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, predicate, "WHERE");

            ExpressionAnalysis expressionAnalysis = Analyzer.analyzeExpression(session, metadata, tupleDescriptor, analysis, context, predicate);
            analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());

            if (expressionAnalysis.getType() != Type.BOOLEAN && expressionAnalysis.getType() != Type.NULL) {
                throw new SemanticException(TYPE_MISMATCH, predicate, "WHERE clause must evaluate to a boolean: actual type %s", expressionAnalysis.getType());
            }

            analysis.setWhere(node, predicate);
        }
    }

    private TupleDescriptor analyzeFrom(QuerySpecification node, AnalysisContext context)
    {
        TupleDescriptor fromDescriptor = new TupleDescriptor();

        if (node.getFrom() != null && !node.getFrom().isEmpty()) {
            TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata);
            if (node.getFrom().size() != 1) {
                throw new SemanticException(NOT_SUPPORTED, node, "Cross joins not yet supported");
            }

            fromDescriptor = analyzer.process(Iterables.getOnlyElement(node.getFrom()), context);
        }

        return fromDescriptor;
    }

    private void analyzeAggregations(QuerySpecification node,
            TupleDescriptor tupleDescriptor,
            List<FieldOrExpression> groupByExpressions,
            List<FieldOrExpression> outputExpressions,
            List<FieldOrExpression> orderByExpressions)
    {
        List<FunctionCall> aggregates = extractAggregates(node);

        // is this an aggregation query?
        if (!aggregates.isEmpty() || !groupByExpressions.isEmpty()) {
            // we only support DISTINCT in aggregations for simple cases right now. (single DISTINCT expression in the query)
            if (Iterables.any(aggregates, distinctPredicate())) {
                int uniqueArguments = IterableTransformer.on(aggregates)
                        .transform(FunctionCall.argumentsGetter())
                        .set()
                        .size();
                if (uniqueArguments != 1 || !Iterables.all(aggregates, distinctPredicate())) {
                    throw new SemanticException(NOT_SUPPORTED, node, "All DISTINCT argument lists used in aggregations must match");
                }
                else if (!groupByExpressions.isEmpty()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "DISTINCT with GROUP BY not supported");
                }
            }

            // ensure SELECT, ORDER BY and HAVING are constant with respect to group
            // e.g, these are all valid expressions:
            //     SELECT f(a) GROUP BY a
            //     SELECT f(a + 1) GROUP BY a + 1
            //     SELECT a + sum(b) GROUP BY a
            for (FieldOrExpression fieldOrExpression : Iterables.concat(outputExpressions, orderByExpressions)) {
                verifyAggregations(node, groupByExpressions, tupleDescriptor, fieldOrExpression);
            }

            if (node.getHaving().isPresent()) {
                verifyAggregations(node, groupByExpressions, tupleDescriptor, new FieldOrExpression(node.getHaving().get()));
            }
        }
    }

    private List<FunctionCall> extractAggregates(QuerySpecification node)
    {
        AggregateExtractor extractor = new AggregateExtractor(metadata);
        for (SelectItem item : node.getSelect().getSelectItems()) {
            if (item instanceof SingleColumn) {
                ((SingleColumn) item).getExpression().accept(extractor, null);
            }
        }

        for (SortItem item : node.getOrderBy()) {
            item.getSortKey().accept(extractor, null);
        }

        if (node.getHaving().isPresent()) {
            node.getHaving().get().accept(extractor, null);
        }

        List<FunctionCall> aggregates = extractor.getAggregates();
        analysis.setAggregates(node, aggregates);

        return aggregates;
    }

    private void verifyAggregations(QuerySpecification node, List<FieldOrExpression> groupByExpressions, TupleDescriptor tupleDescriptor, FieldOrExpression fieldOrExpression)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, metadata, tupleDescriptor);

        if (fieldOrExpression.isExpression()) {
            analyzer.analyze(fieldOrExpression.getExpression());
        }
        else {
            int fieldIndex = fieldOrExpression.getFieldIndex();
            if (!analyzer.analyze(fieldIndex)) {
                Field field = tupleDescriptor.getFields().get(fieldIndex);

                if (field.getRelationAlias().isPresent()) {
                    if (field.getName().isPresent()) {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Column '%s.%s' not in GROUP BY clause", field.getRelationAlias().get(), field.getName().get());
                    }
                    else {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Columns from '%s' not in GROUP BY clause", field.getRelationAlias().get());
                    }
                }
                else {
                    if (field.getName().isPresent()) {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Column '%s' not in GROUP BY clause", field.getName().get());
                    }
                    else {
                        throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Some columns from FROM clause not in GROUP BY clause");
                    }
                }
            }
        }
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
                extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
        {
            @Override
            protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableSet.Builder<QualifiedName> builder)
            {
                builder.add(node.getName());
                return null;
            }
        }
    }
}

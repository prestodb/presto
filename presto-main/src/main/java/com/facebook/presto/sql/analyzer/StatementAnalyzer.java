package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.sql.tree.AliasedExpression.aliasGetter;
import static com.facebook.presto.sql.tree.FunctionCall.distinctPredicate;
import static com.facebook.presto.sql.tree.QueryUtil.aliasedName;
import static com.facebook.presto.sql.tree.QueryUtil.ascending;
import static com.facebook.presto.sql.tree.QueryUtil.caseWhen;
import static com.facebook.presto.sql.tree.QueryUtil.equal;
import static com.facebook.presto.sql.tree.QueryUtil.functionCall;
import static com.facebook.presto.sql.tree.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.tree.QueryUtil.nameReference;
import static com.facebook.presto.sql.tree.QueryUtil.selectAll;
import static com.facebook.presto.sql.tree.QueryUtil.selectList;
import static com.facebook.presto.sql.tree.QueryUtil.table;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;

class StatementAnalyzer
        extends DefaultTraversalVisitor<TupleDescriptor, Void>
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;

    public StatementAnalyzer(Analysis analysis, Metadata metadata, Session session)
    {
        this.analysis = analysis;
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    protected TupleDescriptor visitShowTables(ShowTables showTables, Void context)
    {
        String catalogName = session.getCatalog();
        String schemaName = session.getSchema();

        QualifiedName schema = showTables.getSchema();
        if (schema != null) {
            List<String> parts = schema.getParts();
            if (parts.size() > 2) {
                throw new SemanticException(showTables, "too many parts in schema name: %s", schema);
            }
            if (parts.size() == 2) {
                catalogName = parts.get(0);
            }
            schemaName = schema.getSuffix();
        }

        // TODO: throw SemanticException if schema does not exist

        Expression predicate = equal(nameReference("table_schema"), new StringLiteral(schemaName));

        String likePattern = showTables.getLikePattern();
        if (likePattern != null) {
            Expression likePredicate = new LikePredicate(nameReference("table_name"), new StringLiteral(likePattern), null);
            predicate = logicalAnd(predicate, likePredicate);
        }

        Query query = new Query(
                selectList(aliasedName("table_name", "Table")),
                table(QualifiedName.of(catalogName, INFORMATION_SCHEMA, TABLE_TABLES)),
                Optional.of(predicate),
                ImmutableList.<Expression>of(),
                Optional.<Expression>absent(),
                ImmutableList.of(ascending("table_name")),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowColumns(ShowColumns showColumns, Void context)
    {
        QualifiedTableName tableName = MetadataUtil.createQualifiedTableName(session, showColumns.getTable());

        // TODO: throw SemanticException if table does not exist
        Query query = new Query(
                selectList(
                        aliasedName("column_name", "Column"),
                        aliasedName("data_type", "Type"),
                        aliasedName("is_nullable", "Null")),
                table(QualifiedName.of(tableName.getCatalogName(), INFORMATION_SCHEMA, TABLE_COLUMNS)),
                Optional.of(logicalAnd(
                        equal(nameReference("table_schema"), new StringLiteral(tableName.getSchemaName())),
                        equal(nameReference("table_name"), new StringLiteral(tableName.getTableName())))),
                ImmutableList.<Expression>of(),
                Optional.<Expression>absent(),
                ImmutableList.of(ascending("ordinal_position")),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowPartitions(ShowPartitions showPartitions, Void context)
    {
        QualifiedTableName table = MetadataUtil.createQualifiedTableName(session, showPartitions.getTable());

            /*
                Generate a dynamic pivot to output one column per partition key.
                For example, a table with two partition keys (ds, cluster_name)
                would generate the following query:

                SELECT
                  max(CASE WHEN partition_key = 'ds' THEN partition_value END) ds
                , max(CASE WHEN partition_key = 'cluster_name' THEN partition_value END) cluster_name
                FROM ...
                GROUP BY partition_number
                ORDER BY partition_number
            */

        ImmutableList.Builder<Expression> selectList = ImmutableList.builder();
        for (String partition : metadata.listTablePartitionKeys(table)) {
            Expression key = equal(nameReference("partition_key"), new StringLiteral(partition));
            Expression function = functionCall("max", caseWhen(key, nameReference("partition_value")));
            selectList.add(new AliasedExpression(function, partition));
        }

        // TODO: throw SemanticException if table does not exist
        Query query = new Query(
                selectAll(selectList.build()),
                table(QualifiedName.of(table.getCatalogName(), INFORMATION_SCHEMA, TABLE_INTERNAL_PARTITIONS)),
                Optional.of(logicalAnd(
                        equal(nameReference("table_schema"), new StringLiteral(table.getSchemaName())),
                        equal(nameReference("table_name"), new StringLiteral(table.getTableName())))),
                ImmutableList.of(nameReference("partition_number")),
                Optional.<Expression>absent(),
                ImmutableList.of(ascending("partition_number")),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowFunctions(ShowFunctions node, Void context)
    {
        Query query = new Query(
                selectList(
                        aliasedName("function_name", "Function"),
                        aliasedName("return_type", "Return Type"),
                        aliasedName("argument_types", "Argument Types")),
                table(QualifiedName.of(INFORMATION_SCHEMA, TABLE_INTERNAL_FUNCTIONS)),
                Optional.<Expression>absent(),
                ImmutableList.<Expression>of(),
                Optional.<Expression>absent(),
                ImmutableList.of(ascending("function_name")),
                Optional.<String>absent());

        return process(query, context);
    }


    @Override
    protected TupleDescriptor visitCreateMaterializedView(CreateMaterializedView node, Void context)
    {
        // Turn this into a query that has a new table writer node on top.
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());
        analysis.setDestination(targetTable);

        TableMetadata tableMetadata = metadata.getTable(targetTable);

        if (tableMetadata != null) {
            throw new SemanticException(node, "Destination table '%s' already exists", targetTable);
        }

        if (node.getRefresh().isPresent()) {
            int refreshInterval = Integer.parseInt(node.getRefresh().get());
            if (refreshInterval <= 0) {
                throw new SemanticException(node, "Refresh interval must be > 0 (was %s)", refreshInterval);
            }

            analysis.setRefreshInterval(Optional.of(refreshInterval));
        }
        else {
            analysis.setRefreshInterval(Optional.<Integer>absent());
        }

        // Analyze the query that creates the table...
        process(node.getTableDefinition(), context);

        return new TupleDescriptor(ImmutableList.of(new Field(analysis.getNextRelationId(), Optional.of("imported_rows"), Type.LONG, 0)));
    }

    @Override
    protected TupleDescriptor visitRefreshMaterializedView(RefreshMaterializedView node, Void context)
    {
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());

        TableMetadata tableMetadata = metadata.getTable(targetTable);
        if (tableMetadata == null) {
            throw new SemanticException(node, "Destination table '%s' does not exist", targetTable);
        }

        checkState(tableMetadata.getTableHandle().get().getDataSourceType() == DataSourceType.NATIVE, "Cannot import into non-native table %s", tableMetadata.getTable());
        analysis.setDestination(targetTable);
        analysis.setDoRefresh(true);

        return new TupleDescriptor(ImmutableList.of(new Field(analysis.getNextRelationId(), Optional.of("imported_rows"), Type.LONG, 0)));
    }

    @Override
    protected TupleDescriptor visitQuery(Query node, Void context)
    {
        // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
        // to pass down to analyzeFrom

        Scope queryScope = analyzeFrom(node);

        analyzeWhere(node, queryScope);

        List<FieldOrExpression> outputExpressions = analyzeSelect(node, queryScope);
        List<FieldOrExpression> groupByExpressions = analyzeGroupBy(node, queryScope, outputExpressions);
        List<FieldOrExpression> orderByExpressions = analyzeOrderBy(node, queryScope, outputExpressions);
        analyzeHaving(node, queryScope);

        analyzeAggregations(node, queryScope, groupByExpressions, outputExpressions, orderByExpressions);
        analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

        analysis.setQuery(node);
        return computeOutputDescriptor(node, queryScope);
    }


    private List<FunctionCall> extractAggregates(Query node)
    {
        AggregateExtractor extractor = new AggregateExtractor(metadata);
        for (Expression expression : node.getSelect().getSelectItems()) {
            expression.accept(extractor, null);
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

    private void analyzeWindowFunctions(Query node, List<FieldOrExpression> outputExpressions, List<FieldOrExpression> orderByExpressions)
    {
        WindowFunctionExtractor extractor = new WindowFunctionExtractor();

        for (FieldOrExpression fieldOrExpression : Iterables.concat(outputExpressions, orderByExpressions)) {
            if (fieldOrExpression.getExpression().isPresent()) {
                extractor.process(fieldOrExpression.getExpression().get(), null);
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
                throw new SemanticException(node, "Cannot nest window functions inside window function '%s': %s",
                        ExpressionFormatter.toString(windowFunction),
                        Iterables.transform(extractor.getWindowFunctions(), ExpressionFormatter.expressionFormatterFunction()));
            }

            if (windowFunction.isDistinct()) {
                throw new SemanticException(node, "DISTINCT in window function parameters not yet supported: %s", ExpressionFormatter.toString(windowFunction));
            }

            if (window.getFrame().isPresent()) {
                throw new SemanticException(node, "Window frames not yet supported");
            }

            List<TupleInfo.Type> argumentTypes = Lists.transform(windowFunction.getArguments(), new Function<Expression, TupleInfo.Type>()
            {
                @Override
                public TupleInfo.Type apply(Expression input)
                {
                    return analysis.getType(input).getRawType();
                }
            });

            FunctionInfo info = metadata.getFunction(windowFunction.getName(), argumentTypes);
            if (!info.isWindow()) {
                throw new SemanticException(node, "Not a window function: %s", windowFunction.getName());
            }
        }

        analysis.setWindowFunctions(node, windowFunctions);
    }

    private void analyzeHaving(Query node, Scope scope)
    {
        if (node.getHaving().isPresent()) {
            Expression predicate = node.getHaving().get();

            Type type = Analyzer.analyzeExpression(metadata, scope, analysis, predicate);

            if (type != Type.BOOLEAN && type != Type.NULL) {
                throw new SemanticException(predicate, "HAVING clause must evaluate to a boolean: actual type %s", type);
            }

            analysis.setHaving(node, predicate);
        }
    }

    private List<FieldOrExpression> analyzeOrderBy(Query node, Scope scope, List<FieldOrExpression> outputExpressions)
    {
        List<SortItem> items = node.getOrderBy();

        ImmutableList.Builder<FieldOrExpression> orderByExpressionsBuilder = ImmutableList.builder();

        if (!items.isEmpty()) {
            // Compute aliased output terms so we can resolve order by expressions against them first
            Multimap<String, AliasedExpression> byAlias = IterableTransformer.on(node.getSelect().getSelectItems())
                    .select(instanceOf(AliasedExpression.class))
                    .cast(AliasedExpression.class)
                    .index(aliasGetter());

            for (SortItem item : items) {
                Expression expression = item.getSortKey();

                FieldOrExpression orderByExpression = null;
                if (expression instanceof QualifiedNameReference && !((QualifiedNameReference) expression).getName().getPrefix().isPresent()) {
                    // if this is a simple name reference, try to resolve against output columns

                    QualifiedName name = ((QualifiedNameReference) expression).getName();
                    Collection<AliasedExpression> expressions = byAlias.get(name.getSuffix());
                    if (expressions.size() > 1) {
                        throw new SemanticException(expression, "'%s' in ORDER BY is ambiguous", name.getSuffix());
                    }
                    else if (expressions.size() == 1) {
                        orderByExpression = new FieldOrExpression(unalias(Iterables.getOnlyElement(expressions)));
                    }

                    // otherwise, couldn't resolve name against output aliases, so fall through...
                }
                else if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > outputExpressions.size()) {
                        throw new SemanticException(expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    orderByExpression = outputExpressions.get((int) (ordinal - 1));
                }

                // otherwise, just use the expression as is
                if (orderByExpression == null) {
                    orderByExpression = new FieldOrExpression(expression);
                }

                if (orderByExpression.getExpression().isPresent()) {
                    Analyzer.analyzeExpression(metadata, scope, analysis, orderByExpression.getExpression().get());
                }

                orderByExpressionsBuilder.add(orderByExpression);
            }
        }

        List<FieldOrExpression> orderByExpressions = orderByExpressionsBuilder.build();
        analysis.setOrderByExpressions(node, orderByExpressions);

        if (node.getSelect().isDistinct() && !outputExpressions.containsAll(orderByExpressions)) {
            throw new SemanticException(node.getSelect(), "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        }
        return orderByExpressions;
    }

    private List<FieldOrExpression> analyzeGroupBy(Query node, Scope scope, List<FieldOrExpression> outputExpressions)
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
                        throw new SemanticException(expression, "GROUP BY position %s is not in select list", ordinal);
                    }

                    groupByExpression = outputExpressions.get((int) (ordinal - 1));
                }
                else {
                    Analyzer.analyzeExpression(metadata, scope, analysis, expression);
                    groupByExpression = new FieldOrExpression(expression);
                }

                if (groupByExpression.getExpression().isPresent()) {
                    Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, groupByExpression.getExpression().get(), "GROUP BY");
                }

                groupByExpressionsBuilder.add(groupByExpression);
            }
        }

        List<FieldOrExpression> groupByExpressions = groupByExpressionsBuilder.build();
        analysis.setGroupByExpressions(node, groupByExpressions);
        return groupByExpressions;
    }

    private TupleDescriptor computeOutputDescriptor(Query node, Scope queryScope)
    {
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

        int relationId = analysis.getNextRelationId();

        int index = 0;
        for (Expression expression : node.getSelect().getSelectItems()) {
            if (expression instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) expression).getPrefix();
                List<TupleDescriptor> descriptors = queryScope.getDescriptorsMatching(starPrefix);

                for (TupleDescriptor descriptor : descriptors) {
                    for (Field field : descriptor.getFields()) {
                        outputFields.add(new Field(relationId, field.getName(), field.getType(), index++));
                    }
                }
            }
            else {
                Optional<String> alias = Optional.absent();
                if (expression instanceof AliasedExpression) {
                    AliasedExpression aliased = (AliasedExpression) expression;
                    alias = Optional.of(aliased.getAlias());
                }
                else if (expression instanceof QualifiedNameReference) {
                    alias = Optional.of(((QualifiedNameReference) expression).getName().getSuffix());
                }

                outputFields.add(new Field(relationId, alias, analysis.getType(expression), index++)); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
            }
        }

        TupleDescriptor result = new TupleDescriptor(outputFields.build());
        analysis.setOutputDescriptor(node, result);

        return result;
    }

    private List<FieldOrExpression> analyzeSelect(Query node, Scope scope)
    {
        ImmutableList.Builder<FieldOrExpression> outputExpressionBuilder = ImmutableList.builder();

        for (Expression expression : node.getSelect().getSelectItems()) {
            if (expression instanceof AllColumns) {
                // expand * and T.*
                Optional<QualifiedName> starPrefix = ((AllColumns) expression).getPrefix();

                List<TupleDescriptor> descriptors = scope.getDescriptorsMatching(starPrefix);
                if (descriptors.isEmpty()) {
                    throw new SemanticException(expression, "Table '%s' not found", starPrefix.get());
                }

                for (TupleDescriptor descriptor : descriptors) {
                    for (Field field : descriptor.getFields()) {
                        outputExpressionBuilder.add(new FieldOrExpression(field));
                    }
                }
            }
            else {
                Analyzer.analyzeExpression(metadata, scope, analysis, expression);
                outputExpressionBuilder.add(new FieldOrExpression(unalias(expression)));
            }
        }

        ImmutableList<FieldOrExpression> result = outputExpressionBuilder.build();
        analysis.setOutputExpressions(node, result);

        return result;
    }

    private void analyzeWhere(Query node, Scope scope)
    {
        if (node.getWhere().isPresent()) {
            Expression predicate = node.getWhere().get();

            Analyzer.verifyNoAggregatesOrWindowFunctions(metadata, predicate, "WHERE");

            Type type = Analyzer.analyzeExpression(metadata, scope, analysis, predicate);

            if (type != Type.BOOLEAN && type != Type.NULL) {
                throw new SemanticException(predicate, "WHERE clause must evaluate to a boolean: actual type %s", type);
            }

            analysis.setWhere(node, predicate);
        }
    }

    private Scope analyzeFrom(Query node)
    {
        ImmutableMultimap.Builder<Optional<QualifiedName>, TupleDescriptor> fromDescriptorBuilder = ImmutableMultimap.builder();

        TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata);
        for (Relation relation : node.getFrom()) {
            fromDescriptorBuilder.putAll(analyzer.process(relation, null));
        }

        // ensure each relation alias appears only once
        Multimap<Optional<QualifiedName>, TupleDescriptor> fromDescriptors = fromDescriptorBuilder.build();

        for (Optional<QualifiedName> alias : fromDescriptors.keys()) {
            if (alias.isPresent() && fromDescriptors.keys().count(alias) > 1) {
                throw new SemanticException(node, "Relation '%s' appears more than once", alias.get());
            }
        }

        return new Scope(fromDescriptors);
    }

    private void analyzeAggregations(Query node,
            Scope scope,
            List<FieldOrExpression> groupByExpressions,
            List<FieldOrExpression> outputExpressions,
            List<FieldOrExpression> orderByExpressions)
    {
        List<FunctionCall> aggregates = extractAggregates(node);

        // is this an aggregation query?
        if (!aggregates.isEmpty() || !groupByExpressions.isEmpty()) {
            if (Iterables.any(aggregates, distinctPredicate())) {
                throw new SemanticException(node, "DISTINCT in aggregation parameters not yet supported");
            }

            // ensure SELECT, ORDER BY and HAVING are constant with respect to group
            // e.g, these are all valid expressions:
            //     SELECT f(a) GROUP BY a
            //     SELECT f(a + 1) GROUP BY a + 1
            //     SELECT a + sum(b) GROUP BY a
            for (FieldOrExpression fieldOrExpression : Iterables.concat(outputExpressions, orderByExpressions)) {
                verifyAggregations(node, groupByExpressions, scope, fieldOrExpression);
            }

            if (node.getHaving().isPresent()) {
                verifyAggregations(node, groupByExpressions, scope, new FieldOrExpression(node.getHaving().get()));
            }
        }
    }

    private void verifyAggregations(Query query, List<FieldOrExpression> groupByExpressions, Scope scope, FieldOrExpression fieldOrExpression)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, metadata, scope);

        if (fieldOrExpression.getExpression().isPresent()) {
            analyzer.analyze(fieldOrExpression.getExpression().get());
        }
        else if (!analyzer.analyze(fieldOrExpression.getField().get())) {
            Field field = fieldOrExpression.getField().get();
            if (field.getName().isPresent()) {
                throw new SemanticException(query, "Column '%s.%s' not in GROUP BY clause", field.getRelationAlias().get(), field.getName().get());
            }
            else {
                throw new SemanticException(query, "Column %s from '%s' not in GROUP BY clause", field.getIndex() + 1, field.getRelationAlias().get());
            }
        }
    }

    private static Expression unalias(Expression expression)
    {
        if (expression instanceof AliasedExpression) {
            return ((AliasedExpression) expression).getExpression();
        }

        return expression;
    }
}

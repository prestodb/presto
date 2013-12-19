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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.connector.system.CatalogSystemTable.CATALOG_TABLE_NAME;
import static com.facebook.presto.sql.analyzer.Analyzer.ExpressionAnalysis;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_MATERIALIZED_VIEW_REFRESH_INTERVAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.tree.ExplainFormat.Type.TEXT;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.facebook.presto.sql.tree.QueryUtil.aliasedName;
import static com.facebook.presto.sql.tree.QueryUtil.ascending;
import static com.facebook.presto.sql.tree.QueryUtil.caseWhen;
import static com.facebook.presto.sql.tree.QueryUtil.equal;
import static com.facebook.presto.sql.tree.QueryUtil.functionCall;
import static com.facebook.presto.sql.tree.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.tree.QueryUtil.nameReference;
import static com.facebook.presto.sql.tree.QueryUtil.selectAll;
import static com.facebook.presto.sql.tree.QueryUtil.selectList;
import static com.facebook.presto.sql.tree.QueryUtil.subquery;
import static com.facebook.presto.sql.tree.QueryUtil.table;
import static com.facebook.presto.sql.tree.QueryUtil.unaliasedName;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

class StatementAnalyzer
        extends DefaultTraversalVisitor<TupleDescriptor, AnalysisContext>
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;

    public StatementAnalyzer(Analysis analysis, Metadata metadata, Session session, Optional<QueryExplainer> queryExplainer)
    {
        this.analysis = checkNotNull(analysis, "analysis is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.session = checkNotNull(session, "session is null");
        this.queryExplainer = checkNotNull(queryExplainer, "queryExplainer is null");
    }

    @Override
    protected TupleDescriptor visitShowTables(ShowTables showTables, AnalysisContext context)
    {
        String catalogName = session.getCatalog();
        String schemaName = session.getSchema();

        QualifiedName schema = showTables.getSchema();
        if (schema != null) {
            List<String> parts = schema.getParts();
            if (parts.size() > 2) {
                throw new SemanticException(INVALID_SCHEMA_NAME, showTables, "too many parts in schema name: %s", schema);
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
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(aliasedName("table_name", "Table")),
                        table(QualifiedName.of(catalogName, TABLE_TABLES.getSchemaName(), TABLE_TABLES.getTableName())),
                        Optional.of(predicate),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("table_name")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowSchemas(ShowSchemas node, AnalysisContext context)
    {
        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(aliasedName("schema_name", "Schema")),
                        table(QualifiedName.of(node.getCatalog().or(session.getCatalog()), TABLE_SCHEMATA.getSchemaName(), TABLE_SCHEMATA.getTableName())),
                        Optional.<Expression>absent(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("schema_name")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowCatalogs(ShowCatalogs node, AnalysisContext context)
    {
        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(aliasedName("catalog_name", "Catalog")),
                        table(QualifiedName.of(session.getCatalog(), CATALOG_TABLE_NAME.getSchemaName(), CATALOG_TABLE_NAME.getTableName())),
                        Optional.<Expression>absent(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("catalog_name")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowColumns(ShowColumns showColumns, AnalysisContext context)
    {
        QualifiedTableName tableName = MetadataUtil.createQualifiedTableName(session, showColumns.getTable());

        if (!metadata.getTableHandle(tableName).isPresent()) {
            throw new SemanticException(MISSING_TABLE, showColumns, "Table '%s' does not exist", tableName);
        }

        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(
                                aliasedName("column_name", "Column"),
                                aliasedName("data_type", "Type"),
                                aliasedYesNoToBoolean("is_nullable", "Null"),
                                aliasedYesNoToBoolean("is_partition_key", "Partition Key")),
                        table(QualifiedName.of(tableName.getCatalogName(), TABLE_COLUMNS.getSchemaName(), TABLE_COLUMNS.getTableName())),
                        Optional.of(logicalAnd(
                                equal(nameReference("table_schema"), new StringLiteral(tableName.getSchemaName())),
                                equal(nameReference("table_name"), new StringLiteral(tableName.getTableName())))),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("ordinal_position")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    private static SelectItem aliasedYesNoToBoolean(String column, String alias)
    {
        Expression expression = new IfExpression(
                equal(nameReference(column), new StringLiteral("YES")),
                BooleanLiteral.TRUE_LITERAL,
                BooleanLiteral.FALSE_LITERAL);
        return new SingleColumn(expression, alias);
    }

    @Override
    protected TupleDescriptor visitShowPartitions(ShowPartitions showPartitions, AnalysisContext context)
    {
        QualifiedTableName table = MetadataUtil.createQualifiedTableName(session, showPartitions.getTable());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(table);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, showPartitions, "Table '%s' does not exist", table);
        }

            /*
                Generate a dynamic pivot to output one column per partition key.
                For example, a table with two partition keys (ds, cluster_name)
                would generate the following query:

                SELECT
                  partition_number
                , max(CASE WHEN partition_key = 'ds' THEN partition_value END) ds
                , max(CASE WHEN partition_key = 'cluster_name' THEN partition_value END) cluster_name
                FROM ...
                GROUP BY partition_number

                The values are also cast to the type of the partition column.
                The query is then wrapped to allow custom filtering and ordering.
            */

        ImmutableList.Builder<SelectItem> selectList = ImmutableList.builder();
        ImmutableList.Builder<SelectItem> wrappedList = ImmutableList.builder();
        selectList.add(unaliasedName("partition_number"));
        for (ColumnMetadata column : metadata.getTableMetadata(tableHandle.get()).getColumns()) {
            if (!column.isPartitionKey()) {
                continue;
            }
            Expression key = equal(nameReference("partition_key"), new StringLiteral(column.getName()));
            Expression value = caseWhen(key, nameReference("partition_value"));
            value = new Cast(value, Type.fromRaw(column.getType()).getName());
            Expression function = functionCall("max", value);
            selectList.add(new SingleColumn(function, column.getName()));
            wrappedList.add(unaliasedName(column.getName()));
        }

        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectAll(selectList.build()),
                        table(QualifiedName.of(table.getCatalogName(), TABLE_INTERNAL_PARTITIONS.getSchemaName(), TABLE_INTERNAL_PARTITIONS.getTableName())),
                        Optional.of(logicalAnd(
                                equal(nameReference("table_schema"), new StringLiteral(table.getSchemaName())),
                                equal(nameReference("table_name"), new StringLiteral(table.getTableName())))),
                        ImmutableList.of(nameReference("partition_number")),
                        Optional.<Expression>absent(),
                        ImmutableList.<SortItem>of(),
                        Optional.<String>absent()),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectAll(wrappedList.build()),
                        subquery(query),
                        showPartitions.getWhere(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.<SortItem>builder()
                                .addAll(showPartitions.getOrderBy())
                                .add(ascending("partition_number"))
                                .build(),
                        showPartitions.getLimit()),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowFunctions(ShowFunctions node, AnalysisContext context)
    {
        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(
                                aliasedName("function_name", "Function"),
                                aliasedName("return_type", "Return Type"),
                                aliasedName("argument_types", "Argument Types"),
                                aliasedName("function_type", "Function Type"),
                                aliasedName("description", "Description")),
                        table(QualifiedName.of(TABLE_INTERNAL_FUNCTIONS.getSchemaName(), TABLE_INTERNAL_FUNCTIONS.getTableName())),
                        Optional.<Expression>absent(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(
                                ascending("function_name"),
                                ascending("return_type"),
                                ascending("argument_types"),
                                ascending("function_type")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitCreateTable(CreateTable node, AnalysisContext context)
    {
        // turn this into a query that has a new table writer node on top.
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());
        analysis.setCreateTableDestination(targetTable);

        Optional<TableHandle> targetTableHandle = metadata.getTableHandle(targetTable);
        if (targetTableHandle.isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
        }

        // analyze the query that creates the table
        TupleDescriptor descriptor = process(node.getQuery(), context);

        // verify that all column names are specified and unique
        // TODO: collect errors and return them all at once
        Set<String> names = new HashSet<>();
        for (int i = 0; i < descriptor.getFields().size(); i++) {
            Field field = descriptor.getFields().get(i);
            Optional<String> fieldName = field.getName();
            if (!fieldName.isPresent()) {
                throw new SemanticException(COLUMN_NAME_NOT_SPECIFIED, node, "Column name not specified at position %s", i + 1);
            }
            if (!names.add(fieldName.get())) {
                throw new SemanticException(DUPLICATE_COLUMN_NAME, node, "Column name '%s' specified more than once", fieldName.get());
            }
        }

        return new TupleDescriptor(Field.newUnqualified("rows", Type.BIGINT));
    }

    @Override
    protected TupleDescriptor visitCreateMaterializedView(CreateMaterializedView node, AnalysisContext context)
    {
        // Turn this into a query that has a new table writer node on top.
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());
        analysis.setMaterializedViewDestination(targetTable);

        Optional<TableHandle> targetTableHandle = metadata.getTableHandle(targetTable);
        if (targetTableHandle.isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
        }

        if (node.getRefresh().isPresent()) {
            int refreshInterval = Integer.parseInt(node.getRefresh().get());
            if (refreshInterval <= 0) {
                throw new SemanticException(INVALID_MATERIALIZED_VIEW_REFRESH_INTERVAL, node, "Refresh interval must be > 0 (was %s)", refreshInterval);
            }

            analysis.setRefreshInterval(Optional.of(refreshInterval));
        }
        else {
            analysis.setRefreshInterval(Optional.<Integer>absent());
        }

        // Analyze the query that creates the table...
        process(node.getTableDefinition(), context);

        return new TupleDescriptor(Field.newUnqualified("imported_rows", Type.BIGINT));
    }

    @Override
    protected TupleDescriptor visitRefreshMaterializedView(RefreshMaterializedView node, AnalysisContext context)
    {
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(targetTable);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, node, "Destination table '%s' does not exist", targetTable);
        }

        checkState(tableHandle.get() instanceof NativeTableHandle, "Cannot import into non-native table %s", targetTable);
        analysis.setMaterializedViewDestination(targetTable);
        analysis.setDoRefresh(true);

        return new TupleDescriptor(Field.newUnqualified("imported_rows", Type.BIGINT));
    }

    @Override
    protected TupleDescriptor visitExplain(Explain node, AnalysisContext context)
            throws SemanticException
    {
        checkState(queryExplainer.isPresent(), "query explainer not available");
        ExplainType.Type planType = LOGICAL;
        ExplainFormat.Type planFormat = TEXT;
        List<ExplainOption> options = node.getOptions();

        for (ExplainOption option : options) {
            if (option instanceof ExplainType) {
                planType = ((ExplainType) option).getType();
                break;
            }
        }

        for (ExplainOption option : options) {
            if (option instanceof ExplainFormat) {
                planFormat = ((ExplainFormat) option).getType();
                break;
            }
        }
        String queryPlan = getQueryPlan(node, planType, planFormat);

        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(
                                new SingleColumn(new StringLiteral(queryPlan), "Query Plan")),
                        null,
                        Optional.<Expression>absent(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.<SortItem>of(),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    private String getQueryPlan(Explain node, ExplainType.Type planType, ExplainFormat.Type planFormat)
            throws IllegalArgumentException
    {
        switch (planFormat) {
            case GRAPHVIZ:
                return queryExplainer.get().getGraphvizPlan(node.getStatement(), planType);
            case TEXT:
                return queryExplainer.get().getPlan(node.getStatement(), planType);
        }
        throw new IllegalArgumentException("Invalid Explain Format: " + planFormat.toString());
    }

    @Override
    protected TupleDescriptor visitQuery(Query node, AnalysisContext parentContext)
    {
        AnalysisContext context = new AnalysisContext(parentContext);

        analyzeWith(node, context);

        TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata);
        TupleDescriptor descriptor = analyzer.process(node.getQueryBody(), context);
        analyzeOrderBy(node, descriptor, context);

        // Input fields == Output fields
        analysis.setOutputDescriptor(node, descriptor);
        analysis.setOutputExpressions(node, descriptorToFields(descriptor));
        analysis.setQuery(node);

        return descriptor;
    }

    private List<FieldOrExpression> descriptorToFields(TupleDescriptor tupleDescriptor)
    {
        ImmutableList.Builder<FieldOrExpression> builder = ImmutableList.builder();
        for (int fieldIndex = 0; fieldIndex < tupleDescriptor.getFields().size(); fieldIndex++) {
            builder.add(new FieldOrExpression(fieldIndex));
        }
        return builder.build();
    }

    private void analyzeWith(Query node, AnalysisContext context)
    {
        // analyze WITH clause
        if (!node.getWith().isPresent()) {
            return;
        }

        With with = node.getWith().get();
        if (with.isRecursive()) {
            throw new SemanticException(NOT_SUPPORTED, with, "Recursive WITH queries are not supported");
        }

        for (WithQuery withQuery : with.getQueries()) {
            if (withQuery.getColumnNames() != null && !withQuery.getColumnNames().isEmpty()) {
                throw new SemanticException(NOT_SUPPORTED, withQuery, "Column alias not supported in WITH queries");
            }

            Query query = withQuery.getQuery();
            process(query, context);

            String name = withQuery.getName();
            if (context.isNamedQueryDeclared(name)) {
                throw new SemanticException(DUPLICATE_RELATION, withQuery, "WITH query name '%s' specified more than once", name);
            }

            context.addNamedQuery(name, query);
        }
    }

    private void analyzeOrderBy(Query node, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        List<SortItem> items = node.getOrderBy();

        ImmutableList.Builder<FieldOrExpression> orderByFieldsBuilder = ImmutableList.builder();

        if (!items.isEmpty()) {
            for (SortItem item : items) {
                Expression expression = item.getSortKey();

                FieldOrExpression orderByField;
                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > tupleDescriptor.getFields().size()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    orderByField = new FieldOrExpression((int) (ordinal - 1));
                }
                else {
                    // otherwise, just use the expression as is
                    orderByField = new FieldOrExpression(expression);
                    ExpressionAnalysis expressionAnalysis = Analyzer.analyzeExpression(session, metadata, tupleDescriptor, analysis, context, orderByField.getExpression());
                    analysis.addInPredicates(node, expressionAnalysis.getSubqueryInPredicates());
                }

                orderByFieldsBuilder.add(orderByField);
            }
        }

        analysis.setOrderByExpressions(node, orderByFieldsBuilder.build());
    }
}

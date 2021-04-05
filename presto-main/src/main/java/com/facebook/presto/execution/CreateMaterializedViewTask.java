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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.sql.NodeUtils.mapFromProperties;
import static com.facebook.presto.sql.SqlFormatterUtil.getFormattedSql;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MATERIALIZED_VIEW_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class CreateMaterializedViewTask
        implements DataDefinitionTask<CreateMaterializedView>
{
    private final SqlParser sqlParser;

    @Inject
    public CreateMaterializedViewTask(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return "CREATE MATERIALIZED VIEW";
    }

    @Override
    public ListenableFuture<?> execute(CreateMaterializedView statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getName());

        Optional<TableHandle> viewHandle = metadata.getTableHandle(session, viewName);
        if (viewHandle.isPresent()) {
            if (!statement.isNotExists()) {
                throw new SemanticException(MATERIALIZED_VIEW_ALREADY_EXISTS, statement, "Materialized view '%s' already exists", viewName);
            }
            return immediateFuture(null);
        }

        accessControl.checkCanCreateTable(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), viewName);
        accessControl.checkCanCreateView(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), viewName);

        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.empty(), parameters, stateMachine.getWarningCollector());
        Analysis analysis = analyzer.analyze(statement);

        ConnectorId connectorId = metadata.getCatalogHandle(session, viewName.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + viewName.getCatalogName()));
        List<ColumnMetadata> columnMetadata = analysis.getOutputDescriptor(statement.getQuery())
                .getVisibleFields().stream()
                .map(field -> new ColumnMetadata(field.getName().get(), field.getType()))
                .collect(toImmutableList());

        Map<String, Expression> sqlProperties = mapFromProperties(statement.getProperties());
        Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                connectorId,
                viewName.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                parameters);

        ConnectorTableMetadata viewMetadata = new ConnectorTableMetadata(
                toSchemaTableName(viewName),
                columnMetadata,
                properties,
                statement.getComment());

        String sql = getFormattedSql(statement.getQuery(), sqlParser, Optional.of(parameters));

        List<SchemaTableName> baseTables = analysis.getTableNodes().stream()
                .map(table -> {
                    QualifiedObjectName tableName = createQualifiedObjectName(session, table, table.getName());
                    if (!viewName.getCatalogName().equals(tableName.getCatalogName())) {
                        throw new SemanticException(
                                NOT_SUPPORTED,
                                statement,
                                "Materialized view %s created from a base table in a different catalog %s is not supported.",
                                viewName, tableName);
                    }
                    return toSchemaTableName(tableName);
                })
                .distinct()
                .collect(toImmutableList());

        ConnectorMaterializedViewDefinition viewDefinition = new ConnectorMaterializedViewDefinition(
                sql,
                viewName.getSchemaName(),
                viewName.getObjectName(),
                baseTables,
                Optional.of(session.getUser()),
                analysis.getOriginalColumnMapping(statement.getQuery()));
        try {
            metadata.createMaterializedView(session, viewName.getCatalogName(), viewMetadata, viewDefinition, statement.isNotExists());
        }
        catch (PrestoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }

        return immediateFuture(null);
    }

    @Override
    public String explain(CreateMaterializedView statement, List<Expression> parameters)
    {
        return "CREATE MATERIALIZED VIEW" + statement.getName();
    }

    private static void validateMaterializedView(CreateMaterializedView statement, Analysis analysis)
    {
        Query query = statement.getQuery();
        if (analysis.hasJoins()) {
            if (analysis.getJoinNodes().size() > 1) {
                throw new SemanticException(NOT_SUPPORTED, query, "More than one join in materialized view is not supported yet.");
            }
            analysis.getJoinNodes().forEach(joinNode -> validateJoin(joinNode, analysis));
        }
    }

    private static Map<String, Map<SchemaTableName, String>> getViewToBaseColumnMapping(Query query, Analysis analysis)
    {
        ImmutableList.Builder<List<Field>> joinColumns = ImmutableList.builder();
        analysis.getJoinNodes().forEach(joinNode -> new JoinAnalyzerVisitor(joinNode, analysis).process(joinNode, joinColumns));

        ImmutableMap.Builder<String, Map<SchemaTableName, String>> columnMapping = ImmutableMap.builder();

        for (Map.Entry<String, Map<SchemaTableName, String>> entry: analysis.getOriginalColumnMapping(query).entrySet()) {
            String viewColumn = entry.getKey();

            Map<SchemaTableName, String> baseColumns = new HashMap<>();
            baseColumns.putAll(entry.getValue());

            for (SchemaTableName baseTable : entry.getValue().keySet()) {
                String baseColumn = entry.getValue().get(baseTable);

                for (List<Field> joinColumnPair : joinColumns.build()) {
                    Optional<SchemaTableName> oneJoinBaseTable = joinColumnPair.get(0).getOriginTable().map(name -> toSchemaTableName(name));
                    Optional<String> oneJoinBaseColumn = joinColumnPair.get(0).getOriginColumnName();
                    Optional<SchemaTableName> otherJoinBaseTable = joinColumnPair.get(1).getOriginTable().map(name -> toSchemaTableName(name));
                    Optional<String> otherJoinBaseColumn = joinColumnPair.get(1).getOriginColumnName();
                    if (!oneJoinBaseTable.isPresent() || !oneJoinBaseColumn.isPresent() || !otherJoinBaseTable.isPresent() || !otherJoinBaseColumn.isPresent()) {
                        continue;
                    }

                    if (baseTable.equals(oneJoinBaseTable.get()) && baseColumn.equals(oneJoinBaseColumn.get())) {
                        baseColumns.put(otherJoinBaseTable.get(), otherJoinBaseColumn.get());
                    }
                    else if (baseTable.equals(otherJoinBaseTable.get()) && baseColumn.equals(otherJoinBaseColumn.get())) {
                        baseColumns.put(oneJoinBaseTable.get(), oneJoinBaseColumn.get());
                    }
                }
            }

            columnMapping.put(viewColumn, ImmutableMap.copyOf(baseColumns));
        }

        return columnMapping.build();
    }

    private static void validateJoin(Join joinNode, Analysis analysis)
    {
        new JoinAnalyzerVisitor(joinNode, analysis).process(joinNode, ImmutableList.builder());
    }

    private static class JoinAnalyzerVisitor extends DefaultTraversalVisitor<Void, ImmutableList.Builder<List<Field>>>
    {
        private final Join joinNode;
        private final Analysis analysis;

        public JoinAnalyzerVisitor(Join joinNode, Analysis analysis)
        {
            this.joinNode = joinNode;
            this.analysis = analysis;
        }

        @Override
        protected Void visitJoin(Join node, ImmutableList.Builder<List<Field>> builder)
        {
            if (!node.getType().equals(Join.Type.INNER)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only inner join is supported for materialized view.");
            }
            if (!node.getCriteria().isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Join with no criteria is not supported for materialized view.");
            }

            JoinCriteria joinCriteria = node.getCriteria().get();
            if (!(joinCriteria instanceof JoinOn)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only join-on is supported for materialized view.");
            }

            process(((JoinOn) joinCriteria).getExpression(), builder);
            return null;
        }

        @Override
        protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, ImmutableList.Builder<List<Field>> builder)
        {
            if (!node.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only AND operator is supported for join criteria for materialized view.");
            }
            return super.visitLogicalBinaryExpression(node, builder);
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, ImmutableList.Builder<List<Field>> builder)
        {
            if (!node.getOperator().equals(ComparisonExpression.Operator.EQUAL)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only EQUAL join is supported for materialized view.");
            }
            Field left = analysis.getScope(joinNode).tryResolveField(node.getLeft())
                    .orElseThrow(() -> new SemanticException(NOT_SUPPORTED, node.getLeft(), "%s in join criteria is not supported for materialized view.", node.getLeft().getClass().getSimpleName()))
                    .getField();
            Field right = analysis.getScope(joinNode).tryResolveField(node.getRight())
                    .orElseThrow(() -> new SemanticException(NOT_SUPPORTED, node.getRight(), "%s in join criteria is not supported for materialized view.", node.getRight().getClass().getSimpleName()))
                    .getField();
            builder.add(ImmutableList.of(left, right));
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, ImmutableList.Builder<List<Field>> builder)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "%s is not supported for materialized view.", node.getClass().getSimpleName());
        }

        private void processJoinUsingAnalysis(Analysis.JoinUsingAnalysis joinUsingAnalysis, ImmutableList.Builder<List<Field>> builder)
        {
            checkState(joinUsingAnalysis != null, "joinUsingAnalysis is missing.");
            checkState(joinUsingAnalysis.getLeftJoinFields().size() == joinUsingAnalysis.getRightJoinFields().size(), "leftJoinFields and rightJoinFields are of different size.");

            for (int i = 0; i < joinUsingAnalysis.getLeftJoinFields().size(); i++) {
                Field left = analysis.getOutputDescriptor(joinNode.getLeft()).getFieldByIndex(joinUsingAnalysis.getLeftJoinFields().get(i));
                Field right = analysis.getOutputDescriptor(joinNode.getRight()).getFieldByIndex(joinUsingAnalysis.getRightJoinFields().get(i));
                builder.add(ImmutableList.of(left, right));
            }
        }
    }
}

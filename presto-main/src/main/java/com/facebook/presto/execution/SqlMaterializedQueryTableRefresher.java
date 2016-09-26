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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.spi.ColumnIdentity;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.MaterializedQueryTableInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableIdentity;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionManager;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.server.StatementResource.Query.isQueryStarted;
import static com.facebook.presto.server.StatementResource.Query.updateExchangeClient;
import static com.facebook.presto.spi.StandardErrorCode.REFRESH_TABLE_FAILED_INTERNAL;
import static com.facebook.presto.spi.StandardErrorCode.REFRESH_TABLE_FAILED_USER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class SqlMaterializedQueryTableRefresher
        implements MaterializedQueryTableRefresher
{
    private final QueryManager queryManager;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final QueryIdGenerator queryIdGenerator;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final TransactionManager transactionManager;
    private static final Duration MAX_WAIT = new Duration(100, MILLISECONDS);

    @Inject
    public SqlMaterializedQueryTableRefresher(
            QueryManager queryManager,
            Metadata metadata,
            SqlParser sqlParser,
            QueryIdGenerator queryIdGenerator,
            ExchangeClientSupplier exchangeClientSupplier,
            TransactionManager transactionManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public void refreshMaterializedQueryTable(
            String materializedQueryTable,
            Map<String, String> predicateForBaseTables,
            String predicateForMaterializedQueryTable,
            ConnectorSession connectorSession)
            throws InterruptedException
    {
        requireNonNull(materializedQueryTable, "materializedQueryTable is null");
        checkArgument(!materializedQueryTable.trim().isEmpty(), "materializedQueryTable must not be empty string");

        Session session = Session.builder(metadata.getSessionPropertyManager())
                .setQueryId(new QueryId(connectorSession.getQueryId()))
                .setIdentity(connectorSession.getIdentity())
                .setSource("system")
                .setTimeZoneKey(connectorSession.getTimeZoneKey())
                .setLocale(connectorSession.getLocale())
                .setStartTime(connectorSession.getStartTime())
                .build();

        // TODO: Do this in a single transaction once we have a connector that supports transactions
        // For now, just use autocommit
        TransactionId transactionId = transactionManager.beginTransaction(true);
        session = session.withTransactionId(transactionId);

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, QualifiedObjectName.valueOf(materializedQueryTable));
        if (!tableHandle.isPresent()) {
            throw new PrestoException(REFRESH_TABLE_FAILED_USER, format("Cannot find materialized query table '%s'", materializedQueryTable));
        }

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get()).getMetadata();
        if (!tableMetadata.getMaterializedQueryTableInfo().isPresent()) {
            throw new PrestoException(REFRESH_TABLE_FAILED_USER, format("Table '%s' is not a materialized query table", materializedQueryTable));
        }

        MaterializedQueryTableInfo materializedQueryTableInfo = tableMetadata.getMaterializedQueryTableInfo().get();
        session = session.withCatalogAndSchema(tableHandle.get().getConnectorId().getCatalogName(), tableMetadata.getTable().getSchemaName());
        validateTableAndColumnIdentities(session, materializedQueryTableInfo);

        Query materializedQuery = (Query) sqlParser.createStatement(materializedQueryTableInfo.getQuery());
        QualifiedName materializedQueryTableName = DereferenceExpression.getQualifiedName((DereferenceExpression) sqlParser.createExpression(materializedQueryTable));

        Optional<Expression> changesToMaterializedQueryTable = Optional.empty();
        boolean skipDelete = false;
        if (predicateForMaterializedQueryTable != null && !predicateForMaterializedQueryTable.trim().isEmpty()) {
            Expression expression = sqlParser.createExpression(predicateForMaterializedQueryTable);
            if (BooleanLiteral.FALSE_LITERAL.equals(expression)) {
                skipDelete = true;
            }
            changesToMaterializedQueryTable = Optional.of(expression);
        }

        if (!skipDelete) {
            Delete delete = new Delete(new Table(materializedQueryTableName), changesToMaterializedQueryTable);
            QueryId deleteQueryId = queryIdGenerator.createNextQueryId();
            QueryInfo queryInfo = queryManager.createQuery(session, SqlFormatter.formatSql(delete, Optional.empty()), deleteQueryId);
            queryInfo = waitForQueryToFinish(queryInfo, deleteQueryId);
            if (queryInfo.getState() != FINISHED) {
                throw new PrestoException(REFRESH_TABLE_FAILED_INTERNAL, format("Failed to delete from materialized query table '%s'", materializedQueryTable));
            }
        }

        Expression refreshPredicateForBaseTables = parseBaseTablePredicates(predicateForBaseTables, session);
        if (!BooleanLiteral.TRUE_LITERAL.equals(refreshPredicateForBaseTables)) {
            QuerySpecification oldQuerySpecification = (QuerySpecification) materializedQuery.getQueryBody();
            Optional<Expression> originalPredicateToBaseTable = oldQuerySpecification.getWhere();

            if (originalPredicateToBaseTable.isPresent()) {
                refreshPredicateForBaseTables = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, originalPredicateToBaseTable.get(), refreshPredicateForBaseTables);
            }

            QueryBody newQuerySpecification = new QuerySpecification(
                    oldQuerySpecification.getLocation(),
                    oldQuerySpecification.getSelect(),
                    oldQuerySpecification.getFrom(),
                    Optional.of(refreshPredicateForBaseTables),
                    oldQuerySpecification.getGroupBy(),
                    oldQuerySpecification.getHaving(),
                    oldQuerySpecification.getOrderBy(),
                    oldQuerySpecification.getLimit());

            materializedQuery = new Query(
                    materializedQuery.getWith(),
                    newQuerySpecification,
                    materializedQuery.getOrderBy(),
                    materializedQuery.getLimit(),
                    materializedQuery.getApproximate());
        }

        Insert insert = new Insert(materializedQueryTableName, Optional.empty(), materializedQuery);
        QueryInfo insertQueryInfo = queryManager.createQuery(session, SqlFormatter.formatSql(insert, Optional.empty()), session.getQueryId());
        insertQueryInfo = waitForQueryToFinish(insertQueryInfo, session.getQueryId());

        if (insertQueryInfo.getState() != FINISHED) {
            throw new PrestoException(REFRESH_TABLE_FAILED_INTERNAL, format("Failed to insert into materialized query table '%s'", materializedQueryTable));
        }

        MaterializedQueryTableInfo updatedMaterializedQueryTableInfo = new MaterializedQueryTableInfo(
                materializedQueryTableInfo.getQuery(),
                materializedQueryTableInfo.getTableIdentities(),
                materializedQueryTableInfo.getColumnIdentities(),
                session.getStartTime());

        metadata.updateMaterializedQueryTableInfo(transactionId, tableHandle.get(), updatedMaterializedQueryTableInfo);
    }

    private void validateTableAndColumnIdentities(Session session, MaterializedQueryTableInfo materializedQueryTableInfo)
    {
        // validate all tables
        Map<String, byte[]> tableIdentities = materializedQueryTableInfo.getTableIdentities();
        for (Map.Entry<String, byte[]> tableIdentityEntry : tableIdentities.entrySet()) {
            String[] parts = tableIdentityEntry.getKey().split("\\.");
            checkArgument(parts != null && parts.length == 3, "Failed to parse table name %s", tableIdentityEntry.getKey());

            QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(parts[0], parts[1], parts[2]);
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);
            checkArgument(tableHandle.isPresent(), "Table %s does not exist any more", tableIdentityEntry.getKey());

            TableIdentity newTableIdentity = metadata.getTableIdentity(session, tableHandle.get());
            byte[] serializedIdentity = tableIdentityEntry.getValue();
            TableIdentity originalTableIdentity = metadata.deserializeTableIdentity(session, qualifiedObjectName.getCatalogName(), serializedIdentity);
            checkArgument(newTableIdentity.equals(originalTableIdentity), "TableIdentity changed for table %s", qualifiedObjectName);

            // validate columns
            Map<String, ColumnIdentity> originalColumnIdentities = materializedQueryTableInfo.getColumnIdentities().get(tableIdentityEntry.getKey()).entrySet().stream()
                    .collect(toMap(entry -> entry.getKey(), entry -> metadata.deserializeColumnIdentity(session, tableHandle.get().getConnectorId().getCatalogName(), entry.getValue())));
            Map<String, ColumnIdentity> currentColumnIdentities = metadata.getColumnHandles(session, tableHandle.get()).entrySet().stream()
                    .collect(toMap(entry -> entry.getKey(), entry -> metadata.getColumnIdentity(session, tableHandle.get(), entry.getValue())));

            for (Map.Entry<String, ColumnIdentity> originalColumnIdentityEntry : originalColumnIdentities.entrySet()) {
                String columnName = originalColumnIdentityEntry.getKey();
                checkArgument(currentColumnIdentities.containsKey(columnName), "Column %s in table %s no longer exists", columnName, qualifiedObjectName);
                checkArgument(currentColumnIdentities.get(columnName).equals(originalColumnIdentityEntry.getValue()));
            }
        }
    }

    private Expression parseBaseTablePredicates(Map<String, String> predicateForBaseTables, Session session)
    {
        if (predicateForBaseTables.isEmpty()) {
            return BooleanLiteral.TRUE_LITERAL;
        }

        Expression predicates = BooleanLiteral.TRUE_LITERAL;

        for (Map.Entry<String, String> entry : predicateForBaseTables.entrySet()) {
            Expression tableExpression = getTableExpression(entry.getKey(), session);

            Expression expression = sqlParser.createExpression(entry.getValue());
            if (BooleanLiteral.TRUE_LITERAL.equals(expression)) {
                throw new PrestoException(
                        REFRESH_TABLE_FAILED_USER,
                        format("Predicate '%s' for materialized query table '%s' should not be equivalent to True", entry.getValue(), tableExpression));
            }

            Expression rewrittenExpression = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    return new DereferenceExpression(tableExpression, node.getName().getSuffix());
                }

                @Override
                public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    return new DereferenceExpression(tableExpression, node.getFieldName());
                }
            }, expression);

            predicates = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, predicates, rewrittenExpression);
        }
        return predicates;
    }

    private DereferenceExpression getTableExpression(String tableName, Session session)
    {
        Expression tableExpression = sqlParser.createExpression(tableName);

        if (tableExpression instanceof QualifiedNameReference) {
            return new DereferenceExpression(
                    new DereferenceExpression(
                            new QualifiedNameReference(QualifiedName.of(session.getCatalog().get())), session.getSchema().get()),
                    tableName);
        }

        QualifiedName qualifiedName = DereferenceExpression.getQualifiedName((DereferenceExpression) tableExpression);
        if (qualifiedName.getParts().size() == 2) {
            return new DereferenceExpression(
                    new DereferenceExpression(
                            new QualifiedNameReference(QualifiedName.of(session.getCatalog().get())), qualifiedName.getParts().get(0)),
                    qualifiedName.getParts().get(1));
        }

        return (DereferenceExpression) tableExpression;
    }

    private QueryInfo waitForQueryToFinish(QueryInfo queryInfo, QueryId queryId)
            throws InterruptedException
    {
        ExchangeClient exchangeClient = exchangeClientSupplier.get(deltaMemoryInBytes -> {
        });
        // wait for it to start
        while (!isQueryStarted(queryInfo)) {
            queryManager.recordHeartbeat(queryId);
            queryManager.waitForStateChange(queryId, queryInfo.getState(), MAX_WAIT);
            queryInfo = queryManager.getQueryInfo(queryId);
        }

        while (!queryInfo.getState().isDone()) {
            queryManager.recordHeartbeat(queryId);
            updateExchangeClient(queryInfo.getOutputStage().get(), exchangeClient);
            exchangeClient.getNextPage(MAX_WAIT);
            queryManager.waitForStateChange(queryId, queryInfo.getState(), MAX_WAIT);
            queryInfo = queryManager.getQueryInfo(queryId);
        }
        return queryInfo;
    }
}

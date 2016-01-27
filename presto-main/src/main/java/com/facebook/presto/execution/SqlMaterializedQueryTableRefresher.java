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
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.transaction.TransactionManager;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.server.StatementResource.Query.isQueryStarted;
import static com.facebook.presto.server.StatementResource.Query.updateExchangeClient;
import static com.facebook.presto.spi.StandardErrorCode.REFRESH_TABLE_FAILED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
    private final Query dummyInsertQuery;
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
        this.dummyInsertQuery = (Query) sqlParser.createStatement("SElECT 1");
    }

    @Override
    public void refreshMaterializedQueryTable(String materializedQueryTable, ConnectorSession connectorSession)
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

        // TODO: Enable transaction
        //        TransactionId transactionId = transactionManager.beginTransaction(false);
        //        session = session.withTransactionId(transactionId);

        // materializedQueryTable is always the fully qualified name.
        QualifiedName materializedQueryTableName = DereferenceExpression.getQualifiedName((DereferenceExpression) sqlParser.createExpression(materializedQueryTable));
        Delete delete = new Delete(new Table(materializedQueryTableName), Optional.empty());
        QueryId deleteQueryId = queryIdGenerator.createNextQueryId();
        QueryInfo queryInfo = queryManager.createQuery(session, SqlFormatter.formatSql(delete), Optional.of(delete), deleteQueryId);
        queryInfo = waitForQueryToFinish(queryInfo, deleteQueryId);
        if (queryInfo.getState() != QueryState.FINISHED) {
            //            transactionManager.asyncAbort(transactionId);
            throw new PrestoException(REFRESH_TABLE_FAILED, String.format("Failed to delete from materialized query table %s", materializedQueryTable));
        }

        Insert insert = new Insert(materializedQueryTableName, Optional.empty(), dummyInsertQuery);
        queryInfo = queryManager.createQuery(session, SqlFormatter.formatSql(insert), Optional.of(insert), session.getQueryId());
        queryInfo = waitForQueryToFinish(queryInfo, session.getQueryId());

        if (queryInfo.getState() != QueryState.FINISHED) {
            //            transactionManager.asyncAbort(transactionId);
            throw new PrestoException(REFRESH_TABLE_FAILED, String.format("Failed to insert into materialized query table %s", materializedQueryTable));
        }

        //        transactionManager.asyncCommit(transactionId);
    }

    private QueryInfo waitForQueryToFinish(QueryInfo queryInfo, QueryId queryId)
            throws InterruptedException
    {
        ExchangeClient exchangeClient = exchangeClientSupplier.get(deltaMemoryInBytes -> { });
        // wait for it to start
        while (!isQueryStarted(queryInfo)) {
            queryManager.recordHeartbeat(queryId);
            queryManager.waitForStateChange(queryId, queryInfo.getState(), MAX_WAIT);
            queryInfo = queryManager.getQueryInfo(queryId);
        }

        while (!queryInfo.getState().isDone()) {
            queryManager.recordHeartbeat(queryId);
            updateExchangeClient(queryInfo.getOutputStage(), exchangeClient);
            exchangeClient.getNextPage(MAX_WAIT);
            queryManager.waitForStateChange(queryId, queryInfo.getState(), MAX_WAIT);
            queryInfo = queryManager.getQueryInfo(queryId);
        }
        return queryInfo;
    }
}

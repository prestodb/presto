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

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.google.common.base.Preconditions.checkNotNull;

public class DropTableExecution
        implements QueryExecution
{
    private final ConnectorSession session;
    private final DropTable statement;
    private final MetadataManager metadataManager;
    private final QueryStateMachine stateMachine;

    DropTableExecution(QueryId queryId,
            String query,
            ConnectorSession session,
            URI self,
            DropTable statement,
            MetadataManager metadataManager,
            Executor executor)
    {
        this.session = checkNotNull(session, "session is null");
        this.statement = statement;
        this.metadataManager = metadataManager;
        this.stateMachine = new QueryStateMachine(queryId, query, session, self, executor);
    }

    @Override
    public void start()
    {
        try {
            // transition to starting
            if (!stateMachine.starting()) {
                // query already started or finished
                return;
            }

            stateMachine.recordExecutionStart();

            dropTable();

            stateMachine.finished();
        }
        catch (RuntimeException e) {
            fail(e);
        }
    }

    @Override
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return stateMachine.waitForStateChange(currentState, maxWait);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void cancel()
    {
        stateMachine.cancel();
    }

    @Override
    public void fail(Throwable cause)
    {
        stateMachine.fail(cause);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return stateMachine.getQueryInfoWithoutDetails();
    }

    private void dropTable()
    {
        QualifiedTableName tableName = createQualifiedTableName(stateMachine.getSession(), statement.getTableName());

        Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }

        metadataManager.dropTable(tableHandle.get());
    }

    public static class DropTableExecutionFactory
            implements QueryExecutionFactory<DropTableExecution>
    {
        private final LocationFactory locationFactory;
        private final MetadataManager metadataManager;
        private final ExecutorService executor;

        @Inject
        DropTableExecutionFactory(LocationFactory locationFactory,
                MetadataManager metadataManager,
                ShardManager shardManager,
                @ForQueryExecution ExecutorService executor)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
            this.executor = checkNotNull(executor, "executor is null");
        }

        @Override
        public DropTableExecution createQueryExecution(QueryId queryId, String query, ConnectorSession session, Statement statement)
        {
            return new DropTableExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    (DropTable) statement,
                    metadataManager,
                    executor);
        }
    }
}

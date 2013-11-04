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
import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableAlias;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.DropAlias;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DropAliasExecution
        implements QueryExecution
{
    private final DropAlias statement;
    private final MetadataManager metadataManager;
    private final AliasDao aliasDao;
    private final QueryStateMachine stateMachine;

    DropAliasExecution(QueryId queryId,
            String query,
            Session session,
            URI self,
            DropAlias statement,
            MetadataManager metadataManager,
            AliasDao aliasDao,
            Executor executor)
    {
        this.statement = statement;
        this.metadataManager = metadataManager;
        this.aliasDao = aliasDao;
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

            dropAlias();

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

    private void dropAlias()
    {
        QualifiedTableName remoteTableName = createQualifiedTableName(stateMachine.getSession(), statement.getRemote());

        Optional<TableHandle> remoteTableHandle = metadataManager.getTableHandle(remoteTableName);
        checkState(remoteTableHandle.isPresent(), "Table %s does not exist", remoteTableName);
        String remoteConnectorId = metadataManager.getConnectorId(remoteTableHandle.get());

        TableAlias tableAlias = aliasDao.getAlias(remoteConnectorId, remoteTableName.getSchemaName(), remoteTableName.getTableName());

        checkState(tableAlias != null, "Table %s has no alias assigned", remoteTableName);

        aliasDao.dropAlias(tableAlias);

        stateMachine.finished();
    }

    public static class DropAliasExecutionFactory
            implements QueryExecutionFactory<DropAliasExecution>
    {
        private final LocationFactory locationFactory;
        private final MetadataManager metadataManager;
        private final AliasDao aliasDao;
        private final ExecutorService executor;
        private final ThreadPoolExecutorMBean executorMBean;

        @Inject
        DropAliasExecutionFactory(LocationFactory locationFactory, MetadataManager metadataManager, AliasDao aliasDao)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
            this.aliasDao = checkNotNull(aliasDao, "aliasDao is null");
            this.executor = Executors.newCachedThreadPool(daemonThreadsNamed("drop-alias-scheduler-%d"));
            this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
        }

        @Managed
        @Nested
        public ThreadPoolExecutorMBean getExecutor()
        {
            return executorMBean;
        }

        @Override
        public DropAliasExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement)
        {
            return new DropAliasExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    (DropAlias) statement,
                    metadataManager,
                    aliasDao,
                    executor);
        }
    }
}

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
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DropAliasExecution
        implements QueryExecution
{
    private final DropAlias statement;
    private final MetadataManager metadataManager;
    private final AliasDao aliasDao;
    private final Sitevars sitevars;

    private final QueryStateMachine stateMachine;

    DropAliasExecution(QueryId queryId,
            String query,
            Session session,
            URI self,
            DropAlias statement,
            MetadataManager metadataManager,
            AliasDao aliasDao,
            Sitevars sitevars,
            Executor executor)
    {
        this.statement = statement;
        this.sitevars = sitevars;
        this.metadataManager = metadataManager;
        this.aliasDao = aliasDao;

        this.stateMachine = new QueryStateMachine(queryId, query, session, self, executor);
    }

    public void start()
    {
        try {
            checkState(sitevars.isAliasEnabled(), "table aliasing is disabled");

            // transition to starting
            if (!stateMachine.starting()) {
                // query already started or finished
                return;
            }

            stateMachine.getStats().recordExecutionStart();

            dropAlias();

            stateMachine.finished();
        }
        catch (Exception e) {
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
        checkState(!remoteTableHandle.isPresent(), "Table %s does not exist", remoteTableName);

        TableAlias tableAlias = aliasDao.getAlias(remoteTableName);

        checkState(tableAlias != null, "Table %s is not aliased", remoteTableName);

        aliasDao.dropAlias(tableAlias);

        stateMachine.finished();
    }

    public static class DropAliasExecutionFactory
            implements QueryExecutionFactory<DropAliasExecution>
    {
        private final LocationFactory locationFactory;
        private final MetadataManager metadataManager;
        private final AliasDao aliasDao;
        private final Sitevars sitevars;
        private final ExecutorService executor;

        @Inject
        DropAliasExecutionFactory(LocationFactory locationFactory,
                MetadataManager metadataManager,
                AliasDao aliasDao,
                Sitevars sitevars)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
            this.aliasDao = checkNotNull(aliasDao, "aliasDao is null");
            this.sitevars = checkNotNull(sitevars, "sitevars is null");
            this.executor = Executors.newCachedThreadPool(daemonThreadsNamed("drop-alias-scheduler-%d"));
        }

        public DropAliasExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement)
        {
            return new DropAliasExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    (DropAlias) statement,
                    metadataManager,
                    aliasDao,
                    sitevars,
                    executor);
        }
    }
}

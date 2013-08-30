package com.facebook.presto.execution;

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableAlias;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.CreateAlias;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class CreateAliasExecution
        implements QueryExecution
{
    private final CreateAlias statement;
    private final MetadataManager metadataManager;
    private final AliasDao aliasDao;
    private final Sitevars sitevars;

    private final QueryStateMachine stateMachine;

    CreateAliasExecution(QueryId queryId,
            String query,
            Session session,
            URI self,
            CreateAlias statement,
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

            createAlias();

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

    private void createAlias()
    {
        QualifiedTableName aliasTableName = createQualifiedTableName(stateMachine.getSession(), statement.getAlias());

        Optional<TableHandle> aliasTableHandle = metadataManager.getTableHandle(aliasTableName);
        checkState(aliasTableHandle.isPresent(), "Table %s does not exist", aliasTableHandle);
        Preconditions.checkState(aliasTableHandle.get() instanceof NativeTableHandle, "Can only use a native table as alias");
        Optional<String> aliasConnectorId = metadataManager.getConnectorId(aliasTableHandle.get());
        Preconditions.checkArgument(aliasConnectorId.isPresent(), "Table %s can not be aliased", aliasTableName);

        QualifiedTableName remoteTableName = createQualifiedTableName(stateMachine.getSession(), statement.getRemote());

        Optional<TableHandle> remoteTableHandle = metadataManager.getTableHandle(remoteTableName);
        checkState(remoteTableHandle.isPresent(), "Table %s does not exist", remoteTableName);
        Optional<String> remoteConnectorId = metadataManager.getConnectorId(remoteTableHandle.get());
        Preconditions.checkArgument(remoteConnectorId.isPresent(), "Table %s can not be aliased", remoteTableName);

        TableAlias tableAlias = new TableAlias(remoteConnectorId.get(),
                remoteTableName.getSchemaName(),
                remoteTableName.getTableName(),
                aliasConnectorId.get(),
                aliasTableName.getSchemaName(),
                aliasTableName.getTableName());

        aliasDao.insertAlias(tableAlias);

        stateMachine.finished();
    }

    public static class CreateAliasExecutionFactory
            implements QueryExecutionFactory<CreateAliasExecution>
    {
        private final LocationFactory locationFactory;
        private final MetadataManager metadataManager;
        private final AliasDao aliasDao;
        private final Sitevars sitevars;
        private final ExecutorService executor;
        private final ThreadPoolExecutorMBean executorMBean;

        @Inject
        CreateAliasExecutionFactory(LocationFactory locationFactory,
                MetadataManager metadataManager,
                AliasDao aliasDao,
                Sitevars sitevars)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
            this.aliasDao = checkNotNull(aliasDao, "aliasDao is null");
            this.sitevars = checkNotNull(sitevars, "sitevars is null");
            this.executor = Executors.newCachedThreadPool(daemonThreadsNamed("alias-scheduler-%d"));
            this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
        }

        @Managed
        @Nested
        public ThreadPoolExecutorMBean getExecutor()
        {
            return executorMBean;
        }

        public CreateAliasExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement)
        {
            return new CreateAliasExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    (CreateAlias) statement,
                    metadataManager,
                    aliasDao,
                    sitevars,
                    executor);
        }
    }
}

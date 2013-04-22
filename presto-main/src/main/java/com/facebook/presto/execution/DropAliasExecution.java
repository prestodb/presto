package com.facebook.presto.execution;

import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.ImportTableHandle;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableAlias;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.DropAlias;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.inject.Inject;
import java.net.URI;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
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
            Sitevars sitevars)
    {
        this.statement = statement;
        this.sitevars = sitevars;
        this.metadataManager = metadataManager;
        this.aliasDao = aliasDao;

        this.stateMachine = new QueryStateMachine(queryId, query, session, self);
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
    public void addListener(Runnable listener)
    {
        stateMachine.addListener(listener);
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
    public void updateState(boolean forceUpdate)
    {
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
        checkState(!remoteTableHandle.isPresent(), "Table %s does not exists", remoteTableName);

        Preconditions.checkState(remoteTableHandle.get() instanceof ImportTableHandle, "Only import tables can be aliased");

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
                    sitevars);
        }
    }
}

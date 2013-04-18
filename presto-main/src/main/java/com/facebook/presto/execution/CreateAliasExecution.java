package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.metadata.AliasDao;
import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableAlias;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.CreateAlias;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Preconditions;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
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
            QueryMonitor queryMonitor,
            Sitevars sitevars)
    {
        this.statement = statement;
        this.sitevars = sitevars;
        this.metadataManager = metadataManager;
        this.aliasDao = aliasDao;

        this.stateMachine = new QueryStateMachine(queryId, query, session, self, queryMonitor);
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
        return stateMachine.getQueryInfo();
    }

    private void createAlias()
    {
        QualifiedTableName aliasTableName = createQualifiedTableName(stateMachine.getSession(), statement.getAlias());

        TableMetadata aliasTableMetadata = metadataManager.getTable(aliasTableName);
        Preconditions.checkState(aliasTableMetadata != null, "Table %s does not exist", aliasTableName);
        Preconditions.checkState(aliasTableMetadata.getTableHandle().isPresent(), "No table handle for %s", aliasTableName);
        TableHandle aliasTableHandle = aliasTableMetadata.getTableHandle().get();
        Preconditions.checkState(DataSourceType.NATIVE == aliasTableHandle.getDataSourceType(), "Can only use a native table as alias");

        QualifiedTableName remoteTableName = createQualifiedTableName(stateMachine.getSession(), statement.getRemote());

        TableMetadata remoteTableMetadata = metadataManager.getTable(remoteTableName);
        Preconditions.checkState(remoteTableMetadata != null, "Table %s does not exist", remoteTableName);
        Preconditions.checkState(remoteTableMetadata.getTableHandle().isPresent(), "No table handle for %s", remoteTableName);
        TableHandle remoteTableHandle = remoteTableMetadata.getTableHandle().get();
        Preconditions.checkState(DataSourceType.IMPORT == remoteTableHandle.getDataSourceType(), "Can only alias an import table");

        TableAlias tableAlias = TableAlias.createTableAlias(remoteTableName, aliasTableName);
        aliasDao.insertAlias(tableAlias);

        stateMachine.finished();
    }

    public static class CreateAliasExecutionFactory
            implements QueryExecutionFactory<CreateAliasExecution>
    {
        private final LocationFactory locationFactory;
        private final MetadataManager metadataManager;
        private final AliasDao aliasDao;
        private final QueryMonitor queryMonitor;
        private final Sitevars sitevars;

        @Inject
        CreateAliasExecutionFactory(LocationFactory locationFactory,
                MetadataManager metadataManager,
                QueryMonitor queryMonitor,
                AliasDao aliasDao,
                Sitevars sitevars)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
            this.aliasDao = checkNotNull(aliasDao, "aliasDao is null");
            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
            this.sitevars = checkNotNull(sitevars, "sitevars is null");
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
                    queryMonitor,
                    sitevars);
        }
    }
}

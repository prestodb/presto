package com.facebook.presto.execution;

import com.facebook.presto.metadata.TablePartition;

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.importer.PersistentPeriodicImportJob;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.StorageManager;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedTableName;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DropTableExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(DropTableExecution.class);

    private final DropTable statement;
    private final MetadataManager metadataManager;
    private final StorageManager storageManager;
    private final ShardManager shardManager;
    private final PeriodicImportManager periodicImportManager;
    private final Sitevars sitevars;

    private final QueryStateMachine stateMachine;

    DropTableExecution(QueryId queryId,
            String query,
            Session session,
            URI self,
            DropTable statement,
            MetadataManager metadataManager,
            StorageManager storageManager,
            ShardManager shardManager,
            PeriodicImportManager periodicImportManager,
            Sitevars sitevars,
            Executor executor)
    {
        this.statement = statement;
        this.sitevars = sitevars;
        this.metadataManager = metadataManager;
        this.storageManager = storageManager;
        this.shardManager = shardManager;
        this.periodicImportManager = periodicImportManager;

        this.stateMachine = new QueryStateMachine(queryId, query, session, self, executor);
    }

    public void start()
    {
        try {
            checkState(sitevars.isDropEnabled(), "table dropping is disabled");

            // transition to starting
            if (!stateMachine.starting()) {
                // query already started or finished
                return;
            }

            stateMachine.getStats().recordExecutionStart();

            dropTable();

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

    private void dropTable()
    {
        QualifiedTableName tableName = createQualifiedTableName(stateMachine.getSession(), statement.getTableName());

        log.debug("Dropping %s", tableName);

        final Optional<TableHandle> tableHandle = metadataManager.getTableHandle(tableName);
        checkState(tableHandle.isPresent(), "Table %s does not exist", tableName);
        Preconditions.checkState(tableHandle.get() instanceof NativeTableHandle, "Can drop only native tables");

        storageManager.dropTableSource((NativeTableHandle) tableHandle.get());

        periodicImportManager.dropJobs(new Predicate<PersistentPeriodicImportJob>() {
            @Override
            public boolean apply(PersistentPeriodicImportJob job)
            {
                return job.getDstTable().equals(tableHandle.get());
            }
        });

        Set<TablePartition> partitions = shardManager.getPartitions(tableHandle.get());
        for (TablePartition partition : partitions) {
            shardManager.dropPartition(tableHandle.get(), partition.getPartitionName());
        }

        metadataManager.dropTable(tableHandle.get());

        stateMachine.finished();
    }

    public static class DropTableExecutionFactory
            implements QueryExecutionFactory<DropTableExecution>
    {
        private final LocationFactory locationFactory;
        private final MetadataManager metadataManager;
        private final StorageManager storageManager;
        private final ShardManager shardManager;
        private final PeriodicImportManager periodicImportManager;
        private final Sitevars sitevars;
        private final ExecutorService executor;

        @Inject
        DropTableExecutionFactory(LocationFactory locationFactory,
                MetadataManager metadataManager,
                StorageManager storageManager,
                ShardManager shardManager,
                PeriodicImportManager periodicImportManager,
                Sitevars sitevars)
        {
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
            this.storageManager = checkNotNull(storageManager, "storageManager is null");
            this.shardManager = checkNotNull(shardManager, "shardManager is null");
            this.periodicImportManager = checkNotNull(periodicImportManager, "periodicImportManager is null");
            this.sitevars = checkNotNull(sitevars, "sitevars is null");
            this.executor = Executors.newCachedThreadPool(daemonThreadsNamed("drop-table-scheduler-%d"));
        }

        public DropTableExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement)
        {
            return new DropTableExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    (DropTable) statement,
                    metadataManager,
                    storageManager,
                    shardManager,
                    periodicImportManager,
                    sitevars,
                    executor);
        }
    }
}

package com.facebook.presto.importer;

import com.facebook.presto.execution.CreateOrReplaceMaterializedViewExecution;
import com.facebook.presto.importer.JobStateFactory.JobState;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PeriodicImportRunnable
        extends AbstractPeriodicImportRunnable
{
    private static final Logger log = Logger.get(PeriodicImportRunnable.class);

    private final ImportManager importManager;
    private final Metadata metadata;

    @Inject
    public PeriodicImportRunnable(ImportManager importManager,
            PeriodicImportManager periodicImportManager,
            Metadata metadata,
            JobState jobState)
    {
        super(jobState, periodicImportManager);

        this.importManager = importManager;
        this.metadata = metadata;
    }

    @Override
    public void doRun()
            throws Exception
    {
        final PersistentPeriodicImportJob job = jobState.getJob();

        TableMetadata sourceTable = metadata.getTable(job.getSrcTable());
        List<ColumnMetadata> sourceColumns = sourceTable.getColumns();

        TableMetadata table = new TableMetadata(job.getDstTable(), sourceColumns);
        metadata.createTable(table);

        table = metadata.getTable(job.getDstTable());
        long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
        List<ImportField> fields = CreateOrReplaceMaterializedViewExecution.getImportFields(sourceColumns, table.getColumns());

        ListenableFuture<?> importFuture = importManager.importTable(tableId, job.getSrcCatalogName(), job.getSrcSchemaName(), job.getSrcTableName(), fields);

        long maxRuntime = (long) (0.7 * job.getInterval());
        try {
            importFuture.get(maxRuntime, TimeUnit.SECONDS); // may take at most 70% of the import interval
        }
        catch (TimeoutException e) {
            log.warn(e, "Job %d: Import did not finish within %ds", job.getJobId(), maxRuntime);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (CancellationException e) {
            log.warn("Job %d was cancelled.", job.getJobId());
        }
        catch (ExecutionException e) {
            log.warn(e.getCause(), "Job %d:", job.getJobId());
        }
        finally {
            // in any case, cancel the future. This will trigger if e.g. runtime exceptions get thrown.
            // For a job that completed normally, cancelling it after the fact has no effect.
            importFuture.cancel(true);
        }
    }

    @Singleton
    public static final class PeriodicImportRunnableFactory
    {
        private final ImportManager importManager;
        private final PeriodicImportManager periodicImportManager;
        private final Metadata metadata;

        @Inject
        public PeriodicImportRunnableFactory(ImportManager importManager,
                PeriodicImportManager periodicImportManager,
                Metadata metadata)
        {
            this.importManager = importManager;
            this.periodicImportManager = periodicImportManager;
            this.metadata = metadata;
        }

        public Runnable create(JobState jobState)
        {
            return new PeriodicImportRunnable(importManager, periodicImportManager, metadata, jobState);
        }
    }
}

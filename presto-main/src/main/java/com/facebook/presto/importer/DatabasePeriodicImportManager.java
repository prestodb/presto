package com.facebook.presto.importer;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.importer.PeriodicImportDao.Utils.createTables;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatabasePeriodicImportManager
        implements PeriodicImportManager
{
    private static final Logger log = Logger.get(PeriodicImportManager.class);

    private static final Duration TABLE_RETRY_INTERVAL = new Duration(10, TimeUnit.SECONDS);

    private final PeriodicImportDao dao;
    private final NodeInfo nodeInfo;

    @Inject
    public DatabasePeriodicImportManager(@ForPeriodicImport IDBI importDbi, NodeInfo nodeInfo)
            throws InterruptedException
    {
        checkNotNull(importDbi, "Dbi was null!");
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo was null!");

        this.dao = importDbi.onDemand(PeriodicImportDao.class);

        createTablesWithRetry();
    }

    public long insertJob(PeriodicImportJob job)
    {
        return dao.insertJob(job);
    }

    public void dropJob(long jobId)
    {
        dao.dropJob(jobId);
    }

    public long getJobCount()
    {
        return dao.getJobCount(true);
    }

    public PersistentPeriodicImportJob getJob(long jobId)
    {
        return dao.getJob(jobId);
    }

    public List<PersistentPeriodicImportJob> getJobs()
    {
        return dao.getJobs(true);
    }

    public long beginRun(long jobId)
    {
        return dao.beginRun(jobId, nodeInfo.getNodeId());
    }

    public void endRun(long runId, boolean result)
    {
        dao.finishRun(runId, result);
    }

    private void createTablesWithRetry()
            throws InterruptedException
    {
        while (true) {
            try {
                createTables(dao);
                return;
            }
            catch (UnableToObtainConnectionException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", TABLE_RETRY_INTERVAL, e.getMessage());
                Thread.sleep((long) TABLE_RETRY_INTERVAL.toMillis());
            }
        }
    }
}

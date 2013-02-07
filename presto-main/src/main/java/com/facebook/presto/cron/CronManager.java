package com.facebook.presto.cron;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Singleton
public class CronManager
{
    private static final Logger log = Logger.get(CronManager.class);

    private final CronDao dao;

    @Inject
    public CronManager(@ForCron IDBI schedulerDbi)
            throws InterruptedException
    {
        this.dao = schedulerDbi.onDemand(CronDao.class);

        createTablesWithRetry();
    }

    public long insertJob(CronJob job)
    {
        return dao.insertJob(job);
    }

    public void dropJob(long jobId)
    {
        dao.dropJob(jobId);
    }

    public long getJobCount()
    {
        return dao.getJobCount();
    }

    public CronJob getJob(long jobId)
    {
        return dao.getJob(jobId);
    }

    public List<PersistentCronJob> getJobs()
    {
        return dao.getJobs();
    }

    private void createTablesWithRetry()
            throws InterruptedException
    {
        Duration delay = new Duration(10, TimeUnit.SECONDS);
        while (true) {
            try {
                createTables();
                return;
            }
            catch (UnableToObtainConnectionException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                Thread.sleep((long) delay.toMillis());
            }
        }
    }

    private void createTables()
    {
        dao.createJobsTable();
    }
}



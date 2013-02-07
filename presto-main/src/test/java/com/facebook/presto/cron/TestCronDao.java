package com.facebook.presto.cron;

import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestCronDao
{
    CronDao dao;
    Handle handle;

    @BeforeMethod
    public void setup() throws Exception
    {
        H2EmbeddedDataSourceConfig dataSourceConfig = new H2EmbeddedDataSourceConfig().setFilename("mem:");
        DataSource dataSource = new H2EmbeddedDataSource(dataSourceConfig);
        DBI h2Dbi = new DBI(dataSource);
        handle = h2Dbi.open();
        dao = handle.attach(CronDao.class);
    }

    @AfterMethod
    public void teardown()
    {
        handle.close();
    }

    @Test
    public void testTableCreation()
    {
        dao.createJobsTable();

        assertEquals(dao.getJobCount(), 0);
    }

    @Test
    public void testJobCreation()
    {
        dao.createJobsTable();
        assertEquals(dao.getJobCount(), 0);

        CronJob dummyJob = new CronJob("ss", "sc", "st", "ds", "dc", "dt", 20);
        long jobId = dao.insertJob(dummyJob);
        assertEquals(jobId, 1);

        assertEquals(dao.getJobCount(), 1);

        PersistentCronJob storedJob = dao.getJob(jobId);
        assertCronjobsEqual(storedJob, dummyJob);
    }

    @Test
    public void testJobDeletion()
    {
        dao.createJobsTable();
        assertEquals(dao.getJobCount(), 0);

        CronJob dummyJob = new CronJob("ss", "sc", "st", "ds", "dc", "dt", 20);
        long jobId = dao.insertJob(dummyJob);
        assertEquals(jobId, 1);
        assertEquals(dao.getJobCount(), 1);

        dao.dropJob(jobId);
        assertEquals(dao.getJobCount(), 0);
    }

    private void assertCronjobsEqual(CronJob job1, CronJob job2)
    {
        assertNotNull(job1, "job1 was null");
        assertNotNull(job2, "job2 was null");
        assertEquals(job1.getSrcCatalogName(), job2.getSrcCatalogName(), "srcCatalogName");
        assertEquals(job1.getSrcSchemaName(), job2.getSrcSchemaName(), "srcCatalogName");
        assertEquals(job1.getSrcTableName(), job2.getSrcTableName(), "srcCatalogName");
        assertEquals(job1.getDstCatalogName(), job2.getDstCatalogName(), "srcCatalogName");
        assertEquals(job1.getDstSchemaName(), job2.getDstSchemaName(), "srcCatalogName");
        assertEquals(job1.getDstTableName(), job2.getDstTableName(), "srcCatalogName");
    }
}

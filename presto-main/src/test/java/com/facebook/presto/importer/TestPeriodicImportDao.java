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
package com.facebook.presto.importer;

import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import java.util.UUID;

import static com.facebook.presto.importer.PeriodicImportDao.Utils.createTables;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPeriodicImportDao
{
    PeriodicImportDao dao;
    Handle handle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        H2EmbeddedDataSourceConfig dataSourceConfig = new H2EmbeddedDataSourceConfig().setFilename("mem:");
        DataSource dataSource = new H2EmbeddedDataSource(dataSourceConfig);
        DBI h2Dbi = new DBI(dataSource);
        handle = h2Dbi.open();
        dao = handle.attach(PeriodicImportDao.class);
    }

    @AfterMethod
    public void teardown()
    {
        handle.close();
    }

    @Test
    public void testTableCreation()
    {
        createTables(dao);

        assertEquals(dao.getJobCount(true), 0);
        assertEquals(dao.getRunCount(true), 0);
    }

    @Test
    public void testJobCreation()
    {
        createTables(dao);
        assertEquals(dao.getJobCount(true), 0);

        PeriodicImportJob dummyJob = new PeriodicImportJob("a", "b", "c", "d", "e", "f", 20);
        long jobId = dao.insertJob(dummyJob);
        assertEquals(jobId, 1);

        assertEquals(dao.getJobCount(true), 1);

        PersistentPeriodicImportJob storedJob = dao.getJob(jobId);
        assertImportjobsEqual(storedJob.getImportJob(), dummyJob);
    }

    @Test
    public void testJobDeletion()
    {
        createTables(dao);
        assertEquals(dao.getJobCount(true), 0);

        PeriodicImportJob dummyJob = new PeriodicImportJob("a", "b", "c", "d", "e", "f", 20);
        long jobId = dao.insertJob(dummyJob);
        assertEquals(jobId, 1);
        assertEquals(dao.getJobCount(true), 1);

        dao.dropJob(jobId);
        assertEquals(dao.getJobCount(true), 0);
        assertEquals(dao.getJobCount(false), 1);
    }

    private void assertImportjobsEqual(PeriodicImportJob job1, PeriodicImportJob job2)
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

    @Test
    public void testRunCreation()
    {
        createTables(dao);
        createDummyJobs(dao);

        String nodeId = UUID.randomUUID().toString();

        long runId = dao.beginRun(1, nodeId);
        assertEquals(runId, 1);

        assertEquals(dao.getJobsStarted().size(), 1); // One distinct job Id running
        assertEquals(dao.getJobsFinished(true).size(), 0);
        assertEquals(dao.getJobsFinished(false).size(), 0);

        dao.finishRun(1, true);

        assertEquals(dao.getJobsStarted().size(), 0);
        assertEquals(dao.getJobsFinished(true).size(), 1);
        assertEquals(dao.getJobsFinished(false).size(), 0);

        runId = dao.beginRun(2, nodeId);
        assertEquals(runId, 2);

        assertEquals(dao.getJobsStarted().size(), 1); // One distinct job Id running
        assertEquals(dao.getJobsFinished(true).size(), 1);
        assertEquals(dao.getJobsFinished(false).size(), 0);

        dao.finishRun(2, false);

        assertEquals(dao.getJobsStarted().size(), 0);
        assertEquals(dao.getJobsFinished(true).size(), 1);
        assertEquals(dao.getJobsFinished(false).size(), 1);
    }

    @Test
    public void testRunMultistart()
    {
        createTables(dao);
        createDummyJobs(dao);
        String nodeId = UUID.randomUUID().toString();

        long runId = dao.beginRun(1, nodeId);
        assertEquals(runId, 1);

        assertEquals(dao.getJobsStarted().size(), 1); // One distinct job Id running
        assertEquals(dao.getJobsFinished(true).size(), 0);
        assertEquals(dao.getJobsFinished(false).size(), 0);

        runId = dao.beginRun(1, nodeId);
        assertEquals(runId, 2);

        assertEquals(dao.getJobsStarted().size(), 1); // Still one, just job restart.
        assertEquals(dao.getJobsFinished(true).size(), 0);
        assertEquals(dao.getJobsFinished(false).size(), 0);

        runId = dao.beginRun(2, nodeId);
        assertEquals(runId, 3);

        assertEquals(dao.getJobsStarted().size(), 2); // second job started
        assertEquals(dao.getJobsFinished(true).size(), 0);
        assertEquals(dao.getJobsFinished(false).size(), 0);

        dao.finishRun(2, true);

        assertEquals(dao.getJobsStarted().size(), 1); // first job, second run finished
        assertEquals(dao.getJobsFinished(true).size(), 1);
        assertEquals(dao.getJobsFinished(false).size(), 0);

        dao.finishRun(3, false);

        assertEquals(dao.getJobsStarted().size(), 0); // second job finished.
        assertEquals(dao.getJobsFinished(true).size(), 1);
        assertEquals(dao.getJobsFinished(false).size(), 1);
    }

    private void createDummyJobs(PeriodicImportDao dao)
    {
        // Foreign keys require the jobs to be present now.
        PeriodicImportJob dummyJob = new PeriodicImportJob("a", "b", "c", "d", "e", "f", 20);
        long jobId = dao.insertJob(dummyJob);
        assertEquals(jobId, 1);
        dummyJob = new PeriodicImportJob("a", "b", "c", "d", "e", "f", 20);
        jobId = dao.insertJob(dummyJob);
        assertEquals(jobId, 2);
    }
}

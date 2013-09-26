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

import com.facebook.presto.metadata.ForMetadata;
import com.google.common.base.Predicate;
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
    private static final Logger log = Logger.get(DatabasePeriodicImportManager.class);

    private static final Duration TABLE_RETRY_INTERVAL = new Duration(10, TimeUnit.SECONDS);

    private final PeriodicImportDao dao;
    private final NodeInfo nodeInfo;

    @Inject
    public DatabasePeriodicImportManager(@ForMetadata IDBI importDbi, NodeInfo nodeInfo)
            throws InterruptedException
    {
        checkNotNull(importDbi, "Dbi was null!");
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo was null!");

        this.dao = importDbi.onDemand(PeriodicImportDao.class);

        createTablesWithRetry();
    }

    @Override
    public long insertJob(PeriodicImportJob job)
    {
        return dao.insertJob(job);
    }

    @Override
    public void dropJob(long jobId)
    {
        dao.dropJob(jobId);
    }

    @Override
    public void dropJobs(Predicate<PersistentPeriodicImportJob> jobPredicate)
    {
        for (PersistentPeriodicImportJob job : getJobs()) {
            if (jobPredicate.apply(job)) {
                dropJob(job.getJobId());
            }
        }
    }

    @Override
    public long getJobCount()
    {
        return dao.getJobCount(true);
    }

    @Override
    public PersistentPeriodicImportJob getJob(long jobId)
    {
        return dao.getJob(jobId);
    }

    @Override
    public List<PersistentPeriodicImportJob> getJobs()
    {
        return dao.getJobs(true);
    }

    @Override
    public long beginRun(long jobId)
    {
        return dao.beginRun(jobId, nodeInfo.getNodeId());
    }

    @Override
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
                Thread.sleep(TABLE_RETRY_INTERVAL.toMillis());
            }
        }
    }
}

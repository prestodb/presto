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

import com.facebook.presto.importer.JobStateFactory.JobState;
import io.airlift.log.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractPeriodicImportRunnable
        implements Runnable
{
    private static final Logger log = Logger.get(AbstractPeriodicImportRunnable.class);

    protected final JobState jobState;
    protected final PeriodicImportManager periodicImportManager;

    protected AbstractPeriodicImportRunnable(JobState jobState, PeriodicImportManager periodicImportManager)
    {
        this.jobState = checkNotNull(jobState, "jobState is null!");
        this.periodicImportManager = checkNotNull(periodicImportManager, "periodicImportManager is null!");
    }

    @Override
    public void run()
    {
        long jobId = jobState.getJob().getJobId();

        long runId = periodicImportManager.beginRun(jobId);
        boolean success = false;

        try {
            long startTime = System.currentTimeMillis();
            log.debug("Job %d: Scheduled for execution!", jobId);

            long lastRun = jobState.getLastRun();
            if (lastRun > 0 && startTime - lastRun < jobState.getJob().getInterval() * 900) {
                // 0.9 * 1000 * interval in sec: Run if at least 90% of interval have passed.
                // In situations where the scheduler wants to "catch up" with job runs, it can rapid-fire
                // the same job multiple times. This check ensures that in these situations, the job is only
                // run once.
                log.debug("Job %d: Last run %dms ago; less than %dms, skipping", jobId, startTime - lastRun, jobState.getJob().getInterval() * 1000);
                return;
            }
            else {
                jobState.setLastRun(startTime);
                doRun();
                log.debug("Job %d: Done!", jobId);
                success = true;
            }
        }
        catch (RuntimeException e) {
            log.warn(e, "Job %d: ", jobId);
        }
        finally {
            // If the interrupted flag is set, the service is in the middle of
            // shutting down and the thread might not be able to access the database etc.
            // in that case, skip the endRun registration (it will be a fail anyway).
            if (!Thread.currentThread().isInterrupted()) {
                periodicImportManager.endRun(runId, success);
            }
        }
    }

    protected abstract void doRun();
}

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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.importer.PersistentPeriodicImportJob.jobIdGetter;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * Loads all available jobs from the jobs table and executes them periodically.
 */
@Singleton
public class PeriodicImportController
{
    private static final Logger log = Logger.get(PeriodicImportController.class);

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private final Duration checkInterval;
    private final AtomicBoolean enabled = new AtomicBoolean();

    private final PeriodicImportManager periodicImportManager;
    private final ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("import-scheduler-%s"));
    private final JobStateFactory jobStateFactory;

    private final AtomicReference<ScheduledFuture<?>> scheduledFuture = new AtomicReference<>();

    private final Map<Long, JobState> runningJobs = new HashMap<>();

    @Inject
    PeriodicImportController(PeriodicImportConfig config,
            PeriodicImportManager periodicImportManager,
            JobStateFactory jobStateFactory)
    {
        checkNotNull(config, "Config was null!");
        this.periodicImportManager = checkNotNull(periodicImportManager, "Import manager was null!");
        this.checkInterval = config.getCheckInterval();
        this.enabled.set(config.isEnabled());

        this.jobStateFactory = jobStateFactory;
    }

    @PostConstruct
    public void start()
    {
        if (enabled.get()) {
            if (started.compareAndSet(false, true)) {
                this.scheduledFuture.set(executorService.scheduleAtFixedRate(new ImportControllerRunnable(),
                        checkInterval.toMillis(), checkInterval.toMillis(), TimeUnit.MILLISECONDS));
            }
            else {
                log.info("Ignored double start.");
            }
        }
        else {
            log.info("Periodic Importer not enabled.");
        }
    }

    @PreDestroy
    public void stop()
    {
        if (!stopped.compareAndSet(false, true)) {
            executorService.shutdownNow();
        }
    }

    private class ImportControllerRunnable
            implements Runnable
    {
        @Override
        public void run()
        {
            try {
                List<PersistentPeriodicImportJob> jobs = periodicImportManager.getJobs();
                Map<Long, PersistentPeriodicImportJob> configuredJobs = Maps.uniqueIndex(jobs, jobIdGetter());

                Set<Long> currentJobs = runningJobs.keySet();
                Set<Long> jobsToAdd = ImmutableSet.copyOf(Sets.difference(configuredJobs.keySet(), currentJobs));
                Set<Long> jobsToRemove = ImmutableSet.copyOf(Sets.difference(currentJobs, configuredJobs.keySet()));

                for (Long jobId : jobsToAdd) {
                    runningJobs.put(jobId, jobStateFactory.forImportJob(configuredJobs.get(jobId)));
                }

                for (Long oldJob : jobsToRemove) {
                    JobState removedJob = runningJobs.remove(oldJob);
                    removedJob.cancel(true);
                }

                log.debug("Current set of jobs is %s", runningJobs.keySet());

                for (JobState importJob : runningJobs.values()) {
                    importJob.schedule();
                }
            }
            catch (Throwable e) {
                log.error(e, "Caught problem when scanning import jobs!");
            }
        }
    }

    @Managed
    public void setEnabled(boolean enabled)
    {
        this.enabled.set(enabled);
    }

    @Managed
    public boolean isEnabled()
    {
        return this.enabled.get();
    }
}

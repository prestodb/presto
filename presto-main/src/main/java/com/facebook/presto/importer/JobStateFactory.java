package com.facebook.presto.importer;

import com.facebook.presto.importer.PeriodicImportRunnable.PeriodicImportRunnableFactory;

import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkArgument;

@Singleton
public class JobStateFactory
{
    private static final Logger log = Logger.get(JobStateFactory.class);

    private final ScheduledExecutorService importExecutorService;
    private final PeriodicImportRunnableFactory periodicImportRunnableFactory;

    @Inject
    public JobStateFactory(PeriodicImportConfig config, PeriodicImportRunnableFactory periodicImportRunnableFactory)
    {
        this.importExecutorService = new ScheduledThreadPoolExecutor(config.getThreadCount(), daemonThreadsNamed("import-%s"));
        this.periodicImportRunnableFactory = periodicImportRunnableFactory;
    }

    public JobState forImportJob(PersistentPeriodicImportJob importJob)
    {
        return new JobState(importJob);
    }

    @PreDestroy
    public void shutdown()
    {
        importExecutorService.shutdownNow();
    }

    public class JobState
    {
        private final PersistentPeriodicImportJob job;
        private final AtomicReference<ScheduledFuture<?>> futureHolder = new AtomicReference<>();
        private final AtomicLong lastRun = new AtomicLong(-1L);
        private final Random random = new Random();

        private JobState(PersistentPeriodicImportJob job)
        {
            this.job = job;
        }

        public void schedule()
        {
            if (futureHolder.get() == null) {
                long initialDelay = random.nextInt(50000) + 10000; // 10 - 60 seconds initial delay
                ScheduledFuture<?> jobFuture = importExecutorService.scheduleAtFixedRate(periodicImportRunnableFactory.create(this), initialDelay, job.getInterval() * 1000L, TimeUnit.MILLISECONDS);
                if (!futureHolder.compareAndSet(null, jobFuture)) {
                    // Something went wrong. Kill the job future
                    jobFuture.cancel(true);
                    return;
                }
                log.info("Scheduled Job for %d, initial delay is %dms", job.getJobId(), initialDelay);
            }
        }

        public void cancel(boolean mayInterruptIfRunning)
        {
            ScheduledFuture<?> jobFuture = futureHolder.getAndSet(null);
            if (jobFuture != null) {
                jobFuture.cancel(mayInterruptIfRunning);
                log.info("Cancelled Job for %d", job.getJobId());
            }
        }

        public void setLastRun(long time)
        {
            checkArgument(time > 0, "The run time most be positive!");
            this.lastRun.set(time);
        }

        public long getLastRun()
        {
            return this.lastRun.get();
        }

        public PersistentPeriodicImportJob getJob()
        {
            return job;
        }
    }
}

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
package com.facebook.presto.raptorx.metadata;

import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CommitCleanerJob
{
    private static final Logger log = Logger.get(CommitCleanerJob.class);

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("commit-cleaner"));
    private final CommitCleaner cleaner;
    private final Duration interval;
    private final AtomicBoolean started = new AtomicBoolean();
    private final CounterStat jobErrors = new CounterStat();

    @Inject
    public CommitCleanerJob(CommitCleaner cleaner, CommitCleanerConfig config)
    {
        this(cleaner, config.getInterval());
    }

    public CommitCleanerJob(CommitCleaner cleaner, Duration interval)
    {
        this.cleaner = requireNonNull(cleaner, "cleaner is null");
        this.interval = requireNonNull(interval, "interval is null");
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            executor.scheduleWithFixedDelay(this::run, interval.toMillis(), interval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public CounterStat getJobErrors()
    {
        return jobErrors;
    }

    private void run()
    {
        try {
            cleaner.removeOldCommits();
        }
        catch (Throwable t) {
            log.error(t, "Error cleaning commits");
            jobErrors.update(1);
        }
    }
}

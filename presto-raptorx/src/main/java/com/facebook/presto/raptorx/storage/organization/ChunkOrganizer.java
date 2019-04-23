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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.storage.StorageConfig;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ChunkOrganizer
{
    private static final Logger log = Logger.get(ChunkOrganizer.class);

    private final ExecutorService executorService;
    private final ThreadPoolExecutorMBean executorMBean;

    private final AtomicBoolean shutdown = new AtomicBoolean();

    // Tracks chunks that are scheduled for compaction so that we do not schedule them more than once
    private final Set<Long> chunksInProgress = newConcurrentHashSet();
    private final JobFactory jobFactory;
    private final CounterStat successCount = new CounterStat();
    private final CounterStat failureCount = new CounterStat();

    @Inject
    public ChunkOrganizer(JobFactory jobFactory, StorageConfig config)
    {
        this(jobFactory, config.getOrganizationThreads());
    }

    public ChunkOrganizer(JobFactory jobFactory, int threads)
    {
        checkArgument(threads > 0, "threads must be > 0");
        this.jobFactory = requireNonNull(jobFactory, "jobFactory is null");
        this.executorService = newFixedThreadPool(threads, daemonThreadsNamed("chunk-organizer-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executorService);
    }

    @PreDestroy
    public void shutdown()
    {
        if (!shutdown.getAndSet(true)) {
            executorService.shutdownNow();
        }
    }

    public CompletableFuture<?> enqueue(OrganizationSet organizationSet)
    {
        chunksInProgress.addAll(organizationSet.getChunkIds());
        return runAsync(jobFactory.create(organizationSet), executorService)
                .whenComplete((none, throwable) -> {
                    chunksInProgress.removeAll(organizationSet.getChunkIds());
                    if (throwable == null) {
                        successCount.update(1);
                    }
                    else {
                        log.warn(throwable, "Error running organization job");
                        failureCount.update(1);
                    }
                });
    }

    public boolean inProgress(long chunkId)
    {
        return chunksInProgress.contains(chunkId);
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Managed
    public int getChunksInProgress()
    {
        return chunksInProgress.size();
    }

    @Managed
    @Nested
    public CounterStat getSuccessCount()
    {
        return successCount;
    }

    @Managed
    @Nested
    public CounterStat getFailureCount()
    {
        return failureCount;
    }
}

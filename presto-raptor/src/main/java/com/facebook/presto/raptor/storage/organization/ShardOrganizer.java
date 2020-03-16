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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.raptor.util.PrioritizedFifoExecutor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toSet;

public class ShardOrganizer
{
    private static final Logger log = Logger.get(ShardOrganizer.class);

    private final ExecutorService executorService;
    private final PrioritizedFifoExecutor<Runnable> prioritizedFifoExecutor;
    private final ThreadPoolExecutorMBean executorMBean;

    private final AtomicBoolean shutdown = new AtomicBoolean();

    // Tracks shards that are scheduled for compaction so that we do not schedule them more than once
    private final Map<UUID, Optional<UUID>> shardsInProgress = new ConcurrentHashMap<>();
    private final JobFactory jobFactory;
    private final CounterStat successCount = new CounterStat();
    private final CounterStat failureCount = new CounterStat();

    private int deltaCountInProgress;

    @Inject
    public ShardOrganizer(JobFactory jobFactory, StorageManagerConfig config)
    {
        this(jobFactory, config.getOrganizationThreads());
    }

    public ShardOrganizer(JobFactory jobFactory, int threads)
    {
        checkArgument(threads > 0, "threads must be > 0");
        this.jobFactory = requireNonNull(jobFactory, "jobFactory is null");
        this.executorService = newCachedThreadPool(daemonThreadsNamed("shard-organizer-%s"));
        this.prioritizedFifoExecutor = new PrioritizedFifoExecutor(executorService, threads, new OrganizationJobComparator());
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executorService);
    }

    @PreDestroy
    public void shutdown()
    {
        if (!shutdown.getAndSet(true)) {
            executorService.shutdownNow();
        }
    }

    public ListenableFuture<?> enqueue(OrganizationSet organizationSet)
    {
        log.info("enqueue organizationSet: %s", organizationSet);
        shardsInProgress.putAll(organizationSet.getShardsMap());
        deltaCountInProgress = shardsInProgress.values().stream().filter(Optional::isPresent).collect(toSet()).size();
        ListenableFuture<?> listenableFuture = prioritizedFifoExecutor.submit(jobFactory.create(organizationSet));
        listenableFuture.addListener(
                () -> {
                    for (UUID uuid : organizationSet.getShardsMap().keySet()) {
                        shardsInProgress.remove(uuid);
                        deltaCountInProgress = shardsInProgress.values().stream().filter(Optional::isPresent).collect(toSet()).size();
                    }
                },
                MoreExecutors.directExecutor());
        Futures.addCallback(
                listenableFuture,
                new FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object o)
                    {
                        successCount.update(1);
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        log.warn(throwable, "Error running organization job");
                        failureCount.update(1);
                    }
                },
                MoreExecutors.directExecutor());
        return listenableFuture;
    }

    public boolean inProgress(UUID shardUuid)
    {
        return shardsInProgress.containsKey(shardUuid);
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Managed
    public int getShardsInProgress()
    {
        return shardsInProgress.size();
    }

    @Managed
    public int getDeltaCountInProgress()
    {
        return deltaCountInProgress;
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

    @VisibleForTesting
    static class OrganizationJobComparator
            implements Comparator<OrganizationJob>
    {
        @Override
        public int compare(OrganizationJob left, OrganizationJob right)
        {
            return right.getPriority() - left.getPriority();
        }
    }
}

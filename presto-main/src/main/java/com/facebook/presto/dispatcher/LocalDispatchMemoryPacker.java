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
package com.facebook.presto.dispatcher;

import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.CounterStat;
import org.jetbrains.annotations.NotNull;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LocalDispatchMemoryPacker
{
    private static final int REFRESH_INTERVAL_MILLIS = 10;

    private final ScheduledExecutorService executorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("resource-aware-dispatcher"));

    @GuardedBy("this")
    private final LinkedHashMap<WaitingQuery, Long> waitingPreallocations = new LinkedHashMap<>();
    @GuardedBy("this")
    private final LoadingCache<LocalDispatchQuery, Long> recentAllocations;

    private final CounterStat deadlineExceeded = new CounterStat();
    private final CounterStat preAllocationExhausted = new CounterStat();

    private final ResourceEstimates defaultResourceEstimates;
    private final Supplier<Long> totalMemorySupplier;
    private final Supplier<Long> reservedMemorySupplier;

    private final double deadlineRatio;
    private final double totalToUserMemoryRatio;
    private final double overcommitRatio;
    private final long minimumSystemMemoryBytes;

    @Inject
    public LocalDispatchMemoryPacker(ClusterMemoryManager memoryManager, LocalDispatchMemoryPackerConfig config)
    {
        this(() -> requireNonNull(memoryManager, "memoryManager is null").getClusterMemoryBytes(), memoryManager::getClusterTotalMemoryReservation, config);
    }

    @VisibleForTesting
    public LocalDispatchMemoryPacker(Supplier<Long> totalMemorySupplier, Supplier<Long> reservedMemorySupplier, LocalDispatchMemoryPackerConfig config)
    {
        this.defaultResourceEstimates = new ResourceEstimates(
                Optional.of(config.getAverageQueryExecutionTime()),
                Optional.empty(),
                Optional.of(config.getAverageQueryPeakMemory()));

        this.deadlineRatio = config.getDeadlineRatio();
        this.totalToUserMemoryRatio = config.getTotalToUserMemoryRatio();
        this.overcommitRatio = config.getOvercommitRatio();
        this.minimumSystemMemoryBytes = config.getMinimumSystemMemory().toBytes();
        this.totalMemorySupplier = totalMemorySupplier;
        this.reservedMemorySupplier = reservedMemorySupplier;

        this.recentAllocations = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(config.getQueryWarmupTime().toMillis(), MILLISECONDS)
                .build(
                        new CacheLoader<LocalDispatchQuery, Long>() {
                            public Long load(@NotNull LocalDispatchQuery query) {
                                throw new UnsupportedOperationException();
                            }
                        });
    }

    @PostConstruct
    public void initialize()
    {
        executorService.scheduleWithFixedDelay(this::runIfEligible, 0, REFRESH_INTERVAL_MILLIS, MILLISECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
    }

    private synchronized long getAvailableMemoryBytes()
    {
        long recentlyAllocatedBytes = recentAllocations.asMap()
                .values()
                .stream()
                .mapToLong(x -> x)
                .sum();

        return (long) (totalMemorySupplier.get() * (1 + overcommitRatio) - (reservedMemorySupplier.get() + recentlyAllocatedBytes));
    }

    public synchronized void addToQueue(LocalDispatchQuery query, Runnable callback)
    {
        // TODO: Somewhere we need to skip the queue for cheap queries (e.g. DDL), this might be where..
        ResourceEstimates estimates = query.getSession().getResourceEstimates().withDefaults(defaultResourceEstimates);
        long estimatedExecutionTimeMillis = estimates.getExecutionTime().get().toMillis();

        WaitingQuery waitingQuery = new WaitingQuery(query, callback, estimates.getPeakMemory().get().toBytes(), (long) (deadlineRatio * estimatedExecutionTimeMillis));
        query.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                waitingPreallocations.remove(waitingQuery);
                recentAllocations.invalidate(query);
            }
        });
        waitingPreallocations.put(waitingQuery, 0L);
    }

    public synchronized void runIfEligible()
    {
        long totalAvailable = getAvailableMemoryBytes();
        long totalPreAllocated = waitingPreallocations.values().stream().reduce(0L, Long::sum);
        long now = System.currentTimeMillis();

        // Iterate through waiting queries FCFS (LinkedHashMap iterator preserves insertion order)
        // and incrementally pre-allocate available memory to oldest queries
        for (WaitingQuery waitingQuery : ImmutableSet.copyOf(waitingPreallocations.keySet())) {
            long bytesRequired = (long) Math.max(
                    waitingQuery.getEstimatedPeakMemoryBytes() * (1 + totalToUserMemoryRatio),
                    waitingQuery.getEstimatedPeakMemoryBytes() + minimumSystemMemoryBytes);

            long existingPreAllocation = waitingPreallocations.getOrDefault(waitingQuery, 0L);
            long bytesAvailableForQuery = (totalAvailable + existingPreAllocation);
            if (bytesAvailableForQuery >= bytesRequired) {
                waitingQuery.run();
                totalPreAllocated -= waitingPreallocations.remove(waitingQuery);
                // for this cycle, assume that the query just started immediately uses up all the memory it needs
                totalAvailable -= bytesRequired;
                recentAllocations.put(waitingQuery.getQuery(), bytesRequired);
                continue;
            }

            long remainingMillis = Math.max(0, now - (waitingQuery.getAddedTime() + waitingQuery.getDeadline()));
            long remainingPreAllocation = (waitingQuery.getEstimatedPeakMemoryBytes() - existingPreAllocation);

            long incrementalPreAllocation;
            if (remainingMillis == 0) {
                deadlineExceeded.update(1);
                incrementalPreAllocation = remainingPreAllocation;
            }
            else {
                // incrementally pre-allocate memory to the query, such that by the 'deadline'
                // the query has ideally been pre-allocated all the memory it needs
                incrementalPreAllocation = remainingPreAllocation / (remainingMillis / REFRESH_INTERVAL_MILLIS);
            }

            long remainingTotalPreAllocation = totalAvailable - totalPreAllocated;
            if (remainingTotalPreAllocation <= 0) {
                preAllocationExhausted.update(1);
                break;
            }

            long newPreAllocation = Math.max(existingPreAllocation + incrementalPreAllocation, remainingTotalPreAllocation);
            waitingPreallocations.put(waitingQuery, newPreAllocation);
            totalPreAllocated += newPreAllocation;
        }
    }

    public static class WaitingQuery
    {
        private final long addedTime = System.currentTimeMillis();
        private final long estimatedPeakMemoryBytes;
        private final long deadline;
        private final LocalDispatchQuery query;
        private final Runnable callback;

        public WaitingQuery(LocalDispatchQuery query, Runnable callback, long estimatedPeakMemoryBytes, long deadline)
        {
            this.query = requireNonNull(query, "query is null");
            this.callback = requireNonNull(callback, "callback is null");
            this.estimatedPeakMemoryBytes = estimatedPeakMemoryBytes;
            this.deadline = deadline;
        }

        public long getAddedTime()
        {
            return addedTime;
        }

        public long getEstimatedPeakMemoryBytes()
        {
            return estimatedPeakMemoryBytes;
        }

        public long getDeadline()
        {
            return deadline;
        }

        public LocalDispatchQuery getQuery()
        {
            return query;
        }

        public void run()
        {
            callback.run();
        }
    }
}

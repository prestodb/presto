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
package com.facebook.presto.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.MemoryPoolListener;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.VoidTraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.QueryId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.execution.MemoryRevokingSchedulerUtils.getMemoryAlreadyBeingRevoked;
import static com.facebook.presto.execution.MemoryRevokingUtils.getMemoryPools;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class QueryLimitMemoryRevokingScheduler
{
    private static final Logger log = Logger.get(QueryLimitMemoryRevokingScheduler.class);

    private final ScheduledExecutorService taskManagementExecutor;
    private final Function<QueryId, QueryContext> queryContextSupplier;

    private final List<MemoryPool> memoryPools;
    private final MemoryPoolListener memoryPoolListener = this::onMemoryReserved;
    private final ConcurrentHashMap<QueryId, Boolean> revocationRequestedByQuery = new ConcurrentHashMap<>();

    @Inject
    public QueryLimitMemoryRevokingScheduler(
            LocalMemoryManager localMemoryManager,
            SqlTaskManager sqlTaskManager,
            TaskManagementExecutor taskManagementExecutor)
    {
        this(
                ImmutableList.copyOf(getMemoryPools(localMemoryManager)),
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getQueryContext,
                requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor());
        log.debug("Using QueryLimitMemoryRevokingScheduler spilling strategy");
    }

    @VisibleForTesting
    QueryLimitMemoryRevokingScheduler(
            List<MemoryPool> memoryPools,
            Function<QueryId, QueryContext> queryContextSupplier,
            ScheduledExecutorService taskManagementExecutor)
    {
        this.memoryPools = ImmutableList.copyOf(requireNonNull(memoryPools, "memoryPools is null"));
        this.queryContextSupplier = requireNonNull(queryContextSupplier, "queryContextSupplier is null");
        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor is null");
    }

    @PostConstruct
    public void start()
    {
        registerPoolListeners();
    }

    @PreDestroy
    public void stop()
    {
        memoryPools.forEach(memoryPool -> memoryPool.removeListener(memoryPoolListener));
    }

    @VisibleForTesting
    void registerPoolListeners()
    {
        memoryPools.forEach(memoryPool -> memoryPool.addListener(memoryPoolListener));
    }

    private void onMemoryReserved(MemoryPool memoryPool, QueryId queryId, long queryTotalMemoryUse)
    {
        try {
            QueryContext queryContext = queryContextSupplier.apply(queryId);
            verify(queryContext != null, "QueryContext not found for queryId %s", queryId);
            long maxTotalMemory = queryContext.getMaxTotalMemory();
            if (queryTotalMemoryUse <= maxTotalMemory) {
                return;
            }

            log.debug("Scheduling check for %s", queryId);
            if (revocationRequestedByQuery.put(queryId, true) == null) {
                scheduleRevoking(queryContext, maxTotalMemory);
            }
        }
        catch (Exception e) {
            log.error(e, "Error when acting on memory pool reservation");
        }
    }

    private void scheduleRevoking(QueryContext queryContext, long maxTotalMemory)
    {
        taskManagementExecutor.execute(() -> {
            try {
                revokeMemory(queryContext, maxTotalMemory);
            }
            catch (Exception e) {
                log.error(e, "Error requesting memory revoking");
            }
        });
    }

    private void revokeMemory(QueryContext queryContext, long maxTotalMemory)
    {
        QueryId queryId = queryContext.getQueryId();
        MemoryPool memoryPool = queryContext.getMemoryPool();
        // (queryId, true) gets put into the revocationRequestByQueryId map in onMemoryReserved() whenever
        // revocation is needed. scheduleRevoking() is called at that time if the map does not already
        // have any entry for that queryId. This ensures that only one revokeMemory() invocation will be running
        // for any queryId at a time. At the start of this loop, we set the value for that queryId to false in the map.
        // If an additional memory revocation request comes in, the value will be changed to true in onMemoryReserve().
        // The condition for this while loop says that if the value for that queryId is false
        // (meaning no further memory revocation requests have come in since our last iteration through the loop),
        // then remove that queryId from the map and break out of the loop.
        while (!revocationRequestedByQuery.remove(queryId, false)) {
            revocationRequestedByQuery.put(queryId, false);
            // get a fresh value for queryTotalMemory in case it's changed (e.g. by a previous revocation request)
            long queryTotalMemory = getTotalQueryMemoryReservation(queryId, memoryPool);
            // order tasks by decreasing revocableMemory so that we don't spill more tasks than needed
            SortedMap<Long, TaskContext> queryTaskContextsMap = new TreeMap<>(Comparator.reverseOrder());
            queryContext.getAllTaskContexts()
                    .forEach(taskContext -> queryTaskContextsMap.put(taskContext.getTaskMemoryContext().getRevocableMemory(), taskContext));

            AtomicLong remainingBytesToRevoke = new AtomicLong(queryTotalMemory - maxTotalMemory);
            Collection<TaskContext> queryTaskContexts = queryTaskContextsMap.values();
            remainingBytesToRevoke.addAndGet(-getMemoryAlreadyBeingRevoked(queryTaskContexts, remainingBytesToRevoke.get()));
            for (TaskContext taskContext : queryTaskContexts) {
                if (remainingBytesToRevoke.get() <= 0) {
                    break;
                }
                taskContext.accept(new VoidTraversingQueryContextVisitor<AtomicLong>()
                {
                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() > 0) {
                            long revokedBytes = operatorContext.requestMemoryRevoking();
                            if (revokedBytes > 0) {
                                remainingBytesToRevoke.addAndGet(-revokedBytes);
                                log.debug("taskId=%s: requested revoking %s; remaining %s", taskContext.getTaskId(), revokedBytes, remainingBytesToRevoke);
                            }
                        }
                        return null;
                    }
                }, remainingBytesToRevoke);
            }
        }
    }

    private static long getTotalQueryMemoryReservation(QueryId queryId, MemoryPool memoryPool)
    {
        return memoryPool.getQueryMemoryReservation(queryId) + memoryPool.getQueryRevocableMemoryReservation(queryId);
    }
}

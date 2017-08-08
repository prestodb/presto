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

import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.TraversingQueryContextVisitor;
import com.facebook.presto.memory.VoidTraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MemoryRevokingScheduler
{
    private static final Logger log = Logger.get(MemoryRevokingScheduler.class);

    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(task -> task.getTaskInfo().getStats().getCreateTime());
    private final List<MemoryPool> memoryPools;
    private final Supplier<? extends Collection<SqlTask>> currentTasksSupplier;
    private final ScheduledExecutorService taskManagementExecutor;
    private final double memoryRevokingThreshold;
    private final double memoryRevokingTarget;

    @Nullable
    private ScheduledFuture<?> scheduledFuture;

    @Inject
    public MemoryRevokingScheduler(
            LocalMemoryManager localMemoryManager,
            SqlTaskManager sqlTaskManager,
            TaskManagementExecutor taskManagementExecutor,
            FeaturesConfig config)
    {
        this(
                ImmutableList.copyOf(getMemoryPools(localMemoryManager)),
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getAllTasks,
                requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor(),
                config.getMemoryRevokingThreshold(),
                config.getMemoryRevokingTarget());
    }

    @VisibleForTesting
    MemoryRevokingScheduler(
            List<MemoryPool> memoryPools,
            Supplier<? extends Collection<SqlTask>> currentTasksSupplier,
            ScheduledExecutorService taskManagementExecutor,
            double memoryRevokingThreshold,
            double memoryRevokingTarget)
    {
        this.memoryPools = ImmutableList.copyOf(requireNonNull(memoryPools, "memoryPools is null"));
        this.currentTasksSupplier = requireNonNull(currentTasksSupplier, "currentTasksSupplier is null");
        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor is null");
        this.memoryRevokingThreshold = checkFraction(memoryRevokingThreshold, "memoryRevokingThreshold");
        this.memoryRevokingTarget = checkFraction(memoryRevokingTarget, "memoryRevokingTarget");
        checkArgument(
                memoryRevokingTarget <= memoryRevokingThreshold,
                "memoryRevokingTarget should be less than or equal memoryRevokingThreshold, but got %s and %s respectively",
                memoryRevokingTarget, memoryRevokingThreshold);
    }

    private static double checkFraction(double value, String valueName)
    {
        requireNonNull(valueName, "valueName is null");
        checkArgument(0 <= value && value <= 1, "%s should be within [0, 1] range, got %s", valueName, value);
        return value;
    }

    private static List<MemoryPool> getMemoryPools(LocalMemoryManager localMemoryManager)
    {
        requireNonNull(localMemoryManager, "localMemoryManager can not be null");
        return ImmutableList.of(localMemoryManager.getPool(LocalMemoryManager.GENERAL_POOL), localMemoryManager.getPool(LocalMemoryManager.RESERVED_POOL));
    }

    @PostConstruct
    public void start()
    {
        this.scheduledFuture = taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                requestMemoryRevokingIfNeeded(currentTasksSupplier.get());
            }
            catch (Throwable e) {
                log.error(e, "Error requesting system memory revoking");
            }
        }, 1, 1, SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
    }

    @VisibleForTesting
    synchronized void requestMemoryRevokingIfNeeded(Collection<SqlTask> sqlTasks)
    {
        memoryPools.forEach(memoryPool -> requestMemoryRevokingIfNeeded(memoryPool, sqlTasks));
    }

    private void requestMemoryRevokingIfNeeded(MemoryPool memoryPool, Collection<SqlTask> sqlTasks)
    {
        long freeBytes = memoryPool.getFreeBytes();
        if (freeBytes > memoryPool.getMaxBytes() * (1.0 - memoryRevokingThreshold)) {
            return;
        }

        long remainingBytesToRevoke = (long) (-freeBytes + (memoryPool.getMaxBytes() * (1.0 - memoryRevokingTarget)));
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(memoryPool, sqlTasks);
        requestRevoking(memoryPool, sqlTasks, remainingBytesToRevoke);
    }

    private long getMemoryAlreadyBeingRevoked(MemoryPool memoryPool, Collection<SqlTask> sqlTasks)
    {
        return sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .filter(task -> task.getQueryContext().getMemoryPool() == memoryPool)
                .mapToLong(task -> task.getQueryContext().accept(new TraversingQueryContextVisitor<Void, Long>()
                {
                    @Override
                    public Long visitOperatorContext(OperatorContext operatorContext, Void context)
                    {
                        if (operatorContext.isMemoryRevokingRequested()) {
                            return operatorContext.getReservedRevocableBytes();
                        }
                        return 0L;
                    }

                    @Override
                    public Long mergeResults(List<Long> childrenResults)
                    {
                        return childrenResults.stream()
                                .mapToLong(i -> i).sum();
                    }
                }, null))
                .sum();
    }

    private void requestRevoking(MemoryPool memoryPool, Collection<SqlTask> sqlTasks, long remainingBytesToRevoke)
    {
        AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
        sqlTasks.stream()
                .filter(task -> task.getTaskInfo().getTaskStatus().getState() == TaskState.RUNNING)
                .filter(task -> task.getQueryContext().getMemoryPool() == memoryPool)
                .sorted(ORDER_BY_CREATE_TIME)
                .forEach(task -> task.getQueryContext().accept(new VoidTraversingQueryContextVisitor<AtomicLong>()
                {
                    @Override
                    public Void visitQueryContext(QueryContext queryContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() < 0) {
                            // exit immediately if no work needs to be done
                            return null;
                        }
                        return super.visitQueryContext(queryContext, remainingBytesToRevoke);
                    }

                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() > 0) {
                            long revokedBytes = operatorContext.requestMemoryRevoking();
                            if (revokedBytes > 0) {
                                remainingBytesToRevoke.addAndGet(-revokedBytes);
                                log.debug("memoryPool=%s: requested revoking %s; remaining %s", memoryPool.getId(), revokedBytes, remainingBytesToRevoke.get());
                            }
                        }
                        return null;
                    }
                }, remainingBytesToRevokeAtomic));
    }
}

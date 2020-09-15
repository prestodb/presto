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
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.VoidTraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TaskThresholdMemoryRevokingScheduler
{
    private static final Logger log = Logger.get(TaskThresholdMemoryRevokingScheduler.class);

    private final Supplier<List<SqlTask>> currentTasksSupplier;
    private final ScheduledExecutorService taskManagementExecutor;
    private final long maxRevocableMemoryPerTask;

    // Technically not thread safe but should be fine since we only call this on PostConstruct and PreDestroy.
    // PreDestroy isn't called until server shuts down/ in between tests.
    @Nullable
    private ScheduledFuture<?> scheduledFuture;

    private final AtomicBoolean checkPending = new AtomicBoolean();

    @Inject
    public TaskThresholdMemoryRevokingScheduler(
            SqlTaskManager sqlTaskManager,
            TaskManagementExecutor taskManagementExecutor,
            FeaturesConfig config)
    {
        this(
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getAllTasks,
                requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor(),
                config.getMaxRevocableMemoryPerTask());
        log.debug("Using TaskThresholdMemoryRevokingScheduler spilling strategy");
    }

    @VisibleForTesting
    TaskThresholdMemoryRevokingScheduler(
            Supplier<List<SqlTask>> currentTasksSupplier,
            ScheduledExecutorService taskManagementExecutor,
            long maxRevocableMemoryPerTask)
    {
        this.currentTasksSupplier = requireNonNull(currentTasksSupplier, "currentTasksSupplier is null");
        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor is null");
        this.maxRevocableMemoryPerTask = maxRevocableMemoryPerTask;
    }

    @PostConstruct
    public void start()
    {
        registerTaskMemoryPeriodicCheck();
    }

    private void registerTaskMemoryPeriodicCheck()
    {
        this.scheduledFuture = taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                revokeHighMemoryTasksIfNeeded();
            }
            catch (Throwable e) {
                log.error(e, "Error requesting task memory revoking");
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
    void revokeHighMemoryTasksIfNeeded()
    {
        if (checkPending.compareAndSet(false, true)) {
            revokeHighMemoryTasks();
        }
    }

    private synchronized void revokeHighMemoryTasks()
    {
        if (checkPending.getAndSet(false)) {
            Collection<SqlTask> sqlTasks = requireNonNull(currentTasksSupplier.get());
            for (SqlTask task : sqlTasks) {
                long currentTaskRevocableMemory = task.getTaskInfo().getStats().getRevocableMemoryReservationInBytes();
                if (currentTaskRevocableMemory < maxRevocableMemoryPerTask) {
                    continue;
                }

                AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(currentTaskRevocableMemory - maxRevocableMemoryPerTask);
                task.getQueryContext().accept(new VoidTraversingQueryContextVisitor<AtomicLong>()
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
                                log.debug("taskId=%s: requested revoking %s; remaining %s", task.getTaskInfo().getTaskId(), revokedBytes, remainingBytesToRevoke.get());
                            }
                        }
                        return null;
                    }
                }, remainingBytesToRevokeAtomic);
            }
        }
    }
}

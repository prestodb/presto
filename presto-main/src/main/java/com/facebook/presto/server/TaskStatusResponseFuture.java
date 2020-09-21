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
package com.facebook.presto.server;

import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.addExceptionCallback;
import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.presto.util.TaskUtils.randomizeWaitTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class TaskStatusResponseFuture
        extends AbstractFuture<TaskStatus>
{
    private final TaskManager taskManager;
    private final TaskId taskId;
    private final TaskStatus initialTaskStatus;
    private final ListenableFuture<TaskStatus> future;
    private final Duration interval;
    private final Duration timeout;
    private final ScheduledExecutorService timeoutExecutor;
    private final TaskStatusResponseStats taskStatusResponseStats;
    private final double taskStatusSignificanceFactor;

    private final long startTimeMillis = System.currentTimeMillis();

    public TaskStatusResponseFuture(
            TaskManager taskManager,
            TaskId taskId,
            TaskStatus initialTaskStatus,
            ListenableFuture<TaskStatus> future,
            Duration interval,
            Duration timeout,
            ScheduledExecutorService timeoutExecutor,
            TaskStatusResponseStats taskStatusResponseStats,
            double taskStatusSignificanceFactor)
    {
        this.taskManager = taskManager;
        this.taskId = taskId;
        this.initialTaskStatus = initialTaskStatus;
        this.future = future;
        this.interval = interval;
        this.timeout = timeout;
        this.timeoutExecutor = timeoutExecutor;
        this.taskStatusResponseStats = taskStatusResponseStats;
        this.taskStatusSignificanceFactor = taskStatusSignificanceFactor;

        // When the delegate future completes, immediately update this future
        addSuccessCallback(future, this::tick);
        addExceptionCallback(future, this::tick);
    }

    public static ListenableFuture<TaskStatus> create(
            TaskManager taskManager,
            TaskId taskId,
            TaskState currentState,
            Optional<Duration> interval,
            Duration timeout,
            ScheduledExecutorService timeoutExecutor,
            TaskStatusResponseStats taskStatusResponseStats, double taskStatusSignificanceFactor)
    {
        TimeStat.BlockTimer responseTimer = taskStatusResponseStats.getTaskStatusResponseTime().time();
        ListenableFuture<TaskStatus> statusFuture = taskManager.getTaskStatus(taskId, currentState);

        // Short circuit if the interval isn't specified, or if it is greater than or equal to the timeout
        if (!interval.isPresent() || timeout.compareTo(interval.get()) <= 0) {
            statusFuture = addTimeout(
                    statusFuture,
                    () -> taskManager.getTaskStatus(taskId),
                    timeout,
                    timeoutExecutor);
        }
        else {
            TaskStatus initialTaskStatus = taskManager.getTaskStatus(taskId);
            TaskStatusResponseFuture taskStatusFuture = new TaskStatusResponseFuture(
                    taskManager,
                    taskId,
                    initialTaskStatus,
                    statusFuture,
                    interval.get(),
                    timeout,
                    timeoutExecutor,
                    taskStatusResponseStats,
                    taskStatusSignificanceFactor);
            timeoutExecutor.schedule(taskStatusFuture::tick, randomizeWaitTime(interval.get()).toMillis(), MILLISECONDS);
            statusFuture = taskStatusFuture;
        }
        addSuccessCallback(statusFuture, responseTimer::close);
        addExceptionCallback(statusFuture, responseTimer::close);
        return statusFuture;
    }

    private synchronized void tick()
    {
        if (future.isDone()) {
            try {
                set(Futures.getDone(future));
            }
            catch (ExecutionException e) {
                setException(e.getCause());
            }
            catch (CancellationException e) {
                set(taskManager.getTaskStatus(taskId));
            }
        }
        else {
            long currentDuration = System.currentTimeMillis() - startTimeMillis;
            TaskStatus taskStatus = taskManager.getTaskStatus(taskId);

            // timeout
            if (currentDuration >= timeout.toMillis()) {
                future.cancel(false);
//                System.out.println("TaskStatus " + taskId + ": Timed out here too, waited: " + currentDuration);
                taskStatusResponseStats.incrementTaskStatusTimedOut();
                set(taskStatus);
                return;
            }
            // Check for significant difference in memory
            long memoryDifference = Math.abs(taskStatus.getMemoryReservationInBytes() - initialTaskStatus.getMemoryReservationInBytes());
            long systemMemoryDifference = Math.abs(taskStatus.getSystemMemoryReservationInBytes() - initialTaskStatus.getSystemMemoryReservationInBytes());
            long queuedPartitionedDriversDifference = Math.abs(taskStatus.getQueuedPartitionedDrivers() - initialTaskStatus.getQueuedPartitionedDrivers());
            long runningPartitionedDriversDifference = Math.abs(taskStatus.getRunningPartitionedDrivers() - initialTaskStatus.getQueuedPartitionedDrivers());
            long fullGcCountDifference = Math.abs(taskStatus.getFullGcCount() - initialTaskStatus.getFullGcCount());
            long physicalWrittenDataSizeDifferenceInBytes = Math.abs(taskStatus.getPhysicalWrittenDataSizeInBytes() - initialTaskStatus.getPhysicalWrittenDataSizeInBytes());

            if (memoryDifference > taskStatusSignificanceFactor * initialTaskStatus.getMemoryReservationInBytes()) {
                future.cancel(false);
                System.out.println("TaskStatus " + taskId + ": Memory grew, waited: " + currentDuration);
                taskStatusResponseStats.incrementTaskStatusMemoryIncreased();
                set(taskStatus);
            }
            else if (systemMemoryDifference > taskStatusSignificanceFactor * initialTaskStatus.getSystemMemoryReservationInBytes()) {
                future.cancel(false);
//                System.out.println(format("TaskStatus %s: System memory grew, waited: %s, difference %s, old: %s, new: %s: ", taskId, currentDuration, systemMemoryDifference, initialTaskStatus.getSystemMemoryReservationInBytes(), taskStatus.getSystemMemoryReservationInBytes()));
                taskStatusResponseStats.incrementTaskStatusSystemMemoryIncreased();
                set(taskStatus);
            }
            else if (queuedPartitionedDriversDifference > taskStatusSignificanceFactor * initialTaskStatus.getQueuedPartitionedDrivers()) {
                future.cancel(false);
                taskStatusResponseStats.incrementQueuedPartitionedDriversIncreased();
                set(taskStatus);
            }
            else if (runningPartitionedDriversDifference > taskStatusSignificanceFactor * initialTaskStatus.getRunningPartitionedDrivers()) {
                future.cancel(false);
//                System.out.println("TaskStatus " + taskId + ": Running partition drivers, waited: " + currentDuration);
                taskStatusResponseStats.incrementRunningPartitionedDriversIncreased();
                set(taskStatus);
            }
            else if (fullGcCountDifference > taskStatusSignificanceFactor * initialTaskStatus.getFullGcCount()) {
                future.cancel(false);
//                System.out.println("TaskStatus " + taskId + ": Full gc count difference, waited: " + currentDuration);
                taskStatusResponseStats.incrementFullGcCountIncreased();
                set(taskStatus);
            }
            else if (physicalWrittenDataSizeDifferenceInBytes > taskStatusSignificanceFactor * initialTaskStatus.getPhysicalWrittenDataSizeInBytes()) {
                future.cancel(false);
//                System.out.println("TaskStatus " + taskId + ": Physical written data size, waited: " + currentDuration);
                taskStatusResponseStats.incrementPhysicalWrittenDataSizeIncreased();
                set(taskStatus);
            }
            else if (taskStatus.isOutputBufferOverutilized()) {
                future.cancel(false);
//                System.out.println("TaskStatus " + taskId + ": Output buffer overutilized, waited: " + currentDuration);
                taskStatusResponseStats.incrementOutputBufferOverutilized();
                set(taskStatus);
            }
            else if (!taskStatus.getCompletedDriverGroups().isEmpty()) {
                future.cancel(false);
//                System.out.println("TaskStatus " + taskId + ": Completed driver groups, waited: " + currentDuration);
                taskStatusResponseStats.incrementCompletedDriverGroupsIncreased();
                set(taskStatus);
            }
            // If no significant differences are present then continue to wait
            else {
                // If close to reaching timeout, adjust delay time to be remaining time until timeout reached
                timeoutExecutor.schedule(this::tick, randomizeWaitTime(interval).toMillis(), MILLISECONDS);
            }
        }
    }
}

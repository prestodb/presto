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

import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GracefulShutdownHandler
{
    private static final Logger log = Logger.get(GracefulShutdownHandler.class);
    private static final Duration LIFECYCLE_STOP_TIMEOUT = new Duration(30, SECONDS);

    private final ScheduledExecutorService shutdownHandler = newSingleThreadScheduledExecutor(threadsNamed("shutdown-handler-%s"));
    private final ExecutorService lifeCycleStopper = newSingleThreadExecutor(threadsNamed("lifecycle-stopper-%s"));
    private final LifeCycleManager lifeCycleManager;
    private final TaskManager sqlTaskManager;
    private final boolean isCoordinator;
    private final ShutdownAction shutdownAction;
    private final Duration gracePeriod;

    @GuardedBy("this")
    private boolean shutdownRequested;
    @GuardedBy("this")
    private boolean cancelShutdown;
    @GuardedBy("this")
    private Future shutdownTaskFuture;

    @Inject
    public GracefulShutdownHandler(
            TaskManager sqlTaskManager,
            ServerConfig serverConfig,
            ShutdownAction shutdownAction,
            LifeCycleManager lifeCycleManager)
    {
        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager is null");
        this.shutdownAction = requireNonNull(shutdownAction, "shutdownAction is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.isCoordinator = requireNonNull(serverConfig, "serverConfig is null").isCoordinator();
        this.gracePeriod = serverConfig.getGracePeriod();
    }

    public synchronized void requestShutdown()
    {
        log.info("Shutdown requested");

        if (isCoordinator) {
            throw new UnsupportedOperationException("Cannot shutdown coordinator");
        }

        if (isShutdownRequested()) {
            // shutdown already in progress
            return;
        }

        setShutdownRequested(true);

        //wait for a grace period to start the shutdown sequence
        shutdownTaskFuture = shutdownHandler.schedule(() -> {
            shutdownAction.onShutdownStart();
            List<TaskInfo> activeTasks = getActiveTasks();
            while (activeTasks.size() > 0) {
                CountDownLatch countDownLatch = new CountDownLatch(activeTasks.size());

                for (TaskInfo taskInfo : activeTasks) {
                    sqlTaskManager.addStateChangeListener(taskInfo.getTaskStatus().getTaskId(), newState -> {
                        if (newState.isDone()) {
                            countDownLatch.countDown();
                        }
                    });
                }

                log.info("Waiting for all tasks to finish");

                try {
                    countDownLatch.await();
                }
                catch (InterruptedException e) {
                    log.warn("Interrupted while waiting for all tasks to finish");
                    synchronized (GracefulShutdownHandler.this) {
                        if (cancelShutdown) {
                            log.warn("Not waiting any longer for tasks to finish as request was made to cancel shutdown");
                            cancelShutdown = false; // reset flag so that future requestShutdown
                            shutdownAction.onCancelWaitForTasksToComplete();
                            currentThread().interrupt();
                            return;
                        }
                    }
                    currentThread().interrupt();
                }
                activeTasks = getActiveTasks();
            }
            synchronized (GracefulShutdownHandler.this) {
                if (cancelShutdown) {
                    shutdownAction.onCancelJVMShutdown();
                    log.warn("Skipping JVM shutdown since request was made to cancel shutdown");
                    cancelShutdown = false; // reset flag so that future shutdown requests can be honored
                    return;
                }
            }
            // wait for another grace period for all task states to be observed by the coordinator
            sleepUninterruptibly(gracePeriod.toMillis(), MILLISECONDS);
            Future<?> stopLifeCycleFuture = lifeCycleStopper.submit(() -> {
                lifeCycleManager.stop();
                return null;
            });

            // terminate the jvm if life cycle cannot be stopped in a timely manner
            try {
                stopLifeCycleFuture.get(LIFECYCLE_STOP_TIMEOUT.toMillis(), MILLISECONDS);
            }
            catch (TimeoutException e) {
                log.warn(e, "Timed out waiting for the life cycle to stop");
            }
            catch (InterruptedException e) {
                log.warn(e, "Interrupted while waiting for the life cycle to stop");
                currentThread().interrupt();
            }
            catch (ExecutionException e) {
                log.warn(e, "Problem stopping the life cycle");
            }
            shutdownAction.onShutdownComplete();
        }, 0, MILLISECONDS);
    }

    private List<TaskInfo> getActiveTasks()
    {
        return sqlTaskManager.getAllTaskInfo()
                .stream()
                .filter(taskInfo -> !taskInfo.getTaskStatus().getState().isDone())
                .collect(toImmutableList());
    }

    private synchronized void setShutdownRequested(boolean shutdownRequested)
    {
        this.shutdownRequested = shutdownRequested;
    }

    public synchronized boolean isShutdownRequested()
    {
        return shutdownRequested;
    }

    public synchronized void cancelShutdown()
    {
        cancelShutdown(true);
    }

    @VisibleForTesting
    synchronized void cancelShutdown(boolean interruptWaitForTasksToFinish)
    {
        cancelShutdown = true;
        setShutdownRequested(false);
        if (shutdownTaskFuture != null && interruptWaitForTasksToFinish) {
            boolean cancelled = shutdownTaskFuture.cancel(true);
            log.info(cancelled ? "Successful in cancelling shutdown task" : "Unsuccessful in cancelling shutdown task");
        }
    }
}

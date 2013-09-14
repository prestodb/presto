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
package com.facebook.presto.hive.util;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Executor that can suspend the execution of all un-run tasks and resume their execution
 * at a later time without blocking any threads. Suspending does not affect tasks that are
 * already running. This Executor follows all standard Executor semantics. However, if
 * the underlying Executor is shutdown, this SuspendingExecutor will fail to resume from
 * a suspended state.
 */
@ThreadSafe
public class SuspendingExecutor
        implements Executor
{
    private final Executor executor;
    private final Queue<Runnable> taskQueue = new LinkedBlockingQueue<>();

    @GuardedBy("this")
    private boolean suspended;
    @GuardedBy("this")
    private int suspendedTaskCount;

    public SuspendingExecutor(Executor executor)
    {
        this.executor = checkNotNull(executor, "executor is null");
    }

    @Override
    public void execute(Runnable task)
    {
        taskQueue.add(task);
        activateOneTask();
    }

    private void activateOneTask()
    {
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                if (acquire()) {
                    Runnable task = taskQueue.poll();
                    checkState(task != null, "No task to execute");
                    task.run();
                }
            }
        });
    }

    private synchronized boolean acquire()
    {
        if (suspended) {
            suspendedTaskCount++;
        }
        return !suspended;
    }

    public synchronized void resume()
    {
        suspended = false;

        // Copy out and reset the suspendedTaskCount
        int count = suspendedTaskCount;
        suspendedTaskCount = 0;
        // Do this in case the current thread also happens to execute the activated tasks inline with the below for-loop (same thread executors)
        // Otherwise will result in inconsistent state

        for (int i = 0; i < count; i++) {
            activateOneTask();
        }
    }

    public synchronized void suspend()
    {
        suspended = true;
    }
}

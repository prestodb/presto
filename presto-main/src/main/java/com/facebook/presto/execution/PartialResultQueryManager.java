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

import com.facebook.presto.execution.scheduler.PartialResultQueryTaskTracker;
import com.google.inject.Inject;

import javax.annotation.PreDestroy;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static java.util.Comparator.comparing;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class PartialResultQueryManager
{
    private final AtomicReference<ScheduledExecutorService> executor = new AtomicReference<>();
    private final PriorityBlockingQueue<PartialResultQueryTaskTracker> queue;

    @Inject
    public PartialResultQueryManager()
    {
        this.queue = new PriorityBlockingQueue<>(1, comparing(PartialResultQueryTaskTracker::getMaxEndTime));
    }

    private void startExecutor()
    {
        // Start the executor if not already started
        if (executor.compareAndSet(null, newSingleThreadScheduledExecutor(threadsNamed("partial-result-query-manager-%s")))) {
            executor.get().scheduleWithFixedDelay(this::checkAndCancelTasks, 1, 1, TimeUnit.SECONDS);
        }
    }

    public void addQueryTaskTracker(PartialResultQueryTaskTracker queryTaskTracker)
    {
        startExecutor();
        queue.add(queryTaskTracker);
    }

    public void checkAndCancelTasks()
    {
        long currentTime = System.nanoTime();
        while (!queue.isEmpty() && currentTime >= queue.peek().getMaxEndTime()) {
            PartialResultQueryTaskTracker queryTracker = queue.poll();
            // Reached max task end time. Cancel pending tasks.
            queryTracker.cancelUnfinishedTasks();
        }
    }

    public int getQueueSize()
    {
        return queue.size();
    }

    @PreDestroy
    public void stop()
    {
        if (executor.get() != null) {
            executor.get().shutdownNow();
        }
    }
}

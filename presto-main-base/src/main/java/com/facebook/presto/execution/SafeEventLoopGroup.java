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
import com.sun.management.ThreadMXBean;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/***
 *  One observation about event loop is if submitted task fails, it could kill the thread but the event loop group will not create a new one.
 *  Here, we wrap it as safe event loop so that if any submitted job fails, we chose to log the error and fail the entire task.
 */

public class SafeEventLoopGroup
        extends DefaultEventLoopGroup
{
    private static final Logger log = Logger.get(SafeEventLoopGroup.class);
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    private static final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

    private final long slowMethodThresholdOnEventLoopInNanos;

    public SafeEventLoopGroup(int nThreads, ThreadFactory threadFactory, long slowMethodThresholdOnEventLoopInNanos)
    {
        super(nThreads, threadFactory);
        this.slowMethodThresholdOnEventLoopInNanos = slowMethodThresholdOnEventLoopInNanos;
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args)
    {
        return new SafeEventLoop(this, executor);
    }

    public class SafeEventLoop
            extends DefaultEventLoop
    {
        public SafeEventLoop(EventLoopGroup parent, Executor executor)
        {
            super(parent, executor);
        }

        @Override
        protected void run()
        {
            do {
                Runnable task = takeTask();
                if (task != null) {
                    try {
                        runTask(task);
                    }
                    catch (Throwable t) {
                        log.error("Error running task on event loop", t);
                    }
                    updateLastExecutionTime();
                }
            }
            while (!this.confirmShutdown());
        }

        public void execute(Runnable task, Consumer<Throwable> failureHandler, SchedulerStatsTracker statsTracker, String methodSignature)
        {
            requireNonNull(task, "task is null");
            long initialGCTime = getTotalGCTime();
            long start = THREAD_MX_BEAN.getCurrentThreadCpuTime();
            this.execute(() -> {
                try {
                    task.run();
                }
                catch (Throwable t) {
                    log.error("Error executing task on event loop", t);
                    if (failureHandler != null) {
                        failureHandler.accept(t);
                    }
                }
                finally {
                    long currentGCTime = getTotalGCTime();
                    long cpuTimeInNanos = THREAD_MX_BEAN.getCurrentThreadCpuTime() - start - (currentGCTime - initialGCTime);

                    statsTracker.recordEventLoopMethodExecutionCpuTime(cpuTimeInNanos);
                    if (cpuTimeInNanos > slowMethodThresholdOnEventLoopInNanos) {
                        log.warn("Slow method execution on event loop: %s took %s milliseconds", methodSignature, NANOSECONDS.toMillis(cpuTimeInNanos));
                    }
                }
            });
        }

        public <T> void execute(Supplier<T> task, Consumer<T> successHandler, Consumer<Throwable> failureHandler)
        {
            requireNonNull(task, "task is null");
            this.execute(() -> {
                try {
                    T result = task.get();
                    if (successHandler != null) {
                        successHandler.accept(result);
                    }
                }
                catch (Throwable t) {
                    log.error("Error executing task on event loop", t);
                    if (failureHandler != null) {
                        failureHandler.accept(t);
                    }
                }
            });
        }
    }

    private long getTotalGCTime()
    {
        return gcBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
    }
}

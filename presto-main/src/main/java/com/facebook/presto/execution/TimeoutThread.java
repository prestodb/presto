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

import io.airlift.units.Duration;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * TimeoutThread spins up a background task
 * that interrupts given thread after the timeout expires.
 */
public class TimeoutThread
        implements AutoCloseable
{
    private final Future<?> future;

    /**
     * @param thread Thread to be interrupted after timeout expires.
     * @param executor Executor to run the background task.
     * @param timeout Expiration time of the thread.
     */
    public TimeoutThread(Thread thread, ScheduledExecutorService executor, Duration timeout)
    {
        requireNonNull(thread, "thread is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(timeout, "timeout is null");

        this.future = executor.schedule(thread::interrupt, timeout.roundTo(NANOSECONDS), NANOSECONDS);
    }

    @Override
    public void close()
    {
        future.cancel(true);
    }
}

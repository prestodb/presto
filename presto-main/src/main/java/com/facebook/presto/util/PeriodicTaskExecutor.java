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
package com.facebook.presto.util;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PeriodicTaskExecutor
        implements AutoCloseable
{
    private final long delayTargetMillis;
    private final ScheduledExecutorService executor;
    private final Runnable runnable;
    private final LongUnaryOperator nextDelayFunction;
    private final AtomicBoolean started = new AtomicBoolean();

    private volatile long delayMillis;
    private volatile ScheduledFuture<?> scheduledFuture;
    private volatile boolean stopped;

    public PeriodicTaskExecutor(long delayTargetMillis, ScheduledExecutorService executor, Runnable runnable)
    {
        this(delayTargetMillis, 0, executor, runnable, PeriodicTaskExecutor::nextDelayWithJitterMillis);
    }

    public PeriodicTaskExecutor(long delayTargetMillis, long initDelayMillis, ScheduledExecutorService executor, Runnable runnable)
    {
        this(delayTargetMillis, initDelayMillis, executor, runnable, PeriodicTaskExecutor::nextDelayWithJitterMillis);
    }

    public PeriodicTaskExecutor(long delayTargetMillis, long initDelayMillis, ScheduledExecutorService executor, Runnable runnable, LongUnaryOperator nextDelayFunction)
    {
        checkArgument(delayTargetMillis > 0, "delayTargetMillis must be > 0");
        checkArgument(initDelayMillis >= 0, "initDelayMillis must be > 0");
        this.delayTargetMillis = delayTargetMillis;
        this.executor = requireNonNull(executor, "executor is null");
        this.runnable = requireNonNull(runnable, "runnable is null");
        this.nextDelayFunction = requireNonNull(nextDelayFunction, "nextDelayFunction is null");
        this.delayMillis = initDelayMillis;
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            tick();
        }
    }

    private void tick()
    {
        scheduledFuture = executor.schedule(this::run, delayMillis, MILLISECONDS);
    }

    private void run()
    {
        forceRun();
        delayMillis = nextDelayFunction.applyAsLong(delayTargetMillis);
        if (!stopped) {
            tick();
        }
    }

    public void forceRun()
    {
        executor.execute(runnable);
    }

    public void stop()
    {
        if (started.get()) {
            stopped = true;
            scheduledFuture.cancel(false);
        }
    }

    private static long nextDelayWithJitterMillis(long delayTargetMillis)
    {
        double minSleepTimeMillis = delayTargetMillis / 2.0;
        return round((minSleepTimeMillis + ThreadLocalRandom.current().nextDouble() * delayTargetMillis));
    }

    @Override
    public void close()
            throws Exception
    {
        stop();
    }
}

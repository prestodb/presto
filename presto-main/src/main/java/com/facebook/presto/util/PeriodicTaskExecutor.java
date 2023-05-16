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

import java.util.Optional;
import java.util.concurrent.ExecutorService;
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
    private final ExecutorService executor;
    private final Optional<ScheduledExecutorService> scheduledExecutor;
    private final Runnable runnable;
    private final LongUnaryOperator nextDelayFunction;
    private final AtomicBoolean started = new AtomicBoolean();

    private volatile long delayMillis;
    private volatile ScheduledFuture<?> scheduledFuture;
    private volatile boolean stopped;

    public PeriodicTaskExecutor(long delayTargetMillis, ScheduledExecutorService scheduledExecutor, Runnable runnable)
    {
        this(delayTargetMillis, 0, scheduledExecutor, Optional.of(scheduledExecutor), runnable, PeriodicTaskExecutor::nextDelayWithJitterMillis);
    }

    public PeriodicTaskExecutor(long delayTargetMillis, long initDelayMillis, ExecutorService executor, Optional<ScheduledExecutorService> scheduledExecutor, Runnable runnable, LongUnaryOperator nextDelayFunction)
    {
        checkArgument(delayTargetMillis >= 0, "delayTargetMillis must be >= 0");
        checkArgument(initDelayMillis >= 0, "initDelayMillis must be >= 0");
        this.delayTargetMillis = delayTargetMillis;
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.runnable = requireNonNull(runnable, "runnable is null");
        this.nextDelayFunction = requireNonNull(nextDelayFunction, "nextDelayFunction is null");
        this.delayMillis = initDelayMillis;

        if (delayTargetMillis == 0) {
            checkArgument(!scheduledExecutor.isPresent(), "scheduledExecutor must not be present when delayTargetMillis is 0");
        }
        else {
            checkArgument(scheduledExecutor.isPresent(), "scheduledExecutor must be present when delayTargetMillis is not 0");
        }
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            tick();
        }
    }

    public void tick()
    {
        if (!stopped) {
            if (scheduledExecutor.isPresent()) {
                scheduledFuture = scheduledExecutor.get().schedule(this::run, delayMillis, MILLISECONDS);
            }
            else {
                forceRun();
            }
        }
    }

    private void run()
    {
        forceRun();
        delayMillis = nextDelayFunction.applyAsLong(delayTargetMillis);
        tick();
    }

    public void forceRun()
    {
        executor.execute(() -> {
            try {
                runnable.run();
            }
            catch (Throwable t) {
                t.printStackTrace();
            }
        });
    }

    public void stop()
    {
        if (started.get()) {
            stopped = true;

            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
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

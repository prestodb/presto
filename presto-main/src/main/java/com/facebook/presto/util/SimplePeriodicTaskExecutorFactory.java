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

import com.facebook.presto.resourcemanager.ForPeriodicTaskExecutor;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;

public class SimplePeriodicTaskExecutorFactory
        implements AutoCloseable, PeriodicTaskExecutorFactory
{
    private final ExecutorService executor;
    private final Optional<ScheduledExecutorService> scheduledExecutor;
    private final LongUnaryOperator nextDelayFunction;

    @Inject
    public SimplePeriodicTaskExecutorFactory(@ForPeriodicTaskExecutor ExecutorService executor, @ForPeriodicTaskExecutor Optional<ScheduledExecutorService> scheduledExecutor)
    {
        this(executor, scheduledExecutor, SimplePeriodicTaskExecutorFactory::nextDelayWithJitterMillis);
    }

    public SimplePeriodicTaskExecutorFactory(ExecutorService executor, Optional<ScheduledExecutorService> scheduledExecutor, LongUnaryOperator nextDelayFunction)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.nextDelayFunction = requireNonNull(nextDelayFunction, "nextDelayFunction is null");
    }

    @Override
    public PeriodicTaskExecutor createPeriodicTaskExecutor(long delayTargetMillis, long initDelayMillis, Runnable runnable)
    {
        if (delayTargetMillis == 0) {
            checkArgument(!scheduledExecutor.isPresent(), "scheduledExecutor must not be present when delayTargetMillis is 0");
        }
        else {
            checkArgument(scheduledExecutor.isPresent(), "scheduledExecutor must be present when delayTargetMillis is not 0");
        }
        return new PeriodicTaskExecutor(delayTargetMillis, initDelayMillis, executor, scheduledExecutor, runnable, nextDelayFunction);
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
        executor.shutdownNow();
        scheduledExecutor.ifPresent(ExecutorService::shutdownNow);
    }
}

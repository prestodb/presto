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
package com.facebook.presto.operator;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Methods setWithDelay and reset should be used in pairs;
 * usually follow the following idiom:
 * <pre> {@code
 * DriverYieldSignal signal = ...;
 * signal.setWithDelay(duration, executor);
 * try {
 *     // block
 * } finally {
 *     signal.reset();
 * }
 * </pre>
 */
@ThreadSafe
public class DriverYieldSignal
{
    @GuardedBy("this")
    private long runningSequence;

    @GuardedBy("this")
    private ScheduledFuture<?> yieldFuture;

    private final AtomicBoolean yield = new AtomicBoolean();

    public synchronized void setWithDelay(long maxRunNanos, ScheduledExecutorService executor)
    {
        checkState(yieldFuture == null, "there is an ongoing yield");
        checkState(!isSet(), "yield while driver was not running");

        this.runningSequence++;
        long expectedRunningSequence = this.runningSequence;
        yieldFuture = executor.schedule(() -> {
            synchronized (this) {
                if (expectedRunningSequence == runningSequence && yieldFuture != null) {
                    yield.set(true);
                }
            }
        }, maxRunNanos, NANOSECONDS);
    }

    public synchronized void reset()
    {
        checkState(yieldFuture != null, "there is no ongoing yield");
        yield.set(false);
        yieldFuture.cancel(true);
        yieldFuture = null;
    }

    public boolean isSet()
    {
        return yield.get();
    }

    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("yieldScheduled", yieldFuture != null)
                .add("yield", yield.get())
                .toString();
    }

    @VisibleForTesting
    public synchronized void forceYieldForTesting()
    {
        yield.set(true);
    }

    @VisibleForTesting
    public synchronized void resetYieldForTesting()
    {
        yield.set(false);
    }
}

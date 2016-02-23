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
package com.facebook.presto.server.remotetask;

import com.google.common.base.Ticker;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
class Backoff
{
    private final long maxFailureIntervalNanos;
    private final Ticker ticker;
    private final long[] backoffDelayIntervalsNanos;

    private long lastSuccessTime;
    private long lastFailureTime;
    private long failureCount;

    public Backoff(Duration maxFailureInterval)
    {
        this(maxFailureInterval,
                new Duration(0, MILLISECONDS),
                new Duration(50, MILLISECONDS),
                new Duration(100, MILLISECONDS),
                new Duration(200, MILLISECONDS),
                new Duration(500, MILLISECONDS));
    }

    public Backoff(Duration maxFailureInterval, Duration... backoffDelayIntervals)
    {
        this(maxFailureInterval, Ticker.systemTicker(), backoffDelayIntervals);
    }

    public Backoff(Duration maxFailureInterval, Ticker ticker, Duration... backoffDelayIntervals)
    {
        requireNonNull(maxFailureInterval, "maxFailureInterval is null");
        requireNonNull(ticker, "ticker is null");
        requireNonNull(backoffDelayIntervals, "backoffDelayIntervals is null");
        checkArgument(backoffDelayIntervals.length > 0, "backoffDelayIntervals must contain at least one entry");

        this.maxFailureIntervalNanos = maxFailureInterval.roundTo(NANOSECONDS);
        this.ticker = ticker;
        this.backoffDelayIntervalsNanos = new long[backoffDelayIntervals.length];
        for (int i = 0; i < backoffDelayIntervals.length; i++) {
            this.backoffDelayIntervalsNanos[i] = backoffDelayIntervals[i].roundTo(NANOSECONDS);
        }

        this.lastSuccessTime = this.ticker.read();
    }

    public synchronized long getFailureCount()
    {
        return failureCount;
    }

    public synchronized Duration getTimeSinceLastSuccess()
    {
        long lastSuccessfulRequest = this.lastSuccessTime;
        long value = ticker.read() - lastSuccessfulRequest;
        return new Duration(value, NANOSECONDS).convertToMostSuccinctTimeUnit();
    }

    public synchronized void success()
    {
        lastSuccessTime = ticker.read();
        failureCount = 0;
        lastFailureTime = 0;
    }

    /**
     * @return true if the failure is considered permanent
     */
    public synchronized boolean failure()
    {
        long lastSuccessfulRequest = this.lastSuccessTime;
        long now = ticker.read();

        lastFailureTime = now;

        failureCount++;

        long timeSinceLastSuccess = now - lastSuccessfulRequest;
        return timeSinceLastSuccess >= maxFailureIntervalNanos;
    }

    public synchronized long getBackoffDelayNanos()
    {
        int failureCount = (int) Math.min(backoffDelayIntervalsNanos.length, this.failureCount);
        if (failureCount == 0) {
            return 0;
        }
        // expected amount of time to delay from the last failure time
        long currentDelay = backoffDelayIntervalsNanos[failureCount - 1];

        // calculate expected delay from now
        long nanosSinceLastFailure = ticker.read() - lastFailureTime;
        return Math.max(0, currentDelay - nanosSinceLastFailure);
    }
}

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
package io.prestosql.server.remotetask;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class Backoff
{
    private static final int MIN_RETRIES = 3;
    private static final List<Duration> DEFAULT_BACKOFF_DELAY_INTERVALS = ImmutableList.<Duration>builder()
            .add(new Duration(0, MILLISECONDS))
            .add(new Duration(50, MILLISECONDS))
            .add(new Duration(100, MILLISECONDS))
            .add(new Duration(200, MILLISECONDS))
            .add(new Duration(500, MILLISECONDS))
            .build();

    private final int minTries;
    private final long maxFailureIntervalNanos;
    private final Ticker ticker;
    private final long[] backoffDelayIntervalsNanos;

    private long firstFailureTime;
    private long lastFailureTime;
    private long failureCount;
    private long failureRequestTimeTotal;

    private long lastRequestStart;

    public Backoff(Duration maxFailureInterval)
    {
        this(maxFailureInterval, Ticker.systemTicker());
    }

    public Backoff(Duration maxFailureInterval, Ticker ticker)
    {
        this(MIN_RETRIES, maxFailureInterval, ticker, DEFAULT_BACKOFF_DELAY_INTERVALS);
    }

    @VisibleForTesting
    public Backoff(int minTries, Duration maxFailureInterval, Ticker ticker, List<Duration> backoffDelayIntervals)
    {
        checkArgument(minTries > 0, "minTries must be at least 1");
        requireNonNull(maxFailureInterval, "maxFailureInterval is null");
        requireNonNull(ticker, "ticker is null");
        requireNonNull(backoffDelayIntervals, "backoffDelayIntervals is null");
        checkArgument(!backoffDelayIntervals.isEmpty(), "backoffDelayIntervals must contain at least one entry");

        this.minTries = minTries;
        this.maxFailureIntervalNanos = maxFailureInterval.roundTo(NANOSECONDS);
        this.ticker = ticker;
        this.backoffDelayIntervalsNanos = backoffDelayIntervals.stream()
                .mapToLong(duration -> duration.roundTo(NANOSECONDS))
                .toArray();
    }

    public synchronized long getFailureCount()
    {
        return failureCount;
    }

    public synchronized Duration getFailureDuration()
    {
        if (firstFailureTime == 0) {
            return new Duration(0, MILLISECONDS);
        }
        long value = ticker.read() - firstFailureTime;
        return new Duration(value, NANOSECONDS);
    }

    public synchronized Duration getFailureRequestTimeTotal()
    {
        return new Duration(max(0, failureRequestTimeTotal), NANOSECONDS);
    }

    public synchronized void startRequest()
    {
        lastRequestStart = ticker.read();
    }

    public synchronized void success()
    {
        lastRequestStart = 0;
        firstFailureTime = 0;
        failureCount = 0;
        lastFailureTime = 0;
    }

    /**
     * @return true if the failure is considered permanent
     */
    public synchronized boolean failure()
    {
        long now = ticker.read();

        lastFailureTime = now;
        failureCount++;
        if (lastRequestStart != 0) {
            failureRequestTimeTotal += now - lastRequestStart;
            lastRequestStart = 0;
        }

        if (firstFailureTime == 0) {
            firstFailureTime = now;
            // can not fail on first failure
            return false;
        }

        if (failureCount < minTries) {
            return false;
        }

        long failureDuration = now - firstFailureTime;
        return failureDuration >= maxFailureIntervalNanos;
    }

    public synchronized long getBackoffDelayNanos()
    {
        int failureCount = (int) min(backoffDelayIntervalsNanos.length, this.failureCount);
        if (failureCount == 0) {
            return 0;
        }
        // expected amount of time to delay from the last failure time
        long currentDelay = backoffDelayIntervalsNanos[failureCount - 1];

        // calculate expected delay from now
        long nanosSinceLastFailure = ticker.read() - lastFailureTime;
        return max(0, currentDelay - nanosSinceLastFailure);
    }
}

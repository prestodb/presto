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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class Backoff
{
    private static final List<Duration> DEFAULT_BACKOFF_DELAY_INTERVALS = ImmutableList.<Duration>builder()
            .add(new Duration(0, MILLISECONDS))
            .add(new Duration(50, MILLISECONDS))
            .add(new Duration(100, MILLISECONDS))
            .add(new Duration(200, MILLISECONDS))
            .add(new Duration(500, MILLISECONDS))
            .build();

    private final long minFailureIntervalNanos;
    private final long maxFailureIntervalNanos;
    private final Ticker ticker;
    private final long[] backoffDelayIntervalsNanos;
    private final long createTime;

    private long lastSuccessTime;
    private long firstRequestAfterSuccessTime;
    private long lastFailureTime;
    private long failureCount;

    public Backoff(Duration executionElapsedTime, Duration minFailureInterval, Duration maxFailureInterval)
    {
        this(executionElapsedTime, minFailureInterval, maxFailureInterval, Ticker.systemTicker());
    }

    public Backoff(Duration executionElapsedTime, Duration minFailureInterval, Duration maxFailureInterval, Ticker ticker)
    {
        this(executionElapsedTime, minFailureInterval, maxFailureInterval, ticker, DEFAULT_BACKOFF_DELAY_INTERVALS);
    }

    @VisibleForTesting
    public Backoff(Duration executionElapsedTime, Duration minFailureInterval, Duration maxFailureInterval, Ticker ticker, List<Duration> backoffDelayIntervals)
    {
        requireNonNull(executionElapsedTime, "executionElapsedTime is null");
        requireNonNull(minFailureInterval, "minFailureInterval is null");
        requireNonNull(maxFailureInterval, "maxFailureInterval is null");
        requireNonNull(ticker, "ticker is null");
        requireNonNull(backoffDelayIntervals, "backoffDelayIntervals is null");
        checkArgument(!backoffDelayIntervals.isEmpty(), "backoffDelayIntervals must contain at least one entry");
        checkArgument(maxFailureInterval.compareTo(minFailureInterval) >= 0, "maxFailureInterval is less than minFailureInterval");

        this.minFailureIntervalNanos = minFailureInterval.roundTo(NANOSECONDS);
        this.maxFailureIntervalNanos = maxFailureInterval.roundTo(NANOSECONDS);
        this.ticker = ticker;
        this.backoffDelayIntervalsNanos = backoffDelayIntervals.stream()
                .mapToLong(duration -> duration.roundTo(NANOSECONDS))
                .toArray();

        this.lastSuccessTime = this.ticker.read();
        this.firstRequestAfterSuccessTime = Long.MIN_VALUE;
        this.createTime = ticker.read() - executionElapsedTime.roundTo(NANOSECONDS);
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

    public synchronized void startRequest()
    {
        if (firstRequestAfterSuccessTime < lastSuccessTime) {
            firstRequestAfterSuccessTime = ticker.read();
        }
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

        long failureInterval;
        if (lastSuccessfulRequest - createTime > maxFailureIntervalNanos) {
            failureInterval = maxFailureIntervalNanos;
        }
        else {
            failureInterval = Math.max(lastSuccessfulRequest - createTime, minFailureIntervalNanos);
        }
        long failureDuration;
        if (firstRequestAfterSuccessTime < lastSuccessTime) {
            // If user didn't call startRequest(), use the time of the last success
            failureDuration = now - lastSuccessfulRequest;
        }
        else {
            // Otherwise only count the time since the first request that started failing
            failureDuration = now - firstRequestAfterSuccessTime;
        }
        return failureDuration >= failureInterval;
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

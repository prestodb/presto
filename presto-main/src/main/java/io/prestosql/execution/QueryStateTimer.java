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
package io.prestosql.execution;

import com.google.common.base.Ticker;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class QueryStateTimer
{
    private final Ticker ticker;

    private final DateTime createTime = DateTime.now();

    private final long createNanos;
    private final AtomicReference<Long> beginResourceWaitingNanos = new AtomicReference<>();
    private final AtomicReference<Long> beginPlanningNanos = new AtomicReference<>();
    private final AtomicReference<Long> beginFinishingNanos = new AtomicReference<>();
    private final AtomicReference<Long> endNanos = new AtomicReference<>();

    private final AtomicReference<Duration> queuedTime = new AtomicReference<>();
    private final AtomicReference<Duration> resourceWaitingTime = new AtomicReference<>();
    private final AtomicReference<Duration> executionTime = new AtomicReference<>();
    private final AtomicReference<Duration> planningTime = new AtomicReference<>();
    private final AtomicReference<Duration> finishingTime = new AtomicReference<>();

    private final AtomicReference<Long> beginAnalysisNanos = new AtomicReference<>();
    private final AtomicReference<Duration> analysisTime = new AtomicReference<>();

    private final AtomicReference<Long> beginDistributedPlanningNanos = new AtomicReference<>();
    private final AtomicReference<Duration> distributedPlanningTime = new AtomicReference<>();

    private final AtomicReference<Long> lastHeartbeatNanos;

    public QueryStateTimer(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.createNanos = tickerNanos();
        this.lastHeartbeatNanos = new AtomicReference<>(createNanos);
    }

    //
    // State transitions
    //

    public void beginWaitingForResources()
    {
        beginWaitingForResources(tickerNanos());
    }

    private void beginWaitingForResources(long now)
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos, now));
        beginResourceWaitingNanos.compareAndSet(null, now);
    }

    public void beginPlanning()
    {
        beginPlanning(tickerNanos());
    }

    private void beginPlanning(long now)
    {
        beginWaitingForResources(now);
        resourceWaitingTime.compareAndSet(null, nanosSince(beginResourceWaitingNanos, now));
        beginPlanningNanos.compareAndSet(null, now);
    }

    public void beginStarting()
    {
        beginStarting(tickerNanos());
    }

    private void beginStarting(long now)
    {
        beginPlanning(now);
        planningTime.compareAndSet(null, nanosSince(beginPlanningNanos, now));
    }

    public void beginRunning()
    {
        beginRunning(tickerNanos());
    }

    private void beginRunning(long now)
    {
        beginStarting(now);
    }

    public void beginFinishing()
    {
        beginFinishing(tickerNanos());
    }

    private void beginFinishing(long now)
    {
        beginRunning(now);
        beginFinishingNanos.compareAndSet(null, now);
    }

    public void endQuery()
    {
        endQuery(tickerNanos());
    }

    private void endQuery(long now)
    {
        beginFinishing(now);
        finishingTime.compareAndSet(null, nanosSince(beginFinishingNanos, now));
        executionTime.compareAndSet(null, nanosSince(beginPlanningNanos, now));
        endNanos.compareAndSet(null, now);
    }

    //
    //  Additional timings
    //

    public void beginAnalyzing()
    {
        beginAnalysisNanos.compareAndSet(null, tickerNanos());
    }

    public void endAnalysis()
    {
        analysisTime.compareAndSet(null, nanosSince(beginAnalysisNanos, tickerNanos()));
    }

    public void beginDistributedPlanning()
    {
        beginDistributedPlanningNanos.compareAndSet(null, tickerNanos());
    }

    public void endDistributedPlanning()
    {
        distributedPlanningTime.compareAndSet(null, nanosSince(beginDistributedPlanningNanos, tickerNanos()));
    }

    public void recordHeartbeat()
    {
        lastHeartbeatNanos.set(tickerNanos());
    }

    //
    // Stats
    //

    public DateTime getCreateTime()
    {
        return createTime;
    }

    public Optional<DateTime> getExecutionStartTime()
    {
        return toDateTime(beginPlanningNanos);
    }

    public Duration getElapsedTime()
    {
        if (endNanos.get() != null) {
            return succinctNanos(endNanos.get() - createNanos);
        }
        return nanosSince(createNanos, tickerNanos());
    }

    public Duration getQueuedTime()
    {
        Duration queuedTime = this.queuedTime.get();
        if (queuedTime != null) {
            return queuedTime;
        }

        // if queue time is not set, the query is still queued
        return getElapsedTime();
    }

    public Duration getResourceWaitingTime()
    {
        return getDuration(resourceWaitingTime, beginResourceWaitingNanos);
    }

    public Duration getPlanningTime()
    {
        return getDuration(planningTime, beginPlanningNanos);
    }

    public Duration getFinishingTime()
    {
        return getDuration(finishingTime, beginFinishingNanos);
    }

    public Duration getExecutionTime()
    {
        return getDuration(executionTime, beginPlanningNanos);
    }

    public Optional<DateTime> getEndTime()
    {
        return toDateTime(endNanos);
    }

    public Duration getAnalysisTime()
    {
        return getDuration(analysisTime, beginAnalysisNanos);
    }

    public Duration getDistributedPlanningTime()
    {
        return getDuration(distributedPlanningTime, beginDistributedPlanningNanos);
    }

    public DateTime getLastHeartbeat()
    {
        return toDateTime(lastHeartbeatNanos.get());
    }

    //
    // Helper methods
    //

    private long tickerNanos()
    {
        return ticker.read();
    }

    private static Duration nanosSince(AtomicReference<Long> start, long end)
    {
        Long startNanos = start.get();
        if (startNanos == null) {
            throw new IllegalStateException("Start time not set");
        }
        return nanosSince(startNanos, end);
    }

    private static Duration nanosSince(long start, long now)
    {
        return succinctNanos(now - start);
    }

    private Duration getDuration(AtomicReference<Duration> finalDuration, AtomicReference<Long> start)
    {
        Duration duration = finalDuration.get();
        if (duration != null) {
            return duration;
        }
        Long startNanos = start.get();
        if (startNanos != null) {
            return nanosSince(startNanos, tickerNanos());
        }
        return new Duration(0, MILLISECONDS);
    }

    private Optional<DateTime> toDateTime(AtomicReference<Long> instantNanos)
    {
        Long nanos = instantNanos.get();
        if (nanos == null) {
            return Optional.empty();
        }
        return Optional.of(toDateTime(nanos));
    }

    private DateTime toDateTime(long instantNanos)
    {
        long millisSinceCreate = NANOSECONDS.toMillis(instantNanos - createNanos);
        return new DateTime(createTime.getMillis() + millisSinceCreate);
    }
}

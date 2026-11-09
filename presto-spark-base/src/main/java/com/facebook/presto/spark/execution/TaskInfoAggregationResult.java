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
package com.facebook.presto.spark.execution;

import com.facebook.presto.common.RuntimeStats;

import java.util.Optional;

import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.facebook.airlift.units.Duration.succinctNanos;
import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Outcome and metrics of a {@link TaskInfoAggregator}. The counters are only populated when the aggregator thread
 * finished before the seal timeout.
 */
public final class TaskInfoAggregationResult<R>
{
    public static final String METRIC_PREFIX = "taskInfoAggregation.";

    private final Optional<R> sinkResult;
    private final boolean degraded;
    private final boolean timedOut;
    private final long receivedCount;
    private final long foldedCount;
    private final long duplicateFinishedCount;
    private final long supersededCount;
    private final long pendingFoldedAtSealCount;
    private final long foldFailureCount;
    private final long droppedCount;
    private final long droppedBytes;
    private final long maxBacklogBytes;
    private final long decodeTimeNanos;
    private final long maxFoldLagMillis;
    private final long averageFoldLagMillis;
    private final long sealWaitNanos;

    TaskInfoAggregationResult(
            Optional<R> sinkResult,
            boolean degraded,
            boolean timedOut,
            long receivedCount,
            long foldedCount,
            long duplicateFinishedCount,
            long supersededCount,
            long pendingFoldedAtSealCount,
            long foldFailureCount,
            long droppedCount,
            long droppedBytes,
            long maxBacklogBytes,
            long decodeTimeNanos,
            long maxFoldLagMillis,
            long averageFoldLagMillis,
            long sealWaitNanos)
    {
        this.sinkResult = requireNonNull(sinkResult, "sinkResult is null");
        this.degraded = degraded;
        this.timedOut = timedOut;
        this.receivedCount = receivedCount;
        this.foldedCount = foldedCount;
        this.duplicateFinishedCount = duplicateFinishedCount;
        this.supersededCount = supersededCount;
        this.pendingFoldedAtSealCount = pendingFoldedAtSealCount;
        this.foldFailureCount = foldFailureCount;
        this.droppedCount = droppedCount;
        this.droppedBytes = droppedBytes;
        this.maxBacklogBytes = maxBacklogBytes;
        this.decodeTimeNanos = decodeTimeNanos;
        this.maxFoldLagMillis = maxFoldLagMillis;
        this.averageFoldLagMillis = averageFoldLagMillis;
        this.sealWaitNanos = sealWaitNanos;
    }

    static <R> TaskInfoAggregationResult<R> incomplete(boolean degraded, boolean timedOut, long sealWaitNanos)
    {
        return new TaskInfoAggregationResult<>(Optional.empty(), degraded, timedOut, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, sealWaitNanos);
    }

    /**
     * The result of {@link TaskInfoSink#seal}, absent if the aggregation is degraded or timed out.
     */
    public Optional<R> getSinkResult()
    {
        return sinkResult;
    }

    /**
     * True if the aggregator did not run to completion: it could not start, failed with an error, was closed before
     * it was sealed, or the sink failed to seal.
     */
    public boolean isDegraded()
    {
        return degraded;
    }

    public boolean isTimedOut()
    {
        return timedOut;
    }

    /**
     * True if some task infos were dropped or failed to fold, so the sink result does not cover every task.
     */
    public boolean isPartial()
    {
        return droppedCount > 0 || foldFailureCount > 0;
    }

    public boolean isComplete()
    {
        return sinkResult.isPresent() && !isPartial();
    }

    public long getReceivedCount()
    {
        return receivedCount;
    }

    /**
     * Number of task infos handed to the sink, including {@link #getPendingFoldedAtSealCount}.
     */
    public long getFoldedCount()
    {
        return foldedCount;
    }

    public long getDuplicateFinishedCount()
    {
        return duplicateFinishedCount;
    }

    /**
     * Number of unfinished attempts that were not selected.
     */
    public long getSupersededCount()
    {
        return supersededCount;
    }

    /**
     * Number of unfinished attempts folded at seal because their task never finished.
     */
    public long getPendingFoldedAtSealCount()
    {
        return pendingFoldedAtSealCount;
    }

    public long getFoldFailureCount()
    {
        return foldFailureCount;
    }

    public long getDroppedCount()
    {
        return droppedCount;
    }

    public long getDroppedBytes()
    {
        return droppedBytes;
    }

    /**
     * Peak size of the compressed task infos retained by the aggregator, both undecoded and pending.
     */
    public long getMaxBacklogBytes()
    {
        return maxBacklogBytes;
    }

    public long getDecodeTimeNanos()
    {
        return decodeTimeNanos;
    }

    /**
     * Peak delay between the last heartbeat of a FINISHED task and the moment it was folded. Unfinished attempts
     * folded at seal are not included.
     */
    public long getMaxFoldLagMillis()
    {
        return maxFoldLagMillis;
    }

    public long getAverageFoldLagMillis()
    {
        return averageFoldLagMillis;
    }

    public long getSealWaitNanos()
    {
        return sealWaitNanos;
    }

    public void recordRuntimeStats(RuntimeStats runtimeStats)
    {
        runtimeStats.addMetricValue(METRIC_PREFIX + "degraded", NONE, degraded ? 1 : 0);
        runtimeStats.addMetricValue(METRIC_PREFIX + "timedOut", NONE, timedOut ? 1 : 0);
        runtimeStats.addMetricValue(METRIC_PREFIX + "partial", NONE, isComplete() ? 0 : 1);
        runtimeStats.addMetricValue(METRIC_PREFIX + "received", NONE, receivedCount);
        runtimeStats.addMetricValue(METRIC_PREFIX + "folded", NONE, foldedCount);
        runtimeStats.addMetricValue(METRIC_PREFIX + "duplicateFinished", NONE, duplicateFinishedCount);
        runtimeStats.addMetricValue(METRIC_PREFIX + "superseded", NONE, supersededCount);
        runtimeStats.addMetricValue(METRIC_PREFIX + "pendingFoldedAtSeal", NONE, pendingFoldedAtSealCount);
        runtimeStats.addMetricValue(METRIC_PREFIX + "foldFailures", NONE, foldFailureCount);
        runtimeStats.addMetricValue(METRIC_PREFIX + "dropped", NONE, droppedCount);
        runtimeStats.addMetricValue(METRIC_PREFIX + "droppedBytes", BYTE, droppedBytes);
        runtimeStats.addMetricValue(METRIC_PREFIX + "maxBacklogBytes", BYTE, maxBacklogBytes);
        runtimeStats.addMetricValue(METRIC_PREFIX + "decodeTimeNanos", NANO, decodeTimeNanos);
        runtimeStats.addMetricValue(METRIC_PREFIX + "maxFoldLagNanos", NANO, MILLISECONDS.toNanos(maxFoldLagMillis));
        runtimeStats.addMetricValue(METRIC_PREFIX + "averageFoldLagNanos", NANO, MILLISECONDS.toNanos(averageFoldLagMillis));
        runtimeStats.addMetricValue(METRIC_PREFIX + "sealWaitNanos", NANO, sealWaitNanos);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("complete", isComplete())
                .add("degraded", degraded)
                .add("timedOut", timedOut)
                .add("received", receivedCount)
                .add("folded", foldedCount)
                .add("duplicateFinished", duplicateFinishedCount)
                .add("superseded", supersededCount)
                .add("pendingFoldedAtSeal", pendingFoldedAtSealCount)
                .add("foldFailures", foldFailureCount)
                .add("dropped", droppedCount)
                .add("droppedSize", succinctBytes(droppedBytes))
                .add("maxBacklogSize", succinctBytes(maxBacklogBytes))
                .add("decodeTime", succinctNanos(decodeTimeNanos))
                .add("maxFoldLagMillis", maxFoldLagMillis)
                .add("averageFoldLagMillis", averageFoldLagMillis)
                .add("sealWait", succinctNanos(sealWaitNanos))
                .toString();
    }
}

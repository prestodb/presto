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
package com.facebook.presto.client;

import com.facebook.presto.common.RuntimeStats;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.OptionalDouble;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@Immutable
public class StatementStats
{
    private final String state;
    private final boolean waitingForPrerequisites;
    private final boolean queued;
    private final boolean scheduled;
    private final int nodes;
    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long waitingForPrerequisitesTimeMillis;
    private final long queuedTimeMillis;
    private final long elapsedTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final long peakMemoryBytes;
    private final long peakTotalMemoryBytes;
    private final long peakTaskTotalMemoryBytes;
    private final long spilledBytes;
    private final StageStats rootStage;
    private final RuntimeStats runtimeStats;

    @JsonCreator
    public StatementStats(
            @JsonProperty("state") String state,
            @JsonProperty("waitingForPrerequisites") boolean waitingForPrerequisites,
            @JsonProperty("queued") boolean queued,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("nodes") int nodes,
            @JsonProperty("totalSplits") int totalSplits,
            @JsonProperty("queuedSplits") int queuedSplits,
            @JsonProperty("runningSplits") int runningSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("cpuTimeMillis") long cpuTimeMillis,
            @JsonProperty("wallTimeMillis") long wallTimeMillis,
            @JsonProperty("waitingForPrerequisitesTimeMillis") long waitingForPrerequisitesTimeMillis,
            @JsonProperty("queuedTimeMillis") long queuedTimeMillis,
            @JsonProperty("elapsedTimeMillis") long elapsedTimeMillis,
            @JsonProperty("processedRows") long processedRows,
            @JsonProperty("processedBytes") long processedBytes,
            @JsonProperty("peakMemoryBytes") long peakMemoryBytes,
            @JsonProperty("peakTotalMemoryBytes") long peakTotalMemoryBytes,
            @JsonProperty("peakTaskTotalMemoryBytes") long peakTaskTotalMemoryBytes,
            @JsonProperty("spilledBytes") long spilledBytes,
            @JsonProperty("rootStage") StageStats rootStage,
            @JsonProperty("runtimeStats") RuntimeStats runtimeStats)
    {
        this.state = requireNonNull(state, "state is null");
        this.waitingForPrerequisites = waitingForPrerequisites;
        this.queued = queued;
        this.scheduled = scheduled;
        this.nodes = nodes;
        this.totalSplits = totalSplits;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.completedSplits = completedSplits;
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.waitingForPrerequisitesTimeMillis = waitingForPrerequisitesTimeMillis;
        this.queuedTimeMillis = queuedTimeMillis;
        this.elapsedTimeMillis = elapsedTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.peakMemoryBytes = peakMemoryBytes;
        this.peakTotalMemoryBytes = peakTotalMemoryBytes;
        this.peakTaskTotalMemoryBytes = peakTaskTotalMemoryBytes;
        this.spilledBytes = spilledBytes;
        this.rootStage = rootStage;
        this.runtimeStats = runtimeStats;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public boolean isWaitingForPrerequisites()
    {
        return waitingForPrerequisites;
    }

    @JsonProperty
    public boolean isQueued()
    {
        return queued;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public int getNodes()
    {
        return nodes;
    }

    @JsonProperty
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @JsonProperty
    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    @JsonProperty
    public int getRunningSplits()
    {
        return runningSplits;
    }

    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @JsonProperty
    public long getCpuTimeMillis()
    {
        return cpuTimeMillis;
    }

    @JsonProperty
    public long getWallTimeMillis()
    {
        return wallTimeMillis;
    }

    @JsonProperty
    public long getWaitingForPrerequisitesTimeMillis()
    {
        return waitingForPrerequisitesTimeMillis;
    }

    @JsonProperty
    public long getQueuedTimeMillis()
    {
        return queuedTimeMillis;
    }

    @JsonProperty
    public long getElapsedTimeMillis()
    {
        return elapsedTimeMillis;
    }

    @JsonProperty
    public long getProcessedRows()
    {
        return processedRows;
    }

    @JsonProperty
    public long getProcessedBytes()
    {
        return processedBytes;
    }

    @JsonProperty
    public long getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    @JsonProperty
    public long getPeakTotalMemoryBytes()
    {
        return peakTotalMemoryBytes;
    }

    @JsonProperty
    public long getPeakTaskTotalMemoryBytes()
    {
        return peakTaskTotalMemoryBytes;
    }

    @Nullable
    @JsonProperty
    public StageStats getRootStage()
    {
        return rootStage;
    }

    @Nullable
    @JsonProperty
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        if (!scheduled || totalSplits == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(min(100, (completedSplits * 100.0) / totalSplits));
    }

    @JsonProperty
    public long getSpilledBytes()
    {
        return spilledBytes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("state", state)
                .add("waitingForPrerequisites", waitingForPrerequisites)
                .add("queued", queued)
                .add("scheduled", scheduled)
                .add("nodes", nodes)
                .add("totalSplits", totalSplits)
                .add("queuedSplits", queuedSplits)
                .add("runningSplits", runningSplits)
                .add("completedSplits", completedSplits)
                .add("cpuTimeMillis", cpuTimeMillis)
                .add("wallTimeMillis", wallTimeMillis)
                .add("waitingForPrerequisitesTimeMillis", waitingForPrerequisitesTimeMillis)
                .add("queuedTimeMillis", queuedTimeMillis)
                .add("elapsedTimeMillis", elapsedTimeMillis)
                .add("processedRows", processedRows)
                .add("processedBytes", processedBytes)
                .add("peakMemoryBytes", peakMemoryBytes)
                .add("peakTotalMemoryBytes", peakTotalMemoryBytes)
                .add("peakTaskTotalMemoryBytes", peakTaskTotalMemoryBytes)
                .add("spilledBytes", spilledBytes)
                .add("rootStage", rootStage)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String state;
        private boolean waitingForPrerequisites;
        private boolean queued;
        private boolean scheduled;
        private int nodes;
        private int totalSplits;
        private int queuedSplits;
        private int runningSplits;
        private int completedSplits;
        private long cpuTimeMillis;
        private long wallTimeMillis;
        private long waitingForPrerequisitesTimeMillis;
        private long queuedTimeMillis;
        private long elapsedTimeMillis;
        private long processedRows;
        private long processedBytes;
        private long peakMemoryBytes;
        private long peakTotalMemoryBytes;
        private long peakTaskTotalMemoryBytes;
        private long spilledBytes;
        private StageStats rootStage;
        private RuntimeStats runtimeStats;

        private Builder() {}

        public Builder setState(String state)
        {
            this.state = requireNonNull(state, "state is null");
            return this;
        }

        public Builder setNodes(int nodes)
        {
            this.nodes = nodes;
            return this;
        }

        public Builder setWaitingForPrerequisites(boolean waitingForPrerequisites)
        {
            this.waitingForPrerequisites = waitingForPrerequisites;
            return this;
        }

        public Builder setQueued(boolean queued)
        {
            this.queued = queued;
            return this;
        }

        public Builder setScheduled(boolean scheduled)
        {
            this.scheduled = scheduled;
            return this;
        }

        public Builder setTotalSplits(int totalSplits)
        {
            this.totalSplits = totalSplits;
            return this;
        }

        public Builder setQueuedSplits(int queuedSplits)
        {
            this.queuedSplits = queuedSplits;
            return this;
        }

        public Builder setRunningSplits(int runningSplits)
        {
            this.runningSplits = runningSplits;
            return this;
        }

        public Builder setCompletedSplits(int completedSplits)
        {
            this.completedSplits = completedSplits;
            return this;
        }

        public Builder setCpuTimeMillis(long cpuTimeMillis)
        {
            this.cpuTimeMillis = cpuTimeMillis;
            return this;
        }

        public Builder setWallTimeMillis(long wallTimeMillis)
        {
            this.wallTimeMillis = wallTimeMillis;
            return this;
        }

        public Builder setWaitingForPrerequisitesTimeMillis(long waitingForPrerequisitesTimeMillis)
        {
            this.waitingForPrerequisitesTimeMillis = waitingForPrerequisitesTimeMillis;
            return this;
        }

        public Builder setQueuedTimeMillis(long queuedTimeMillis)
        {
            this.queuedTimeMillis = queuedTimeMillis;
            return this;
        }

        public Builder setElapsedTimeMillis(long elapsedTimeMillis)
        {
            this.elapsedTimeMillis = elapsedTimeMillis;
            return this;
        }

        public Builder setProcessedRows(long processedRows)
        {
            this.processedRows = processedRows;
            return this;
        }

        public Builder setProcessedBytes(long processedBytes)
        {
            this.processedBytes = processedBytes;
            return this;
        }

        public Builder setPeakMemoryBytes(long peakMemoryBytes)
        {
            this.peakMemoryBytes = peakMemoryBytes;
            return this;
        }

        public Builder setPeakTotalMemoryBytes(long peakTotalMemoryBytes)
        {
            this.peakTotalMemoryBytes = peakTotalMemoryBytes;
            return this;
        }

        public Builder setPeakTaskTotalMemoryBytes(long peakTaskTotalMemoryBytes)
        {
            this.peakTaskTotalMemoryBytes = peakTaskTotalMemoryBytes;
            return this;
        }

        public Builder setSpilledBytes(long spilledBytes)
        {
            this.spilledBytes = spilledBytes;
            return this;
        }

        public Builder setRootStage(StageStats rootStage)
        {
            this.rootStage = rootStage;
            return this;
        }

        public Builder setRuntimeStats(RuntimeStats runtimeStats)
        {
            this.runtimeStats = runtimeStats;
            return this;
        }

        public StatementStats build()
        {
            return new StatementStats(
                    state,
                    waitingForPrerequisites,
                    queued,
                    scheduled,
                    nodes,
                    totalSplits,
                    queuedSplits,
                    runningSplits,
                    completedSplits,
                    cpuTimeMillis,
                    wallTimeMillis,
                    waitingForPrerequisitesTimeMillis,
                    queuedTimeMillis,
                    elapsedTimeMillis,
                    processedRows,
                    processedBytes,
                    peakMemoryBytes,
                    peakTotalMemoryBytes,
                    peakTaskTotalMemoryBytes,
                    spilledBytes,
                    rootStage,
                    runtimeStats);
        }
    }
}

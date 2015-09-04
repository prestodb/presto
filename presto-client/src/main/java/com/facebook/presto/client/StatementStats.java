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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class StatementStats
{
    private final String state;
    private final boolean scheduled;
    private final int nodes;
    private final int totalSplits;
    private final int queuedSplits;
    private final int runningSplits;
    private final int completedSplits;
    private final long userTimeMillis;
    private final long cpuTimeMillis;
    private final long wallTimeMillis;
    private final long processedRows;
    private final long processedBytes;
    private final StageStats rootStage;

    @JsonCreator
    public StatementStats(
            @JsonProperty("state") String state,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("nodes") int nodes,
            @JsonProperty("totalSplits") int totalSplits,
            @JsonProperty("queuedSplits") int queuedSplits,
            @JsonProperty("runningSplits") int runningSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("userTimeMillis") long userTimeMillis,
            @JsonProperty("cpuTimeMillis") long cpuTimeMillis,
            @JsonProperty("wallTimeMillis") long wallTimeMillis,
            @JsonProperty("processedRows") long processedRows,
            @JsonProperty("processedBytes") long processedBytes,
            @JsonProperty("rootStage") StageStats rootStage)
    {
        this.state = requireNonNull(state, "state is null");
        this.scheduled = scheduled;
        this.nodes = nodes;
        this.totalSplits = totalSplits;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.completedSplits = completedSplits;
        this.userTimeMillis = userTimeMillis;
        this.cpuTimeMillis = cpuTimeMillis;
        this.wallTimeMillis = wallTimeMillis;
        this.processedRows = processedRows;
        this.processedBytes = processedBytes;
        this.rootStage = rootStage;
    }

    @NotNull
    @JsonProperty
    public String getState()
    {
        return state;
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
    public long getUserTimeMillis()
    {
        return userTimeMillis;
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
    public long getProcessedRows()
    {
        return processedRows;
    }

    @JsonProperty
    public long getProcessedBytes()
    {
        return processedBytes;
    }

    @Nullable
    @JsonProperty
    public StageStats getRootStage()
    {
        return rootStage;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("state", state)
                .add("scheduled", scheduled)
                .add("nodes", nodes)
                .add("totalSplits", totalSplits)
                .add("queuedSplits", queuedSplits)
                .add("runningSplits", runningSplits)
                .add("completedSplits", completedSplits)
                .add("userTimeMillis", userTimeMillis)
                .add("cpuTimeMillis", cpuTimeMillis)
                .add("wallTimeMillis", wallTimeMillis)
                .add("processedRows", processedRows)
                .add("processedBytes", processedBytes)
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
        private boolean scheduled;
        private int nodes;
        private int totalSplits;
        private int queuedSplits;
        private int runningSplits;
        private int completedSplits;
        private long userTimeMillis;
        private long cpuTimeMillis;
        private long wallTimeMillis;
        private long processedRows;
        private long processedBytes;
        private StageStats rootStage;

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

        public Builder setUserTimeMillis(long userTimeMillis)
        {
            this.userTimeMillis = userTimeMillis;
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

        public Builder setRootStage(StageStats rootStage)
        {
            this.rootStage = rootStage;
            return this;
        }

        public StatementStats build()
        {
            return new StatementStats(
                    state,
                    scheduled,
                    nodes,
                    totalSplits,
                    queuedSplits,
                    runningSplits,
                    completedSplits,
                    userTimeMillis,
                    cpuTimeMillis,
                    wallTimeMillis,
                    processedRows,
                    processedBytes,
                    rootStage);
        }
    }
}

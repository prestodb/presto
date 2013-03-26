/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class StageStats
{
    private final URI stageUri;
    private final int stageNumber;
    private final String state;
    private final boolean done;
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
    private final List<StageStats> subStages;

    @JsonCreator
    public StageStats(
            @JsonProperty("stageUri") URI stageUri,
            @JsonProperty("stageNumber") int stageNumber,
            @JsonProperty("state") String state,
            @JsonProperty("done") boolean done,
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
            @JsonProperty("subStages") List<StageStats> subStages)
    {
        this.stageUri = checkNotNull(stageUri, "stageUri is null");
        this.stageNumber = stageNumber;
        this.state = checkNotNull(state, "state is null");
        this.done = done;
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
        this.subStages = ImmutableList.copyOf(checkNotNull(subStages, "subStages is null"));
    }

    @JsonProperty
    public URI getStageUri()
    {
        return stageUri;
    }

    @JsonProperty
    public int getStageNumber()
    {
        return stageNumber;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public boolean isDone()
    {
        return done;
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

    @JsonProperty
    public List<StageStats> getSubStages()
    {
        return subStages;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("stageUri", stageUri)
                .add("stageNumber", stageNumber)
                .add("state", state)
                .add("done", done)
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
                .add("subStages", subStages)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private URI stageUri;
        private int stageNumber;
        private String state;
        private boolean done;
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
        private List<StageStats> subStages;

        private Builder() {}

        public Builder setStageUri(URI stageUri)
        {
            this.stageUri = checkNotNull(stageUri, "stageUri is null");
            return this;
        }

        public Builder setStageNumber(int stageNumber)
        {
            this.stageNumber = stageNumber;
            return this;
        }

        public Builder setState(String state)
        {
            this.state = checkNotNull(state, "state is null");
            return this;
        }

        public Builder setDone(boolean done)
        {
            this.done = done;
            return this;
        }

        public Builder setNodes(int nodes)
        {
            this.nodes = nodes;
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

        public Builder setSubStages(List<StageStats> subStages)
        {
            this.subStages = ImmutableList.copyOf(checkNotNull(subStages, "subStages is null"));
            return this;
        }

        public StageStats build()
        {
            return new StageStats(
                    stageUri,
                    stageNumber,
                    state,
                    done,
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
                    subStages);
        }
    }
}

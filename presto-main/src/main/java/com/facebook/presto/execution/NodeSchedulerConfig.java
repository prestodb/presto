package com.facebook.presto.execution;

import io.airlift.configuration.Config;

import javax.validation.constraints.Min;

public class NodeSchedulerConfig
{
    private int minCandidates = 10;

    @Min(1)
    public int getMinCandidates()
    {
        return minCandidates;
    }

    @Config("node-scheduler.min-candidates")
    public NodeSchedulerConfig setMinCandidates(int candidates)
    {
        this.minCandidates = candidates;
        return this;
    }
}

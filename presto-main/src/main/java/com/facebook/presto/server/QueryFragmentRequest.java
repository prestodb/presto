package com.facebook.presto.server;

import com.facebook.presto.split.PlanFragment;
import com.facebook.presto.split.Split;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

// Request sent to worker to run a plan fragment on given split
public class QueryFragmentRequest
{
    private final Split split;
    private final PlanFragment planFragment;

    @JsonCreator
    public QueryFragmentRequest(@JsonProperty("split") Split split, @JsonProperty("planFragment") PlanFragment planFragment)
    {
        this.split = checkNotNull(split, "split is null");
        this.planFragment = checkNotNull(planFragment, "planFragment is null");
    }

    @JsonProperty
    public Split getSplit()
    {
        return split;
    }

    @JsonProperty
    public PlanFragment getPlanFragment()
    {
        return planFragment;
    }
}
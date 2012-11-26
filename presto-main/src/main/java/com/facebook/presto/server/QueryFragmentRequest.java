package com.facebook.presto.server;

import com.facebook.presto.split.PlanFragment;
import com.facebook.presto.split.Split;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

// Request sent to worker to run a plan fragment on given split
public class QueryFragmentRequest
{
    private final List<Split> splits;
    private final PlanFragment planFragment;

    @JsonCreator
    public QueryFragmentRequest(@JsonProperty("splits") List<Split> splits, @JsonProperty("planFragment") PlanFragment planFragment)
    {
        this.splits = ImmutableList.copyOf(checkNotNull(splits, "splits is null"));
        this.planFragment = checkNotNull(planFragment, "planFragment is null");
    }

    @JsonProperty
    public List<Split> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public PlanFragment getPlanFragment()
    {
        return planFragment;
    }
}

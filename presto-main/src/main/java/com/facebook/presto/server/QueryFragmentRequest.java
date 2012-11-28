package com.facebook.presto.server;

import com.facebook.presto.split.Split;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.transformValues;

// Request sent to worker to run a plan fragment on given split
public class QueryFragmentRequest
{
    private final Map<String, List<Split>> sourceSplits;
    private final PlanFragment planFragment;

    @JsonCreator
    public QueryFragmentRequest(@JsonProperty("sourceSplits") Map<String, List<Split>> sourceSplits, @JsonProperty("planFragment") PlanFragment planFragment)
    {
        Preconditions.checkNotNull(sourceSplits, "sourceSplits is null");
        this.sourceSplits = ImmutableMap.copyOf(transformValues(sourceSplits, new Function<List<Split>, List<Split>>()
        {
            @Override
            public List<Split> apply(List<Split> input)
            {
                return ImmutableList.copyOf(input);
            }
        }));
        this.planFragment = checkNotNull(planFragment, "planFragment is null");
    }

    @JsonProperty
    public Map<String, List<Split>> getSourceSplits()
    {
        return sourceSplits;
    }

    @JsonProperty
    public PlanFragment getPlanFragment()
    {
        return planFragment;
    }
}

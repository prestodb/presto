package com.facebook.presto.server;

import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryFragmentRequest
{
    private final PlanFragment fragment;
    private final Map<String, List<PlanFragmentSource>> fragmentSources;

    @JsonCreator
    public QueryFragmentRequest(
            @JsonProperty("fragment") PlanFragment fragment,
            @JsonProperty("fragmentSources") Map<String, List<PlanFragmentSource>> fragmentSources)
    {
        this.fragment = checkNotNull(fragment, "fragment is null");

        checkNotNull(fragmentSources, "fragmentSources is null");
        this.fragmentSources = ImmutableMap.copyOf(Maps.transformValues(fragmentSources, new Function<List<PlanFragmentSource>, List<PlanFragmentSource>>()
        {
            @Override
            public List<PlanFragmentSource> apply(List<PlanFragmentSource> sources)
            {
                return ImmutableList.copyOf(sources);
            }
        }));
    }

    @JsonProperty
    public PlanFragment getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public Map<String, List<PlanFragmentSource>> getFragmentSources()
    {
        return fragmentSources;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("fragment", fragment)
                .add("fragmentSources", fragmentSources)
                .toString();
    }
}

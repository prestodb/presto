/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.tuple.TupleInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;

@Immutable
public class ExchangePlanFragmentSource
        implements PlanFragmentSource
{
    private final List<URI> sources;
    private final List<TupleInfo> tupleInfos;

    @JsonCreator
    public ExchangePlanFragmentSource(
            @JsonProperty("sources") List<URI> sources,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos)
    {
        Preconditions.checkNotNull(sources, "sources is null");
        Preconditions.checkArgument(!sources.isEmpty(), "sources is empty");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");

        this.sources = ImmutableList.copyOf(sources);
        this.tupleInfos = tupleInfos;
    }

    @JsonProperty
    public List<URI> getSources()
    {
        return sources;
    }

    @JsonProperty
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sources", sources)
                .add("tupleInfos", tupleInfos)
                .toString();
    }
}

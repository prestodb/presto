/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;
import java.util.Map;

@Immutable
public class ExchangePlanFragmentSource
        implements PlanFragmentSource
{
    private final Map<String, URI> sources;
    private final String outputId;
    private final List<TupleInfo> tupleInfos;

    @JsonCreator
    public ExchangePlanFragmentSource(
            @JsonProperty("sources") Map<String, URI> sources,
            @JsonProperty("outputId") String outputId,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos)
    {
        this.sources = ImmutableMap.copyOf(sources);
        this.outputId = outputId;
        this.tupleInfos = tupleInfos;
    }

    @JsonProperty
    public Map<String, URI> getSources()
    {
        return sources;
    }

    @JsonProperty
    public String getOutputId()
    {
        return outputId;
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
                .add("outputId", outputId)
                .add("tupleInfos", tupleInfos)
                .toString();
    }
}

/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.tuple.TupleInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

public class RemoteSplit
        implements Split
{
    private final URI location;
    private final List<TupleInfo> tupleInfos;

    @JsonCreator
    public RemoteSplit(
            @JsonProperty("location") URI location,
            @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos)
    {
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");

        this.location = location;
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.REMOTE;
    }

    @JsonProperty
    public URI getLocation()
    {
        return location;
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
                .add("location", location)
                .add("tupleInfos", tupleInfos)
                .toString();
    }
}

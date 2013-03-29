/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class QueryData
{
    private final List<Column> columns;
    private final Iterable<List<Object>> data;

    @JsonCreator
    public QueryData(
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("data") List<List<Object>> data)
    {
        this(columns, (Iterable<List<Object>>) data);
    }

    public QueryData(List<Column> columns, Iterable<List<Object>> data)
    {
        this.columns = checkNotNull(columns, "columns is null");
        this.data = checkNotNull(data, "data is null");
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("columns", columns)
                .add("rowCount", Iterables.size(data))
                .toString();
    }
}

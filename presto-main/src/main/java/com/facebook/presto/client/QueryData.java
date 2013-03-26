/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryData
{
    private final List<Column> columns;
    private final Iterable<List<Object>> data;

    @JsonCreator
    public QueryData(@JsonProperty("columns") List<Column> columns, @JsonProperty("data") Iterable<List<Object>> data)
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
}

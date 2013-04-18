package com.facebook.presto.execution;

import com.facebook.presto.spi.Split;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

public class DataSource
{
    private final String dataSourceName;
    private final Iterable<Split> splits;

    public DataSource(@Nullable String dataSourceName, Iterable<Split> splits)
    {
        this.dataSourceName = dataSourceName;
        // do not copy splits, we want this to be streaming
        this.splits = splits;
    }

    @Nullable
    public String getDataSourceName()
    {
        return dataSourceName;
    }

    public Iterable<Split> getSplits()
    {
        return splits;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("dataSourceName", dataSourceName)
                .toString();
    }
}

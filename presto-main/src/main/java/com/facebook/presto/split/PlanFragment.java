package com.facebook.presto.split;

import com.facebook.presto.metadata.ColumnHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

// TODO: stand-in class until we have real plan fragments
public class PlanFragment
{
    private final String query;
    private final List<ColumnHandle> columnHandles;

    @JsonCreator
    public PlanFragment(@JsonProperty("query") String query, @JsonProperty("columnHandles") List<ColumnHandle> columnHandles)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkNotNull(columnHandles, "columnHandles is null");
        Preconditions.checkArgument(!columnHandles.isEmpty(), "must provide at least one column");

        this.query = query;
        this.columnHandles = ImmutableList.copyOf(columnHandles);
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public List<ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }
}

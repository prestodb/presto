package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTableHandle;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class InternalSplit
        implements Split
{
    private final InternalTableHandle tableHandle;
    private final Map<InternalColumnHandle, String> filters;

    public InternalSplit(InternalTableHandle handle)
    {
        this(handle, ImmutableMap.<Integer, String>of());
    }

    @JsonCreator
    public InternalSplit(
            @JsonProperty("tableHandle") InternalTableHandle tableHandle,
            @JsonProperty("filters") Map<Integer, String> filters)
    {
        this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        this.filters = checkNotNull(deserializeFilters(filters), "filters is null");
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.INTERNAL;
    }

    @JsonProperty
    public InternalTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public Map<InternalColumnHandle, String> getFilters()
    {
        return filters;
    }

    // TODO: remove this after moving to Jackson 2
    @JsonProperty("filters")
    public Map<Integer, String> getFiltersForJackson()
    {
        return serializeFilters(filters);
    }

    private static Map<InternalColumnHandle, String> deserializeFilters(Map<Integer, String> map)
    {
        ImmutableMap.Builder<InternalColumnHandle, String> builder = ImmutableMap.builder();
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            builder.put(new InternalColumnHandle(entry.getKey()), entry.getValue());
        }
        return builder.build();
    }

    // TODO: remove this after moving to Jackson 2
    public static InternalSplit create(InternalTableHandle tableHandle, Map<InternalColumnHandle, String> filters)
    {
        return new InternalSplit(tableHandle, serializeFilters(filters));
    }

    private static ImmutableMap<Integer, String> serializeFilters(Map<InternalColumnHandle, String> filters)
    {
        ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
        for (Map.Entry<InternalColumnHandle, String> entry : filters.entrySet()) {
            builder.put(entry.getKey().getColumnIndex(), entry.getValue());
        }
        return builder.build();
    }
}

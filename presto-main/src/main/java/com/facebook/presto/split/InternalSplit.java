package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class InternalSplit
        implements Split
{
    private final InternalTableHandle tableHandle;
    private final Map<InternalColumnHandle, String> filters;

    public InternalSplit(InternalTableHandle handle)
    {
        this(handle, ImmutableMap.<InternalColumnHandle, String>of());
    }

    @JsonCreator
    public InternalSplit(
            @JsonProperty("tableHandle") InternalTableHandle tableHandle,
            @JsonProperty("filters") @JsonDeserialize(keyUsing = ICHDeserializer.class) Map<InternalColumnHandle, String> filters)
    {
        this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        this.filters = checkNotNull(filters, "filters is null");
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

    @JsonProperty
    @JsonSerialize(keyUsing = ICHSerializer.class)
    public Map<InternalColumnHandle, String> getFilters()
    {
        return filters;
    }

    public static class ICHSerializer
            extends JsonSerializer<InternalColumnHandle>
    {
        @Override
        public void serialize(InternalColumnHandle value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeFieldName(String.valueOf(value.getColumnIndex()));
        }
    }

    public static class ICHDeserializer
            extends KeyDeserializer
    {
        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt)
                throws IOException
        {
            return new InternalColumnHandle(Integer.parseInt(key));
        }
    }
}

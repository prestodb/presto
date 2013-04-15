package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.HostAddress;
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
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InternalSplit
        implements Split
{
    private final InternalTableHandle tableHandle;
    private final Map<InternalColumnHandle, Object> filters;
    private final List<HostAddress> addresses;

    @JsonCreator
    public InternalSplit(
            @JsonProperty("tableHandle") InternalTableHandle tableHandle,
            @JsonProperty("filters") @JsonDeserialize(keyUsing = ICHDeserializer.class) Map<InternalColumnHandle, Object> filters,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        this.filters = checkNotNull(filters, "filters is null");


        checkNotNull(addresses, "hosts is null");
        checkArgument(!addresses.isEmpty(), "hosts is empty");
        this.addresses = ImmutableList.copyOf(addresses);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.INTERNAL;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public InternalTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    @JsonSerialize(keyUsing = ICHSerializer.class)
    public Map<InternalColumnHandle, Object> getFilters()
    {
        return filters;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("filters", filters)
                .add("addresses", addresses)
                .toString();
    }

    public static class ICHSerializer
            extends JsonSerializer<InternalColumnHandle>
    {
        @Override
        public void serialize(InternalColumnHandle value, JsonGenerator jsonGenerator, SerializerProvider provider)
                throws IOException
        {
            jsonGenerator.writeFieldName(value.getColumnName());
        }
    }

    public static class ICHDeserializer
            extends KeyDeserializer
    {
        @Override
        public Object deserializeKey(String key, DeserializationContext context)
                throws IOException
        {
            return new InternalColumnHandle(key);
        }
    }
}

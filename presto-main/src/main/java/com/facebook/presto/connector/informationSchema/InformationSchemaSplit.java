package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InformationSchemaSplit
        implements Split
{
    private final InformationSchemaTableHandle tableHandle;
    private final Map<String, Object> filters;
    private final List<HostAddress> addresses;

    @JsonCreator
    public InformationSchemaSplit(
            @JsonProperty("tableHandle") InformationSchemaTableHandle tableHandle,
            @JsonProperty("filters") Map<String, Object> filters,
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
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public InformationSchemaTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Map<String, Object> getFilters()
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
}

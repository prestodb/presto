package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class JmxSplit
        implements Split
{
    private final JmxTableHandle tableHandle;
    private final List<HostAddress> addresses;

    @JsonCreator
    public JmxSplit(
            @JsonProperty("tableHandle") JmxTableHandle tableHandle,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        this.addresses = ImmutableList.copyOf(checkNotNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public JmxTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}

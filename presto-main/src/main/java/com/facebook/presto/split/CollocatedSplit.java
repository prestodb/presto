package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.HostAddress;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class CollocatedSplit
        implements Split
{

    private final Map<PlanNodeId, Split> splits;
    private final List<HostAddress> addresses;
    private final boolean remotelyAccessible;

    @JsonCreator
    public CollocatedSplit(@JsonProperty("splits") Map<PlanNodeId, Split> splits,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("remotelyAccessible") boolean remotelyAccessible)
    {
        this.splits = splits;
        this.addresses = addresses;
        this.remotelyAccessible = remotelyAccessible;
    }

    @JsonProperty
    public Map<PlanNodeId, Split> getSplits()
    {
        return splits;
    }

    @JsonProperty
    @Override
    public boolean isRemotelyAccessible()
    {
        return remotelyAccessible;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.COLLOCATED;
    }

    @Override
    public Object getInfo()
    {
        return null;
    }
}

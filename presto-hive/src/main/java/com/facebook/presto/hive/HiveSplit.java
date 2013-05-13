package com.facebook.presto.hive;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PartitionedSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveSplit
    implements PartitionedSplit
{
    private final String clientId;
    private final String partitionId;
    private final boolean lastSplit;
    private final String path;
    private final long start;
    private final long length;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<HostAddress> addresses;

    @JsonCreator
    public HiveSplit(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("partitionId") String partitionId,
            @JsonProperty("lastSplit") boolean lastSplit,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("schema") Properties schema,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        checkNotNull(clientId, "clientId is null");
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkNotNull(partitionId, "partitionName is null");
        checkNotNull(path, "path is null");
        checkNotNull(schema, "schema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(addresses, "addresses is null");

        this.clientId = clientId;
        this.partitionId = partitionId;
        this.lastSplit = lastSplit;
        this.path = path;
        this.start = start;
        this.length = length;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.addresses = ImmutableList.copyOf(addresses);
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    @Override
    public String getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    @Override
    public boolean isLastSplit()
    {
        return lastSplit;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public Properties getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path.toString())
                .put("start", start)
                .put("length", length)
                .put("hosts", addresses)
                .put("partitionId", partitionId)
                .build();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .toString();
    }

    public static HiveSplit markAsLastSplit(HiveSplit split)
    {
        if (split.isLastSplit()) {
            return split;
        }

        return new HiveSplit(split.getClientId(),
                split.getPartitionId(),
                true,
                split.getPath(),
                split.getStart(),
                split.getLength(),
                split.getSchema(),
                split.getPartitionKeys(),
                split.getAddresses());
    }
}

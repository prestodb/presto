package com.facebook.presto.hive;

import com.facebook.presto.spi.PartitionChunk;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;

import java.net.InetAddress;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartitionChunk
    implements PartitionChunk
{
    private final String clientId;
    private final String partitionName;
    private final boolean lastChunk;
    private final Path path;
    private final long start;
    private final long length;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<InetAddress> hosts;

    public static HivePartitionChunk makeLastChunk(HivePartitionChunk chunk)
    {
        if (chunk.isLastChunk()) {
            return chunk;
        }
        else {
            return new HivePartitionChunk(chunk.getClientId(),
                    chunk.getPartitionName(),
                    true,
                    chunk.getPath(),
                    chunk.getStart(),
                    chunk.getLength(),
                    chunk.getSchema(),
                    chunk.getPartitionKeys(),
                    chunk.getHosts());
        }
    }

    @JsonCreator
    public HivePartitionChunk(
            @JsonProperty("clientId") String clientId,
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("lastChunk") boolean lastChunk,
            @JsonProperty("path") Path path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("schema") Properties schema,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("hosts") List<InetAddress> hosts)
    {
        checkNotNull(clientId, "clientId is null");
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkNotNull(partitionName, "partitionName is null");
        checkNotNull(path, "path is null");
        checkNotNull(schema, "schema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(hosts, "hosts is null");

        this.clientId = clientId;
        this.partitionName = partitionName;
        this.lastChunk = lastChunk;
        this.path = path;
        this.start = start;
        this.length = length;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.hosts = ImmutableList.copyOf(hosts);
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public boolean isLastChunk()
    {
        return lastChunk;
    }

    @JsonProperty
    public Path getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @Override
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
    public List<InetAddress> getHosts()
    {
        return hosts;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path.toString())
                .put("start", start)
                .put("length", length)
                .put("hosts", hosts)
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
}

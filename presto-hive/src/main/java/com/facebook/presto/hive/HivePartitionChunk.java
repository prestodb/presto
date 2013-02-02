package com.facebook.presto.hive;

import com.facebook.presto.spi.PartitionChunk;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartitionChunk
    implements PartitionChunk
{
    private final Path path;
    private final long start;
    private final long length;
    private final Properties schema;
    private final List<HivePartitionKey> partitionKeys;
    private final List<HiveColumn> columns;

    @JsonCreator
    public HivePartitionChunk(
            @JsonProperty("path") Path path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("schema") Properties schema,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("columns") List<HiveColumn> columns)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkNotNull(path, "path is null");
        checkNotNull(schema, "schema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");

        this.path = path;
        this.start = start;
        this.length = length;
        this.schema = schema;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.columns = ImmutableList.copyOf(columns);
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
    public List<HiveColumn> getColumns()
    {
        return columns;
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

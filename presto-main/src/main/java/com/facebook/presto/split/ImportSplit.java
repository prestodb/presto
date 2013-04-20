package com.facebook.presto.split;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.DataSourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportSplit
        implements PartitionedSplit
{
    private final String sourceName;
    private final String partition;
    private final boolean lastSplit;
    private final SerializedPartitionChunk serializedChunk;
    private final Object info;

    @JsonCreator
    public ImportSplit(@JsonProperty("sourceName") String sourceName,
                       @JsonProperty("partition") String partition,
                       @JsonProperty("lastSplit") boolean lastSplit,
            @JsonProperty("serializedChunk") SerializedPartitionChunk serializedChunk,
            @JsonProperty("info") Object info)
    {
        this.sourceName = checkNotNull(sourceName, "sourceName is null");
        this.partition = checkNotNull(partition, "partition is null");
        this.serializedChunk = checkNotNull(serializedChunk, "serializedChunk is null");
        this.info = checkNotNull(info, "info is null");

        this.lastSplit = lastSplit;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.IMPORT;
    }

    @JsonProperty
    public String getSourceName()
    {
        return sourceName;
    }

    @JsonProperty
    public String getPartition()
    {
        return partition;
    }

    @JsonProperty
    public boolean isLastSplit()
    {
        return lastSplit;
    }

    @JsonProperty
    public SerializedPartitionChunk getSerializedChunk()
    {
        return serializedChunk;
    }

    @JsonProperty
    public Object getInfo()
    {
        return info;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sourceName", sourceName)
                .add("partition", partition)
                .add("lastSplit", lastSplit)
                .toString();
    }
}

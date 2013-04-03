package com.facebook.presto.split;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.DataSourceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportSplit
        implements Split
{
    private final String sourceName;
    private final SerializedPartitionChunk serializedChunk;
    private final Object info;

    @JsonCreator
    public ImportSplit(@JsonProperty("sourceName") String sourceName, @JsonProperty("serializedChunk") SerializedPartitionChunk serializedChunk, @JsonProperty("info") Object info)
    {
        this.sourceName = checkNotNull(sourceName, "sourceName is null");
        this.serializedChunk = checkNotNull(serializedChunk, "serializedChunk is null");
        this.info = checkNotNull(info, "info is null");
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
    public SerializedPartitionChunk getSerializedChunk()
    {
        return serializedChunk;
    }

    @JsonProperty
    public Object getInfo()
    {
        return info;
    }
}

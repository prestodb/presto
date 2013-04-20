package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableWriterResult
{
    private final long shardId;
    private final String nodeIdentifier;

    public static TableWriterResult forMap(Map<String, Object> map)
    {
        return new TableWriterResult(
                ((Number) map.get("shardId")).longValue(),
                (String) map.get("nodeIdentifier"));
    }

    @JsonCreator
    public TableWriterResult(@JsonProperty("shardId") long shardId,
                             @JsonProperty("nodeIdentifier") String nodeIdentifier)
    {
        this.shardId = shardId;
        this.nodeIdentifier = checkNotNull(nodeIdentifier, "nodeIdentifier is null");
    }


    @JsonProperty
    public long getShardId()
    {
        return shardId;
    }

    @JsonProperty
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
            .add("shardId", shardId)
            .add("nodeIdentifier", nodeIdentifier)
            .toString();
    }
}


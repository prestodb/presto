package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExchangeClientStatus
{
    private final long bufferedBytes;
    private final long averageBytesPerRequest;
    private final int bufferedPages;
    private final List<PageBufferClientStatus> pageBufferClientStatuses;

    @JsonCreator
    public ExchangeClientStatus(
            @JsonProperty("bufferedBytes") long bufferedBytes,
            @JsonProperty("averageBytesPerRequest") long averageBytesPerRequest,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("pageBufferClientStatuses") List<PageBufferClientStatus> pageBufferClientStatuses)
    {
        this.bufferedBytes = bufferedBytes;
        this.averageBytesPerRequest = averageBytesPerRequest;
        this.bufferedPages = bufferedPages;
        this.pageBufferClientStatuses = ImmutableList.copyOf(checkNotNull(pageBufferClientStatuses, "pageBufferClientStatuses is null"));
    }

    @JsonProperty
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @JsonProperty
    public long getAverageBytesPerRequest()
    {
        return averageBytesPerRequest;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    public List<PageBufferClientStatus> getPageBufferClientStatuses()
    {
        return pageBufferClientStatuses;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bufferBytes", bufferedBytes)
                .add("averageBytesPerRequest", averageBytesPerRequest)
                .add("bufferedPages", bufferedPages)
                .add("pageBufferClientStatuses", pageBufferClientStatuses)
                .toString();
    }
}

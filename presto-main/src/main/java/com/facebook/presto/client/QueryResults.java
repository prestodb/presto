/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class QueryResults
{
    private final String queryId;
    private final URI queryInfoUri;
    private final URI partialCancelUri;
    private final URI next;
    private final QueryData data;
    private final StatementStats stats;
    private final QueryError error;

    @JsonCreator
    public QueryResults(
            @JsonProperty("queryId") String queryId,
            @JsonProperty("queryInfoUri") URI queryInfoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("next") URI next,
            @JsonProperty("data") QueryData data,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error)
    {
        this.queryId = checkNotNull(queryId, "queryId is null");
        this.queryInfoUri = checkNotNull(queryInfoUri, "queryInfoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.next = next;
        this.data = data;
        this.stats = checkNotNull(stats, "stats is null");
        this.error = error;
    }

    @NotNull
    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @NotNull
    @JsonProperty
    public URI getQueryInfoUri()
    {
        return queryInfoUri;
    }

    @Nullable
    @JsonProperty
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    public URI getNext()
    {
        return next;
    }

    @Nullable
    @JsonProperty
    public QueryData getData()
    {
        return data;
    }

    @NotNull
    @JsonProperty
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    public QueryError getError()
    {
        return error;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("queryId", queryId)
                .add("queryInfoUri", queryInfoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("next", next)
                .add("data", data)
                .add("stats", stats)
                .add("error", error)
                .toString();
    }
}

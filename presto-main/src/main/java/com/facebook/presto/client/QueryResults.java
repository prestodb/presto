/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.net.URI;

public class QueryResults
{
    private final URI queryInfoUri;
    private final URI next;
    private final QueryData data;
    private final StatementStats stats;

    @JsonCreator
    public QueryResults(@JsonProperty("queryInfoUri") URI queryInfoUri, @JsonProperty("next") URI next, @JsonProperty("data") QueryData data, @JsonProperty("stats") StatementStats stats)
    {
        this.queryInfoUri = queryInfoUri;
        this.next = next;
        this.data = data;
        this.stats = stats;
    }

    @JsonProperty
    public URI getQueryInfoUri()
    {
        return queryInfoUri;
    }

    @JsonProperty
    public URI getNext()
    {
        return next;
    }

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
}

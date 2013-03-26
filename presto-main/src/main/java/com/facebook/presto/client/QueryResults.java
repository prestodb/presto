/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.facebook.presto.execution.QueryInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.net.URI;

public class QueryResults
{
    private final URI self;
    private final URI next;
    private final QueryData data;
    private final QueryInfo stats;

    @JsonCreator
    public QueryResults(@JsonProperty("self") URI self, @JsonProperty("next") URI next, @JsonProperty("data") QueryData data, @JsonProperty("stats") QueryInfo stats)
    {
        this.self = self;
        this.next = next;
        this.data = data;
        this.stats = stats;
    }

    public URI getSelf()
    {
        return self;
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
    public QueryInfo getStats()
    {
        return stats;
    }
}

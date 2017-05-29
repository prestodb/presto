/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.time.ZonedDateTime;
import java.util.Optional;

public class GetTagsRequest
{
    private final ZonedDateTime start;
    private final ZonedDateTime end;

    @JsonCreator
    public GetTagsRequest(
            @JsonProperty("start") ZonedDateTime start,
            @JsonProperty("end") ZonedDateTime end)
    {
        this.start = start;
        this.end = end;
    }

    public Optional<ZonedDateTime> getEnd()
    {
        return Optional.ofNullable(end);
    }

    public Optional<ZonedDateTime> getStart()
    {
        return Optional.ofNullable(start);
    }
}

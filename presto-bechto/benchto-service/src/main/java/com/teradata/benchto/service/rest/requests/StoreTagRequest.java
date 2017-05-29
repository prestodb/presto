/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public class StoreTagRequest
{
    @NotNull
    @Size(min = 1, max = 255)
    private final String name;
    @NotNull
    @Size(min = 0, max = 1024)
    private final String description;

    @JsonCreator
    public StoreTagRequest(
            @JsonProperty("name") String name,
            @JsonProperty("description") String description)
    {
        this.name = name;
        this.description = description;
    }

    public String getDescription()
    {
        return description;
    }

    public String getName()
    {
        return name;
    }
}

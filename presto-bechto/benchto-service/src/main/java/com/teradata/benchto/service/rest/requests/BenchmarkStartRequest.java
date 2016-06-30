/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Map;

public class BenchmarkStartRequest
{
    @NotNull
    @Size(min = 1, max = 64)
    private final String name;
    @NotNull
    @Size(min = 1, max = 64)
    private final String environmentName;
    private final Map<String, String> variables;
    private final Map<String, String> attributes;

    @JsonCreator
    public BenchmarkStartRequest(@JsonProperty("name") String name, @JsonProperty("environmentName") String environmentName,
            @JsonProperty("variables") Map<String, String> variables, @JsonProperty("attributes") Map<String, String> attributes)
    {
        this.name = name;
        this.environmentName = environmentName;
        this.variables = variables;
        this.attributes = attributes;
    }

    public String getName()
    {
        return name;
    }

    public String getEnvironmentName()
    {
        return environmentName;
    }

    public Map<String, String> getVariables()
    {
        return variables;
    }

    public Map<String, String> getAttributes()
    {
        return attributes;
    }
}

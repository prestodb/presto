/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
package com.teradata.benchto.service.rest.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Map;

public class GenerateBenchmarkNamesRequestItem
{

    @NotNull
    @Size(min = 1, max = 255)
    private final String name;
    private final Map<String, String> variables;

    @JsonCreator
    public GenerateBenchmarkNamesRequestItem(@JsonProperty("name") String name, @JsonProperty("variables") Map<String, String> variables)
    {
        this.name = name;
        this.variables = variables;
    }

    public String getName()
    {
        return name;
    }

    public Map<String, String> getVariables()
    {
        return variables;
    }
}

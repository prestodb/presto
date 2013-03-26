/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class Column
{
    private final String name;
    private final String type;

    @JsonCreator
    public Column(@JsonProperty("name") String name, @JsonProperty("type") String type)
    {
        this.name = checkNotNull(name, "name is null");
        this.type = checkNotNull(type, "type is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }
}

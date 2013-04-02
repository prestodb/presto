/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
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

    public static Function<Column, String> nameGetter()
    {
        return new Function<Column, String>()
        {
            @Override
            public String apply(Column input)
            {
                return input.getName();
            }
        };
    }
}

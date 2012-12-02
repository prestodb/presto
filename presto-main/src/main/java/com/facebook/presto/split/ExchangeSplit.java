/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.split;

import com.facebook.presto.metadata.DataSourceType;
import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.net.URI;

public class ExchangeSplit
        implements Split
{
    private final URI location;

    @JsonCreator
    public ExchangeSplit(@JsonProperty("location") URI location)
    {
        this.location = location;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.EXCHANGE;
    }

    @JsonProperty
    public URI getLocation()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("location", location)
                .toString();
    }
}

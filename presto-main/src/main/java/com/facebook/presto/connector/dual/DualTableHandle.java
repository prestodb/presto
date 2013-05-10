package com.facebook.presto.connector.dual;

import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DualTableHandle
        implements TableHandle
{
    private final String schemaName;

    @JsonCreator
    public DualTableHandle(@JsonProperty("schemaName") String schemaName)
    {
        this.schemaName = schemaName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public String toString()
    {
        return "dual:" + schemaName;
    }
}

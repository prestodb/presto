package com.facebook.presto.spi.analyzer;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@ThriftStruct
public class UpdateInfo
{
    private final String updateType;
    private final String updateObject;

    @JsonCreator
    @ThriftConstructor
    public UpdateInfo(
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateObject") String updateObject)
    {
        this.updateType = updateType;
        this.updateObject = updateObject;
    }

    @JsonProperty
    @ThriftField(1)
    public String getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    @ThriftField(2)
    public String getUpdateObject()
    {
        return updateObject;
    }
}

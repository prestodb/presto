package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchTableHandle;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NativeTableHandle.class, name = "native"),
        @JsonSubTypes.Type(value = InternalTableHandle.class, name = "internal"),
        @JsonSubTypes.Type(value = ImportTableHandle.class, name = "import"),
        @JsonSubTypes.Type(value = TpchTableHandle.class, name = "tpch")})
public interface TableHandle
{
    DataSourceType getDataSourceType();
}

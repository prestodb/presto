package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchColumnHandle;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "columnHandleType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NativeColumnHandle.class, name = "native"),
        @JsonSubTypes.Type(value = InternalColumnHandle.class, name = "internal"),
        @JsonSubTypes.Type(value = ImportColumnHandle.class, name = "import"),
        @JsonSubTypes.Type(value = TpchColumnHandle.class, name = "tpch")})
public interface ColumnHandle
{
    DataSourceType getDataSourceType();
}

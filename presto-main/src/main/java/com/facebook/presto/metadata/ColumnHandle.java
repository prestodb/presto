package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchColumnHandle;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "columnHandleType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NativeColumnHandle.class, name = "native"),
        @JsonSubTypes.Type(value = ImportColumnHandle.class, name = "import"),
        @JsonSubTypes.Type(value = TpchColumnHandle.class, name = "tpch")})
public interface ColumnHandle
{
    DataSourceType getDataSourceType();
}

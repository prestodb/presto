package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchTableHandle;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NativeTableHandle.class, name = "native"),
        @JsonSubTypes.Type(value = ImportTableHandle.class, name = "import"),
        @JsonSubTypes.Type(value = TpchTableHandle.class, name = "tpch")})
public interface TableHandle
{
    String getHandleId();
    DataSourceType getDataSourceType();
}

package com.facebook.presto.metadata;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NativeColumnHandle.class, name = "native"),
        @JsonSubTypes.Type(value = ImportColumnHandle.class, name = "import")})
public interface ColumnHandle
{
    DataSourceType getDataSourceType();
}

package com.facebook.presto.metadata;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NativeTableHandle.class, name = "native"),
        @JsonSubTypes.Type(value = ImportTableHandle.class, name = "import")})
public interface TableHandle
{
    DataSourceType getDataSourceType();
}

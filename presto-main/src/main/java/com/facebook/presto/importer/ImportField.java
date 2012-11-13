package com.facebook.presto.importer;

import com.facebook.presto.tuple.TupleInfo;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class ImportField
{
    private final long columnId;
    private final TupleInfo.Type columnType;
    private final String importFieldName;

    public ImportField(
            @JsonProperty("columnId") long columnId,
            @JsonProperty("columnType") TupleInfo.Type columnType,
            @JsonProperty("importFieldName") String importFieldName)
    {
        this.columnId = columnId;
        this.columnType = checkNotNull(columnType, "columnType is null");
        this.importFieldName = checkNotNull(importFieldName, "importFieldName is null");
    }

    @JsonProperty
    public long getColumnId()
    {
        return columnId;
    }

    @JsonProperty
    public TupleInfo.Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public String getImportFieldName()
    {
        return importFieldName;
    }
}

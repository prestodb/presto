package com.facebook.presto.importer;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Function;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class ImportField
{
    private final long columnId;
    private final TupleInfo.Type columnType;
    private final String importFieldName;

    @JsonCreator
    public ImportField(
            @JsonProperty("columnId") long columnId,
            @JsonProperty("columnType") TupleInfo.Type columnType,
            @JsonProperty("importFieldName") String importFieldName)
    {
        checkArgument(columnId > 0, "columnId must be greater than zero");
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

    public static Function<ImportField, String> nameGetter()
    {
        return new Function<ImportField, String>()
        {
            @Override
            public String apply(ImportField input)
            {
                return input.getImportFieldName();
            }
        };
    }

    public static Function<ImportField, Type> typeGetter()
    {
        return new Function<ImportField, Type>()
        {
            @Override
            public Type apply(ImportField input)
            {
                return input.getColumnType();
            }
        };
    }
}

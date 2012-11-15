package com.facebook.presto.metadata;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportColumnHandle
    implements ColumnHandle
{
    private final String sourceName;
    private final int columnIndex;

    @JsonCreator
    public ImportColumnHandle(@JsonProperty("sourceName") String sourceName, @JsonProperty("columnIndex") int columnIndex)
    {
        checkNotNull(sourceName, "sourceName is null");
        checkArgument(columnIndex >= 0, "columnIndex must be greater than or equal to zero");

        this.sourceName = sourceName;
        this.columnIndex = columnIndex;
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.IMPORT;
    }

    @JsonProperty
    public String getSourceName()
    {
        return sourceName;
    }

    @JsonProperty
    public int getColumnIndex()
    {
        return columnIndex;
    }
}

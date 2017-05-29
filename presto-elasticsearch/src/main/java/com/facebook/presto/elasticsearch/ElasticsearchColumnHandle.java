
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public final class ElasticsearchColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final Type columnType;
    private final String columnJsonPath;
    private final String columnJsonType;
    /* ordinalPosition of a columnhandle is the -> number of the column in the entire list of columns of this table
        IT DOESNT DEPEND ON THE QUERY (select clm3, clm0, clm1  from tablename)
        The columnhandle of clm3 : has ordinalposition = 3
     */
    private final int ordinalPosition;

    @JsonCreator
    public ElasticsearchColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("columnJsonPath") String columnJsonPath,
            @JsonProperty("columnJsonType") String columnJsonType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.columnName = checkNotNull(columnName, "columnName is null");
        this.columnType = checkNotNull(columnType, "columnType is null");
        this.columnJsonPath = checkNotNull(columnJsonPath, "columnJsonPath is null");
        this.columnJsonType = checkNotNull(columnJsonType, "columnJsonType is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }


    @JsonProperty
    public String getColumnJsonPath()
    {
        return columnJsonPath;
    }

    @JsonProperty
    public String getColumnJsonType()
    {
        return columnJsonType;
    }


    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, false);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ElasticsearchColumnHandle other = (ElasticsearchColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("columnJsonPath", columnJsonPath)
                .add("columnJsonType", columnJsonType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}

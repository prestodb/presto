package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;

public class JmxColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final ColumnType columnType;
    private final int ordinalPosition;

    @JsonCreator
    public JmxColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.connectorId = connectorId;
        this.columnName = columnName;
        this.columnType = columnType;
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
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, columnName, columnType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final JmxColumnHandle other = (JmxColumnHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) && Objects.equal(this.columnName, other.columnName) && Objects.equal(this.columnType, other.columnType);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .toString();
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, ordinalPosition, false);
    }

    public static Function<JmxColumnHandle, ColumnMetadata> columnMetadataGetter()
    {
        return new Function<JmxColumnHandle, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(JmxColumnHandle jmxColumnHandle)
            {
                return jmxColumnHandle.getColumnMetadata();
            }
        };
    }
}

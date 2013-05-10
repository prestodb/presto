package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.connector.jmx.JmxColumnHandle.columnMetadataGetter;
import static com.google.common.collect.Iterables.transform;

public class JmxTableHandle
        implements TableHandle
{
    private final String connectorId;
    private final String objectName;
    private final List<JmxColumnHandle> columns;

    @JsonCreator
    public JmxTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("objectName") String objectName,
            @JsonProperty("columns") List<JmxColumnHandle> columns)
    {
        this.connectorId = connectorId;
        this.objectName = objectName;
        this.columns = columns;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getObjectName()
    {
        return objectName;
    }

    @JsonProperty
    public List<JmxColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, objectName, columns);
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
        final JmxTableHandle other = (JmxTableHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) && Objects.equal(this.objectName, other.objectName) && Objects.equal(this.columns, other.columns);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("objectName", objectName)
                .add("columns", columns)
                .toString();
    }

    public TableMetadata getTableMetadata()
    {
        return new TableMetadata(new SchemaTableName(JmxMetadata.SCHEMA_NAME, objectName), ImmutableList.copyOf(transform(columns, columnMetadataGetter())));
    }
}


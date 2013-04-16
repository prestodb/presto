package com.facebook.presto.metadata;

import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class ImportTableHandle
        implements TableHandle
{
    private final String clientId;
    private final QualifiedTableName tableName;
    private final TableHandle tableHandle;

    @JsonCreator
    public ImportTableHandle(@JsonProperty("clientId") String clientId,
            @JsonProperty("tableName") QualifiedTableName tableName,
            @JsonProperty("tableHandle") TableHandle tableHandle)
    {
        this.clientId = clientId;
        this.tableName = tableName;
        this.tableHandle = tableHandle;
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public QualifiedTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    @Override
    public String toString()
    {
        return "import:" + clientId + ":" + tableHandle;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(clientId, tableHandle);
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
        final ImportTableHandle other = (ImportTableHandle) obj;
        return Objects.equal(this.clientId, other.clientId) &&
                Objects.equal(this.tableHandle, other.tableHandle);
    }
}

package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveTableHandle
        implements TableHandle
{
    private final String clientId;
    private final SchemaTableName tableName;

    @JsonCreator
    public HiveTableHandle(@JsonProperty("clientId") String clientId, @JsonProperty("tableName") SchemaTableName tableName)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getClientId()
    {
        return clientId;
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName);
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
        final HiveTableHandle other = (HiveTableHandle) obj;
        return Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("HiveTableHandle{");
        sb.append("tableName=").append(tableName);
        sb.append('}');
        return sb.toString();
    }
}

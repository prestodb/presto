package com.facebook.presto.hive;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveConnectorId
{
    private final String connectorId;

    public HiveConnectorId(String connectorId)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkArgument(!connectorId.isEmpty(), "connectorId is empty");
        this.connectorId = connectorId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId);
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
        final HiveConnectorId other = (HiveConnectorId) obj;
        return Objects.equal(this.connectorId, other.connectorId);
    }

    @Override
    public String toString()
    {
        return connectorId;
    }
}

package com.facebook.presto.connector.jmx;

import com.google.common.base.Objects;

public class JmxConnectorId
{
    private final String id;

    public JmxConnectorId(String id)
    {
        this.id = id;
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id);
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
        final JmxConnectorId other = (JmxConnectorId) obj;
        return Objects.equal(this.id, other.id);
    }
}

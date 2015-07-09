
package com.facebook.presto.elasticsearch;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ElasticsearchConnectorId
{
    private final String id;

    public ElasticsearchConnectorId(String id)
    {
        this.id = checkNotNull(id, "id is null");
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
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

        ElasticsearchConnectorId other = (ElasticsearchConnectorId) obj;
        return Objects.equals(this.id, other.id);
    }
}

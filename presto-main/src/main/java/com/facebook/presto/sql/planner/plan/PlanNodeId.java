package com.facebook.presto.sql.planner.plan;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class PlanNodeId
{
    private final String id;

    @JsonCreator
    public PlanNodeId(String id)
    {
        checkNotNull(id, "id is null");
        this.id = id;
    }

    @JsonValue
    public String toString()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PlanNodeId that = (PlanNodeId) o;

        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }
}

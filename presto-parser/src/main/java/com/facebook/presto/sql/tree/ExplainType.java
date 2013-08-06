package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExplainType
        extends ExplainOption
{
    public enum Type
    {
        LOGICAL,
        DISTRIBUTED
    }

    private final Type type;

    public ExplainType(Type type)
    {
        this.type = checkNotNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type);
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
        ExplainType o = (ExplainType) obj;
        return Objects.equal(type, o.type);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .toString();
    }
}

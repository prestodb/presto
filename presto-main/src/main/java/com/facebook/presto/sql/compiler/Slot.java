package com.facebook.presto.sql.compiler;

import com.google.common.base.Preconditions;

/**
 * A slot is a unique identifier for a source of data. E.g., a column in a table or an attribute in a subquery
 */
public class Slot
{
    private final int id;
    private final Type type;

    public Slot(int id, Type type)
    {
        Preconditions.checkArgument(id >= 0, "Id must be positive");
        Preconditions.checkNotNull(type, "type is null");

        this.id = id;
        this.type = type;
    }

    public String getName()
    {
        return "$" + id;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return getName();
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

        Slot slot = (Slot) o;

        if (id != slot.id) {
            return false;
        }
        if (type != slot.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = id;
        result = 31 * result + type.hashCode();
        return result;
    }
}

package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;

public class ColumnMetadata
{
    private final String name;
    private final TupleInfo.Type type;
    private final int ordinalPosition;

    public ColumnMetadata(String name, Type type, int ordinalPosition)
    {
        checkNotNull(emptyToNull(name), "name is null or empty");
        checkNotNull(type, "type is null");
        Preconditions.checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");

        this.name = name.toLowerCase();
        this.type = type;
        this.ordinalPosition = ordinalPosition;
    }

    public String getName()
    {
        return name;
    }

    public TupleInfo.Type getType()
    {
        return type;
    }

    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type);
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
        final ColumnMetadata other = (ColumnMetadata) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type);
    }
}

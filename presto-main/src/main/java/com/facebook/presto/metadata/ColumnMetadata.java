package com.facebook.presto.metadata;

import com.facebook.presto.TupleInfo;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;

public class ColumnMetadata
{
    private final String name;
    private final TupleInfo.Type type;

    public ColumnMetadata(TupleInfo.Type type, String name)
    {
        checkNotNull(type, "type is null");
        checkNotNull(emptyToNull(name), "name is null or empty");

        this.type = type;
        this.name = name.toLowerCase();
    }

    public String getName()
    {
        return name;
    }

    public TupleInfo.Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
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
        ColumnMetadata o = (ColumnMetadata) obj;
        return Objects.equal(name, o.name) &&
                Objects.equal(type, o.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type);
    }
}

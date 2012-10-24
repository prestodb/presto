package com.facebook.presto.metadata;

import com.facebook.presto.TupleInfo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;

public class ColumnMetadata
{
    private final String name;
    private final TupleInfo.Type type;

    public ColumnMetadata(TupleInfo.Type type, String name)
    {
        this.type = checkNotNull(type, "type is null");
        this.name = checkNotNull(emptyToNull(name), "name is null or empty");
    }

    public String getName()
    {
        return name;
    }

    public TupleInfo.Type getType()
    {
        return type;
    }
}

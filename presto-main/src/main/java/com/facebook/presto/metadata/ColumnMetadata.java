package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;

public class ColumnMetadata
{
    public static Function<ColumnMetadata, String> columnNameGetter()
    {
        return new Function<ColumnMetadata, String>() {

            @Override
            public String apply(ColumnMetadata input)
            {
                return input.getName();
            }
        };
    }

    public static Function<ColumnMetadata, ColumnHandle> columnHandleGetter()
    {
        return new Function<ColumnMetadata, ColumnHandle>() {

            @Override
            public ColumnHandle apply(ColumnMetadata input)
            {
                return input.getColumnHandle().get();
            }
        };
    }

    private final String name;
    private final TupleInfo.Type type;
    private final Optional<ColumnHandle> columnHandle;

    public ColumnMetadata(String name, TupleInfo.Type type)
    {
        this(name, type, Optional.<ColumnHandle>absent());
    }

    public ColumnMetadata(String name, TupleInfo.Type type, ColumnHandle columnHandle)
    {
        this(name, type, Optional.of(checkNotNull(columnHandle, "columnHandle is null")));
    }

    private ColumnMetadata(String name, TupleInfo.Type type, Optional<ColumnHandle> columnHandle)
    {
        checkNotNull(emptyToNull(name), "name is null or empty");
        checkNotNull(type, "type is null");

        this.name = name.toLowerCase();
        this.type = type;
        this.columnHandle = columnHandle;
    }

    public String getName()
    {
        return name;
    }

    public TupleInfo.Type getType()
    {
        return type;
    }

    public Optional<ColumnHandle> getColumnHandle()
    {
        return columnHandle;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
    }
}

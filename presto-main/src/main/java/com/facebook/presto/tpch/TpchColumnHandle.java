package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchColumnHandle
    implements ColumnHandle
{
    private final int fieldIndex;
    private final ColumnType type;

    @JsonCreator
    public TpchColumnHandle(@JsonProperty("fieldIndex") int fieldIndex, @JsonProperty("type") ColumnType type)
    {
        checkArgument(fieldIndex >= 0, "fieldIndex must be at least zero");
        checkNotNull(type, "type is null");
        this.fieldIndex = fieldIndex;
        this.type = type;
    }

    @JsonProperty
    public int getFieldIndex()
    {
        return fieldIndex;
    }

    @JsonProperty
    public ColumnType getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "tpch:" + fieldIndex;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TpchColumnHandle)) {
            return false;
        }

        TpchColumnHandle that = (TpchColumnHandle) o;

        if (fieldIndex != that.fieldIndex) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = fieldIndex;
        result = 31 * result + type.hashCode();
        return result;
    }
}

package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseColumnHandle implements ColumnHandle {
    private final String columnName;
    private final Type columnType;
    private final int ordinalPosition;

    @JsonCreator
    public BaseColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    public ColumnMetadata toColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public boolean equals(Object o)
    {
        if(o == this){
            return true;
        }
        if(o instanceof BaseColumnHandle){
            BaseColumnHandle baseColumnHandle = (BaseColumnHandle) o;
            return new EqualsBuilder()
                    .append(columnName, baseColumnHandle.columnName)
                    .append(columnType, baseColumnHandle.columnType)
                    .append(ordinalPosition, baseColumnHandle.ordinalPosition)
                    .isEquals();
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                .append(columnName)
                .append(columnType)
                .append(ordinalPosition)
                .toHashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}

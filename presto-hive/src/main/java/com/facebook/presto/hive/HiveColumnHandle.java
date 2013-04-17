package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final int ordinalPosition;
    private final HiveType hiveType;
    private final int hiveColumnIndex;
    private final boolean partitionKey;

    @JsonCreator
    public HiveColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("hiveColumnIndex") int hiveColumnIndex,
            @JsonProperty("partitionKey") boolean partitionKey)
    {
        checkNotNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        checkArgument(hiveColumnIndex >= 0 || partitionKey, "hiveColumnIndex is negative");
        checkNotNull(hiveType, "hiveType is null");

        this.name = name;
        this.ordinalPosition = ordinalPosition;
        this.hiveColumnIndex = hiveColumnIndex;
        this.hiveType = hiveType;
        this.partitionKey = partitionKey;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public int getHiveColumnIndex()
    {
        return hiveColumnIndex;
    }

    @JsonProperty
    public boolean isPartitionKey()
    {
        return partitionKey;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, hiveType.getNativeType(), ordinalPosition);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("hiveType", hiveType)
                .add("hiveColumnIndex", hiveColumnIndex)
                .toString();
    }

    public static Function<HiveColumnHandle, Integer> hiveColumnIndexGetter()
    {
        return new Function<HiveColumnHandle, Integer>()
        {
            @Override
            public Integer apply(HiveColumnHandle input)
            {
                return input.getHiveColumnIndex();
            }
        };
    }

    public static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter()
    {
        return new Function<HiveColumnHandle, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(HiveColumnHandle input)
            {
                return input.getColumnMetadata();
            }
        };
    }
}

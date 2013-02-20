package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveColumn
{
    private final String name;
    private final int index;
    private final HiveType hiveType;

    @JsonCreator
    public HiveColumn(
            @JsonProperty("name") String name,
            @JsonProperty("index") int index,
            @JsonProperty("hiveType") HiveType hiveType)
    {
        checkNotNull(name, "name is null");
        checkArgument(index >= 0, "index is negative");
        checkNotNull(hiveType, "hiveType is null");

        this.name = name;
        this.index = index;
        this.hiveType = hiveType;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getIndex()
    {
        return index;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("index", index)
                .add("hiveType", hiveType)
                .toString();
    }

    public static Function<HiveColumn, Integer> indexGetter()
    {
        return new Function<HiveColumn, Integer>()
        {
            @Override
            public Integer apply(HiveColumn input)
            {
                return input.getIndex();
            }
        };
    }
}

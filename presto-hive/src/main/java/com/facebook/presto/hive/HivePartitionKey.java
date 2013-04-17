package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartitionKey
{
    private final String name;
    private final HiveType hiveType;
    private final String value;

    @JsonCreator
    public HivePartitionKey(
            @JsonProperty("name") String name,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("value") String value)
    {
        checkNotNull(name, "name is null");
        checkNotNull(hiveType, "hiveType is null");
        checkNotNull(value, "value is null");

        this.name = name;
        this.hiveType = hiveType;
        this.value = value;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("hiveType", hiveType)
                .add("value", value)
                .toString();
    }

    public static Function<HivePartitionKey, String> nameGetter()
    {
        return new Function<HivePartitionKey, String>() {
            @Override
            public String apply(HivePartitionKey input)
            {
                return input.getName();
            }
        };
    }
}

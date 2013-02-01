package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.spi.SchemaField.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public class HiveColumn
{
    private final String name;
    private final int index;
    private final SchemaField.Type type;
    private final PrimitiveCategory hiveType;

    @JsonCreator
    public HiveColumn(
            @JsonProperty("name") String name,
            @JsonProperty("index") int index,
            @JsonProperty("type") Type type,
            @JsonProperty("hiveType") PrimitiveCategory hiveType)
    {
        checkNotNull(name, "name is null");
        checkArgument(index >= 0, "index is negative");
        checkNotNull(type, "type is null");
        checkNotNull(hiveType, "hiveType is null");

        this.name = name;
        this.index = index;
        this.type = type;
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
    public SchemaField.Type getType()
    {
        return type;
    }

    @JsonProperty
    public PrimitiveCategory getHiveType()
    {
        return hiveType;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("index", index)
                .add("type", type)
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

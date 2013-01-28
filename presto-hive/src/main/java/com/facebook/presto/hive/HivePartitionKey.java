package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.spi.SchemaField.Type;
import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public class HivePartitionKey
{
    private final String name;
    private final SchemaField.Type type;
    private final PrimitiveCategory hiveType;
    private final String value;

    @JsonCreator
    public HivePartitionKey(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("hiveType") PrimitiveCategory hiveType,
            @JsonProperty("value") String value)
    {
        checkNotNull(name, "name is null");
        checkNotNull(type, "type is null");
        checkNotNull(hiveType, "hiveType is null");
        checkNotNull(value, "value is null");

        this.name = name;
        this.type = type;
        this.hiveType = hiveType;
        this.value = value;
    }

    @JsonProperty
    public String getName()
    {
        return name;
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
                .add("type", type)
                .add("hiveType", hiveType)
                .add("value", value)
                .toString();
    }
}

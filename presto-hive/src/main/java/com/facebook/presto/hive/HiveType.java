package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaField;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

public enum HiveType
{
    BOOLEAN(SchemaField.Type.LONG),
    BYTE(SchemaField.Type.LONG),
    SHORT(SchemaField.Type.LONG),
    INT(SchemaField.Type.LONG),
    LONG(SchemaField.Type.LONG),
    FLOAT(SchemaField.Type.DOUBLE),
    DOUBLE(SchemaField.Type.DOUBLE),
    STRING(SchemaField.Type.STRING),
    LIST(SchemaField.Type.STRING),
    MAP(SchemaField.Type.STRING),
    STRUCT(SchemaField.Type.STRING);

    private final SchemaField.Type nativeType;

    private HiveType(SchemaField.Type nativeType)
    {
        this.nativeType = nativeType;
    }

    public SchemaField.Type getNativeType()
    {
        return nativeType;
    }

    public static HiveType getSupportedHiveType(PrimitiveCategory primitiveCategory)
    {
        HiveType hiveType = getHiveType(primitiveCategory);
        checkArgument(hiveType != null, "Unknown primitive Hive category: " + primitiveCategory);
        return hiveType;
    }

    public static HiveType getHiveType(PrimitiveCategory primitiveCategory)
    {
        switch (primitiveCategory) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return BYTE;
            case SHORT:
                return SHORT;
            case INT:
                return INT;
            case LONG:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return STRING;
            default:
                return null;
        }
    }

    public static HiveType getSupportedHiveType(ObjectInspector fieldInspector)
    {
        HiveType hiveType = getHiveType(fieldInspector);
        checkArgument(hiveType != null, "Unknown Hive category: " + fieldInspector.getCategory());
        return hiveType;
    }

    public static HiveType getHiveType(ObjectInspector fieldInspector)
    {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                return getHiveType(((PrimitiveObjectInspector) fieldInspector).getPrimitiveCategory());
            case MAP:
                return MAP;
            case LIST:
                return LIST;
            case STRUCT:
                return STRUCT;
            default:
                return null;
        }
    }
}

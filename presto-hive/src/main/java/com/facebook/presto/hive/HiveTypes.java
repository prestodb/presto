package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaField;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

class HiveTypes
{
    private HiveTypes()
    {
    }

    static SchemaField.Type getSupportedPrimitiveType(PrimitiveObjectInspector.PrimitiveCategory category)
    {
        SchemaField.Type type = getPrimitiveType(category);
        if (type == null) {
            throw new IllegalArgumentException("Hive type not supported: " + category);
        }
        return type;
    }

    static SchemaField.Type getPrimitiveType(PrimitiveObjectInspector.PrimitiveCategory category)
    {
        switch (category) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return SchemaField.Type.LONG;
            case FLOAT:
            case DOUBLE:
                return SchemaField.Type.DOUBLE;
            case STRING:
                return SchemaField.Type.STRING;
            case BOOLEAN:
                return SchemaField.Type.LONG;
            default:
                return null;
        }
    }

    static SchemaField.Type convertHiveType(String type)
    {
        return getSupportedPrimitiveType(convertNativeHiveType(type));
    }

    static PrimitiveObjectInspector.PrimitiveCategory convertNativeHiveType(String type)
    {
        return PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(type).primitiveCategory;
    }
}

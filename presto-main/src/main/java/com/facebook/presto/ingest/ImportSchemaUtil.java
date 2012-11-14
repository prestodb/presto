package com.facebook.presto.ingest;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ImportSchemaUtil
{
    public static List<ColumnMetadata> createColumnMetadata(List<SchemaField> schemaFields)
    {
        ImmutableList.Builder<ColumnMetadata> list = ImmutableList.builder();
        for (SchemaField field : schemaFields) {
            checkArgument(field.getCategory() == SchemaField.Category.PRIMITIVE, "Unhandled category: %s", field.getCategory());
            TupleInfo.Type type = getTupleType(field.getPrimitiveType());
            list.add(new ColumnMetadata(type, field.getFieldName()));
        }
        return list.build();
    }

    public static TupleInfo.Type getTupleType(SchemaField.Type type)
    {
        switch (type) {
            case LONG:
                return TupleInfo.Type.FIXED_INT_64;
            case DOUBLE:
                return TupleInfo.Type.DOUBLE;
            case STRING:
                return TupleInfo.Type.VARIABLE_BINARY;
            default:
                throw new IllegalArgumentException("Unhandled type: " + type);
        }
    }
}

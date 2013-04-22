package com.facebook.presto.ingest;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.transform;

public class ImportSchemaUtil
{
    public static List<ColumnMetadata> convertToMetadata(final String sourceName, List<SchemaField> schemaFields)
    {
        return transform(schemaFields, new Function<SchemaField, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(SchemaField field)
            {
                checkArgument(field.getCategory() == SchemaField.Category.PRIMITIVE, "Unhandled category: %s", field.getCategory());
                TupleInfo.Type type = getTupleType(field.getPrimitiveType());
                ColumnHandle handle = new ImportColumnHandle(sourceName, field.getFieldName(), field.getFieldId(), type);
                return new ColumnMetadata(field.getFieldName(), type, field.getFieldId());
            }
        });
    }

    private static TupleInfo.Type getTupleType(SchemaField.Type type)
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

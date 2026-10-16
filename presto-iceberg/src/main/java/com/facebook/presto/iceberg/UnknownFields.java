/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.PrestoException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

/**
 * A field of type {@code unknown} is never stored in a data file: it always reads back as null.
 * Iceberg leaves such a field out of the Parquet and ORC schemas it writes, so Presto has to leave
 * it out of both the schema it writes and the values it writes.
 */
public final class UnknownFields
{
    private UnknownFields() {}

    static boolean isUnknownType(Type type)
    {
        return type.typeId() == Type.TypeID.UNKNOWN;
    }

    static boolean containsUnknownType(Type type)
    {
        if (isUnknownType(type)) {
            return true;
        }
        return type.isNestedType() && type.asNestedType().fields().stream()
                .anyMatch(field -> containsUnknownType(field.type()));
    }

    /**
     * The schema of the data file, which is the table schema without its top-level {@code unknown} columns.
     */
    static Schema fileSchema(Schema tableSchema)
    {
        if (tableSchema.columns().stream().noneMatch(col -> isUnknownType(col.type()))) {
            return tableSchema;
        }
        List<Types.NestedField> columns = tableSchema.columns().stream()
                .filter(col -> !isUnknownType(col.type()))
                .collect(toImmutableList());
        return new Schema(columns);
    }

    /**
     * Rejects tables where writing would produce data that cannot be represented in the file format.
     * Top-level unknown columns are simply dropped from the file schema; a table where every column
     * is unknown produces a zero-column file, which is valid per the Iceberg spec.
     */
    public static void validateWritable(Schema tableSchema)
    {
        for (Types.NestedField column : tableSchema.columns()) {
            if (!isUnknownType(column.type()) && containsUnknownType(column.type())) {
                throw new PrestoException(NOT_SUPPORTED, format(
                        "Writing to an Iceberg table with unknown type nested inside a row, array, or map is not supported: %s",
                        column.name()));
            }
        }
    }
}

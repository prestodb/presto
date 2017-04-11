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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveColumnHandle;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.io.ColumnIO;
import parquet.io.ColumnIOFactory;
import parquet.io.InvalidRecordException;
import parquet.io.ParquetDecodingException;
import parquet.io.PrimitiveColumnIO;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Optional.empty;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils()
    {
    }

    public static List<PrimitiveColumnIO> getColumns(MessageType fileSchema, MessageType requestedSchema)
    {
        return (new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true).getLeaves();
    }

    public static Optional<RichColumnDescriptor> getDescriptor(MessageType fileSchema, MessageType requestedSchema, List<String> path)
    {
        checkArgument(path.size() >= 1, "Parquet nested path should have at least one component");
        int level = path.size();
        for (PrimitiveColumnIO columnIO : getColumns(fileSchema, requestedSchema)) {
            ColumnIO[] fields = columnIO.getPath();
            if (fields.length <= level) {
                continue;
            }
            if (fields[level].getName().equalsIgnoreCase(path.get(level - 1))) {
                boolean match = true;
                for (int i = 0; i < level - 1; i++) {
                    if (!fields[i + 1].getName().equalsIgnoreCase(path.get(i))) {
                        match = false;
                    }
                }

                if (match) {
                    ColumnDescriptor descriptor = columnIO.getColumnDescriptor();
                    return Optional.of(new RichColumnDescriptor(descriptor.getPath(), columnIO.getType().asPrimitiveType(), descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel()));
                }
            }
        }
        return empty();
    }

    public static int getFieldIndex(MessageType fileSchema, String name)
    {
        try {
            return fileSchema.getFieldIndex(name);
        }
        catch (InvalidRecordException e) {
            for (Type type : fileSchema.getFields()) {
                if (type.getName().equalsIgnoreCase(name)) {
                    return fileSchema.getFieldIndex(type.getName());
                }
            }
            return -1;
        }
    }

    public static parquet.schema.Type getParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return getParquetTypeByName(column.getName(), messageType);
        }

        if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
            return messageType.getType(column.getHiveColumnIndex());
        }
        return null;
    }

    public static ParquetEncoding getParquetEncoding(Encoding encoding)
    {
        switch (encoding) {
            case PLAIN:
                return ParquetEncoding.PLAIN;
            case RLE:
                return ParquetEncoding.RLE;
            case BIT_PACKED:
                return ParquetEncoding.BIT_PACKED;
            case PLAIN_DICTIONARY:
                return ParquetEncoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED:
                return ParquetEncoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY:
                return ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY:
                return ParquetEncoding.DELTA_BYTE_ARRAY;
            case RLE_DICTIONARY:
                return ParquetEncoding.RLE_DICTIONARY;
            default:
                throw new ParquetDecodingException("Unsupported Parquet encoding: " + encoding);
        }
    }

    private static parquet.schema.Type getParquetTypeByName(String columnName, MessageType messageType)
    {
        if (messageType.containsField(columnName)) {
            return messageType.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }
}

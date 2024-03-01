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
package com.facebook.presto.parquet;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.joining;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils()
    {
    }

    public static List<PrimitiveColumnIO> getColumns(MessageType fileSchema, MessageType requestedSchema)
    {
        return (new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true).getLeaves();
    }

    public static MessageColumnIO getColumnIO(MessageType fileSchema, MessageType requestedSchema)
    {
        return (new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true);
    }

    public static GroupColumnIO getMapKeyValueColumn(GroupColumnIO groupColumnIO)
    {
        while (groupColumnIO.getChildrenCount() == 1) {
            groupColumnIO = (GroupColumnIO) groupColumnIO.getChild(0);
        }
        return groupColumnIO;
    }

    /* For backward-compatibility, the type of elements in LIST-annotated structures should always be determined by the following rules:
     * 1. If the repeated field is not a group, then its type is the element type and elements are required.
     * 2. If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
     * 3. If the repeated field is a group with one field and is named either array or uses the LIST-annotated group's name with _tuple appended then the repeated type is the element type and elements are required.
     * 4. Otherwise, the repeated field's type is the element type with the repeated field's repetition.
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     */
    public static ColumnIO getArrayElementColumn(ColumnIO columnIO)
    {
        while (columnIO instanceof GroupColumnIO && !columnIO.getType().isRepetition(REPEATED)) {
            columnIO = ((GroupColumnIO) columnIO).getChild(0);
        }

        /* If array has a standard 3-level structure with middle level repeated group with a single field:
         *  optional group my_list (LIST) {
         *     repeated group element {
         *        required binary str (UTF8);
         *     };
         *  }
         */
        if (columnIO instanceof GroupColumnIO &&
                columnIO.getType().getOriginalType() == null &&
                ((GroupColumnIO) columnIO).getChildrenCount() == 1 &&
                !columnIO.getName().equals("array") &&
                !columnIO.getName().equals(columnIO.getParent().getName() + "_tuple")) {
            return ((GroupColumnIO) columnIO).getChild(0);
        }

        /* Backward-compatibility support for 2-level arrays where a repeated field is not a group:
         *   optional group my_list (LIST) {
         *      repeated int32 element;
         *   }
         */
        return columnIO;
    }

    public static Map<List<String>, RichColumnDescriptor> getDescriptors(MessageType fileSchema, MessageType requestedSchema)
    {
        Map<List<String>, RichColumnDescriptor> descriptorsByPath = new HashMap<>();
        List<PrimitiveColumnIO> columns = getColumns(fileSchema, requestedSchema);
        for (String[] paths : fileSchema.getPaths()) {
            List<String> columnPath = Arrays.asList(paths);
            getDescriptor(columns, columnPath)
                    .ifPresent(richColumnDescriptor -> descriptorsByPath.put(columnPath, richColumnDescriptor));
        }
        return descriptorsByPath;
    }

    public static Optional<RichColumnDescriptor> getDescriptor(List<PrimitiveColumnIO> columns, List<String> path)
    {
        checkArgument(path.size() >= 1, "Parquet nested path should have at least one component");
        int index = getPathIndex(columns, path);
        if (index == -1) {
            return Optional.empty();
        }
        PrimitiveColumnIO columnIO = columns.get(index);
        return Optional.of(new RichColumnDescriptor(columnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType()));
    }

    private static int getPathIndex(List<PrimitiveColumnIO> columns, List<String> path)
    {
        int maxLevel = path.size();
        int index = -1;
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ColumnIO[] fields = columns.get(columnIndex).getPath();
            if (fields.length <= maxLevel) {
                continue;
            }
            if (fields[maxLevel].getName().equalsIgnoreCase(path.get(maxLevel - 1))) {
                boolean match = true;
                for (int level = 0; level < maxLevel - 1; level++) {
                    if (!fields[level + 1].getName().equalsIgnoreCase(path.get(level))) {
                        match = false;
                    }
                }

                if (match) {
                    index = columnIndex;
                }
            }
        }
        return index;
    }

    public static int getFieldIndex(MessageType fileSchema, String name)
    {
        try {
            return fileSchema.getFieldIndex(name.toLowerCase(Locale.ENGLISH));
        }
        catch (InvalidRecordException e) {
            for (org.apache.parquet.schema.Type type : fileSchema.getFields()) {
                if (type.getName().equalsIgnoreCase(name)) {
                    return fileSchema.getFieldIndex(type.getName());
                }
            }
            return -1;
        }
    }

    @SuppressWarnings("deprecation")
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

    public static org.apache.parquet.schema.Type getParquetTypeByName(String columnName, GroupType messageType)
    {
        if (messageType.containsField(columnName)) {
            return messageType.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (org.apache.parquet.schema.Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }

        return null;
    }

    /**
     * Parquet column names are case-sensitive unlike Hive, which converts all column names to lowercase.
     * Therefore, when we look up columns we first check for exact match, and if that fails we look for a case-insensitive match.
     */
    public static ColumnIO lookupColumnByName(GroupColumnIO groupColumnIO, String columnName)
    {
        ColumnIO columnIO = groupColumnIO.getChild(columnName);

        if (columnIO != null) {
            return columnIO;
        }

        for (int i = 0; i < groupColumnIO.getChildrenCount(); i++) {
            if (groupColumnIO.getChild(i).getName().equalsIgnoreCase(columnName)) {
                return groupColumnIO.getChild(i);
            }
        }

        return null;
    }

    public static Optional<Type> createDecimalType(RichColumnDescriptor descriptor)
    {
        if (descriptor.getPrimitiveType().getOriginalType() != DECIMAL) {
            return Optional.empty();
        }
        return Optional.of(createDecimalType(descriptor.getPrimitiveType().getDecimalMetadata()));
    }

    private static Type createDecimalType(DecimalMetadata decimalMetadata)
    {
        return DecimalType.createDecimalType(decimalMetadata.getPrecision(), decimalMetadata.getScale());
    }

    /**
     * For optional fields:
     * definitionLevel == maxDefinitionLevel     => Value is defined
     * definitionLevel == maxDefinitionLevel - 1 => Value is null
     * definitionLevel < maxDefinitionLevel - 1  => Value does not exist, because one of its optional parent fields is null
     */
    public static boolean isValueNull(boolean required, int definitionLevel, int maxDefinitionLevel)
    {
        return !required && (definitionLevel == maxDefinitionLevel - 1);
    }

    // copied from presto-hive DecimalUtils
    public static long getShortDecimalValue(byte[] bytes)
    {
        long value = 0;
        if ((bytes[0] & 0x80) != 0) {
            for (int i = 0; i < 8 - bytes.length; ++i) {
                value |= 0xFFL << (8 * (7 - i));
            }
        }

        for (int i = 0; i < bytes.length; i++) {
            value |= ((long) bytes[bytes.length - i - 1] & 0xFFL) << (8 * i);
        }

        return value;
    }

    public static Optional<org.apache.parquet.schema.Type> getSubfieldType(GroupType baseType, String rootName, List<String> nestedColumnPath)
    {
        checkArgument(nestedColumnPath.size() >= 1, "subfield size is less than 1");
        ImmutableList.Builder<org.apache.parquet.schema.Type> typeBuilder = ImmutableList.builder();

        org.apache.parquet.schema.Type parentType = getParquetTypeByName(rootName, baseType.asGroupType());
        if (parentType == null) {
            // column doesn't exist in the file
            return Optional.empty();
        }
        typeBuilder.add(parentType);

        for (String field : nestedColumnPath) {
            org.apache.parquet.schema.Type childType = getParquetTypeByName(field, parentType.asGroupType());
            if (childType == null) {
                return Optional.empty();
            }
            typeBuilder.add(childType);
            parentType = childType;
        }
        List<org.apache.parquet.schema.Type> typeChain = typeBuilder.build();
        if (typeChain.isEmpty()) {
            return Optional.empty();
        }
        else if (typeChain.size() == 1) {
            return Optional.of(getOnlyElement(typeChain));
        }
        else {
            org.apache.parquet.schema.Type messageType = typeChain.get(typeChain.size() - 1);
            for (int i = typeChain.size() - 2; i >= 0; --i) {
                GroupType groupType = typeChain.get(i).asGroupType();
                messageType = new MessageType(groupType.getName(), ImmutableList.of(messageType));
            }
            return Optional.of(messageType);
        }
    }

    public static List<String> nestedColumnPath(Subfield subfield)
    {
        ImmutableList.Builder<String> nestedColumnPathBuilder = ImmutableList.builder();
        for (Subfield.PathElement pathElement : subfield.getPath()) {
            checkArgument(pathElement instanceof Subfield.NestedField, "Unsupported subfield. Expected only nested field path elements. " + subfield);
            nestedColumnPathBuilder.add(((Subfield.NestedField) pathElement).getName());
        }
        return nestedColumnPathBuilder.build();
    }

    public static String pushdownColumnNameForSubfield(Subfield subfield)
    {
        // Using the delimiter `$_$_$` to avoid conflict with Subfield serialization when `.` is used as delimiter
        return columnPathFromSubfield(subfield).stream().collect(joining("$_$_$"));
    }

    public static List<String> columnPathFromSubfield(Subfield subfield)
    {
        ImmutableList.Builder<String> columnPath = ImmutableList.builder();
        columnPath.add(subfield.getRootName());
        columnPath.addAll(nestedColumnPath(subfield));
        return columnPath.build();
    }

    public static boolean isTimeStampMicrosType(ColumnDescriptor descriptor)
    {
        return TIMESTAMP_MICROS.equals(descriptor.getPrimitiveType().getOriginalType());
    }
}

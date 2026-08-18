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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.Type;

import java.util.List;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;

/**
 * The Presto types of a table that has fields of type {@code unknown}. A data file never stores such
 * a field, so the type a file has is the type the table has without those fields, and the reader has
 * to add them back to what it reads.
 *
 * @see UnknownFields for the Iceberg schema and the values written to a file
 */
public final class UnknownFieldTypes
{
    private UnknownFieldTypes() {}

    /**
     * Whether the type is, or holds, the {@code unknown} type. The Hive types the readers convert
     * through have no equivalent of it, so a type that holds it has to be passed around as is.
     */
    public static boolean hasUnknownType(Type type)
    {
        if (type.equals(UNKNOWN)) {
            return true;
        }
        return type.getTypeParameters().stream().anyMatch(UnknownFieldTypes::hasUnknownType);
    }

    /**
     * The type to read a column with, given the type it has in the table and the type it has in the
     * data file. A file leaves out the {@code unknown} fields of a row, so the type the file has is
     * the type the table has without them, and reading with the table type is what fills them back in
     * with nulls.
     *
     * <p>Any other difference between the two types, such as a field added to a row after the file was
     * written, is left alone: the file type is what the reader has always been given for those.
     */
    static Type readType(Type tableType, Type fileType)
    {
        return isTableTypeWithoutUnknownFields(fileType, tableType) ? tableType : fileType;
    }

    /**
     * Whether the file type is the table type with its {@code unknown} fields left out, and so differs
     * from it in nothing else.
     */
    private static boolean isTableTypeWithoutUnknownFields(Type fileType, Type tableType)
    {
        if (tableType instanceof RowType && fileType instanceof RowType) {
            List<Field> tableFields = ((RowType) tableType).getFields();
            List<Field> fileFields = ((RowType) fileType).getFields();
            int fileField = 0;
            for (Field field : tableFields) {
                if (field.getType().equals(UNKNOWN)) {
                    continue;
                }
                if (fileField >= fileFields.size() ||
                        !hasSameName(field, fileFields.get(fileField)) ||
                        !isTableTypeWithoutUnknownFields(fileFields.get(fileField).getType(), field.getType())) {
                    return false;
                }
                fileField++;
            }
            return fileField == fileFields.size();
        }
        if (tableType instanceof ArrayType && fileType instanceof ArrayType) {
            return isTableTypeWithoutUnknownFields(((ArrayType) fileType).getElementType(), ((ArrayType) tableType).getElementType());
        }
        if (tableType instanceof MapType && fileType instanceof MapType) {
            return isTableTypeWithoutUnknownFields(((MapType) fileType).getKeyType(), ((MapType) tableType).getKeyType()) &&
                    isTableTypeWithoutUnknownFields(((MapType) fileType).getValueType(), ((MapType) tableType).getValueType());
        }
        return tableType.equals(fileType);
    }

    private static boolean hasSameName(Field tableField, Field fileField)
    {
        if (!tableField.getName().isPresent() || !fileField.getName().isPresent()) {
            return tableField.getName().equals(fileField.getName());
        }
        return tableField.getName().get().equalsIgnoreCase(fileField.getName().get());
    }
}

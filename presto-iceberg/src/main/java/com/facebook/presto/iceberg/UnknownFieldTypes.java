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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.avro.AvroSchemaUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

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
     * <p>For row, array, and map types, unknown fields are merged into the file type independently of
     * any other schema-evolution differences (e.g. fields added after the file was written). Any other
     * type difference is left alone: the file type is what the reader has always been given for those.
     */
    static Type readType(Type tableType, Type fileType)
    {
        return readType(tableType, fileType, null);
    }

    /**
     * Like {@link #readType(Type, Type)} but uses the given {@link TypeManager} to construct a merged
     * map type when the map's value type contains unknown fields. Callers that have a {@code TypeManager}
     * should prefer this overload to get correct behaviour for maps with unknown-typed value fields.
     */
    static Type readType(Type tableType, Type fileType, TypeManager typeManager)
    {
        if (tableType instanceof RowType && fileType instanceof RowType) {
            return mergeRowReadType((RowType) tableType, (RowType) fileType, typeManager);
        }
        if (tableType instanceof ArrayType && fileType instanceof ArrayType) {
            Type fileElement = ((ArrayType) fileType).getElementType();
            Type merged = readType(((ArrayType) tableType).getElementType(), fileElement, typeManager);
            return merged.equals(fileElement) ? fileType : new ArrayType(merged);
        }
        if (tableType instanceof MapType && fileType instanceof MapType) {
            MapType tableMap = (MapType) tableType;
            MapType fileMap = (MapType) fileType;
            Type fileKey = fileMap.getKeyType();
            Type fileValue = fileMap.getValueType();
            Type mergedKey = readType(tableMap.getKeyType(), fileKey, typeManager);
            Type mergedValue = readType(tableMap.getValueType(), fileValue, typeManager);
            if (mergedKey.equals(fileKey) && mergedValue.equals(fileValue)) {
                return fileType;
            }
            if (typeManager != null) {
                return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                        TypeSignatureParameter.of(mergedKey.getTypeSignature()),
                        TypeSignatureParameter.of(mergedValue.getTypeSignature())));
            }
            // Fallback when TypeManager is unavailable: all-or-nothing check
            return isTableTypeWithoutUnknownFields(fileType, tableType) ? tableType : fileType;
        }
        return fileType;
    }

    /**
     * Merges the {@code unknown} fields from {@code tableType} into the structure of {@code fileType},
     * so that schema-evolution differences in the non-unknown fields are preserved while unknown fields
     * are restored at their correct positions.
     */
    private static RowType mergeRowReadType(RowType tableType, RowType fileType, TypeManager typeManager)
    {
        List<Field> tableFields = tableType.getFields();
        List<Field> fileFields = fileType.getFields();

        Map<String, Field> fileFieldsByName = new HashMap<>();
        for (Field field : fileFields) {
            field.getName().ifPresent(name -> fileFieldsByName.put(name.toLowerCase(Locale.ENGLISH), field));
        }

        boolean changed = false;
        int filePosition = 0;
        List<Field> resultFields = new ArrayList<>(tableFields.size());
        for (Field tableField : tableFields) {
            if (tableField.getType().equals(UNKNOWN)) {
                resultFields.add(tableField);
                changed = true;
                continue;
            }
            String key = tableField.getName().map(n -> n.toLowerCase(Locale.ENGLISH)).orElse(null);
            Field fileField;
            boolean matchedViaAvroEncoding = false;
            if (key != null) {
                fileField = fileFieldsByName.get(key);
                if (fileField == null) {
                    // Parquet stores field names Avro-encoded (e.g. "field-one" → "field_x2done"),
                    // so also try the encoded form when the plain name isn't found.
                    fileField = fileFieldsByName.get(AvroSchemaUtil.makeCompatibleName(key).toLowerCase(Locale.ENGLISH));
                    matchedViaAvroEncoding = (fileField != null);
                }
            }
            else {
                // Anonymous field: fall back to positional matching against the file's fields.
                // Iceberg schemas always name their fields, so this branch handles only edge cases
                // (e.g. a RowType constructed without names). Unknown fields are not stored in the
                // file, so the position is the index among the non-unknown table fields seen so far.
                fileField = filePosition < fileFields.size() ? fileFields.get(filePosition) : null;
            }
            if (fileField == null) {
                // Field was added to the table after the file was written; reader will produce null
                resultFields.add(tableField);
                changed = true;
            }
            else {
                filePosition++;
                Type mergedType = readType(tableField.getType(), fileField.getType(), typeManager);
                // When matched via Avro encoding the file field carries the encoded name (e.g.
                // "field_x2done"). Use that name in the result so constructField can locate the
                // column in the Parquet GroupColumnIO, which is also keyed by encoded names.
                Optional<String> resultName = matchedViaAvroEncoding ? fileField.getName() : tableField.getName();
                resultFields.add(new Field(resultName, mergedType));
                if (!mergedType.equals(fileField.getType())) {
                    changed = true;
                }
            }
        }

        return changed ? RowType.from(resultFields) : fileType;
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
        return tableField.getName().get().toLowerCase(Locale.ENGLISH).equals(fileField.getName().get().toLowerCase(Locale.ENGLISH));
    }
}

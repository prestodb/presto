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
import org.apache.iceberg.types.Types;

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
        return readType(tableType, fileType, null, null, null);
    }

    /**
     * Like {@link #readType(Type, Type)} but uses the given {@link TypeManager} to construct a merged
     * map type when the map's value type contains unknown fields. Callers that have a {@code TypeManager}
     * should prefer this overload to get correct behaviour for maps with unknown-typed value fields.
     */
    static Type readType(Type tableType, Type fileType, TypeManager typeManager)
    {
        return readType(tableType, fileType, typeManager, null, null);
    }

    /**
     * Like {@link #readType(Type, Type, TypeManager)} but uses Iceberg field IDs to match struct
     * children before falling back to names. This correctly handles field renames: a historical
     * Parquet file keeps the old physical name but retains the same Iceberg field ID, so a renamed
     * child is matched by ID rather than being treated as newly added (which would read as NULL).
     *
     * <p>Falls back to name-only matching when either {@code tableIdentity} or
     * {@code icebergFileType} is {@code null} (e.g. for files written without embedded Iceberg IDs).
     *
     * @param tableIdentity the {@link ColumnIdentity} for the table's column whose children carry
     *        the Iceberg field IDs for each struct field; {@code null} disables ID-based matching
     * @param icebergFileType the Iceberg type as embedded in the Parquet file's metadata, whose
     *        nested field IDs identify the physical fields stored in the file; {@code null} disables
     *        ID-based matching
     */
    static Type readType(
            Type tableType,
            Type fileType,
            TypeManager typeManager,
            ColumnIdentity tableIdentity,
            org.apache.iceberg.types.Type icebergFileType)
    {
        if (tableType instanceof RowType && fileType instanceof RowType) {
            Types.StructType icebergFileStruct = (icebergFileType instanceof Types.StructType)
                    ? (Types.StructType) icebergFileType : null;
            return mergeRowReadType(
                    (RowType) tableType,
                    (RowType) fileType,
                    typeManager,
                    tableIdentity,
                    icebergFileStruct);
        }
        if (tableType instanceof ArrayType && fileType instanceof ArrayType) {
            Type fileElement = ((ArrayType) fileType).getElementType();
            // For ARRAY, ColumnIdentity.getChildren() has exactly one child: the element
            ColumnIdentity elementIdentity = (tableIdentity != null && !tableIdentity.getChildren().isEmpty())
                    ? tableIdentity.getChildren().get(0) : null;
            org.apache.iceberg.types.Type icebergFileElement = (icebergFileType instanceof Types.ListType)
                    ? ((Types.ListType) icebergFileType).elementType() : null;
            Type merged = readType(
                    ((ArrayType) tableType).getElementType(),
                    fileElement,
                    typeManager,
                    elementIdentity,
                    icebergFileElement);
            return merged.equals(fileElement) ? fileType : new ArrayType(merged);
        }
        if (tableType instanceof MapType && fileType instanceof MapType) {
            MapType tableMap = (MapType) tableType;
            MapType fileMap = (MapType) fileType;
            Type fileKey = fileMap.getKeyType();
            Type fileValue = fileMap.getValueType();
            // For MAP, ColumnIdentity.getChildren() has two children: key then value
            List<ColumnIdentity> mapChildren = (tableIdentity != null) ? tableIdentity.getChildren() : ImmutableList.of();
            ColumnIdentity keyIdentity = mapChildren.size() >= 1 ? mapChildren.get(0) : null;
            ColumnIdentity valueIdentity = mapChildren.size() >= 2 ? mapChildren.get(1) : null;
            org.apache.iceberg.types.Type icebergFileKey = (icebergFileType instanceof Types.MapType)
                    ? ((Types.MapType) icebergFileType).keyType() : null;
            org.apache.iceberg.types.Type icebergFileValue = (icebergFileType instanceof Types.MapType)
                    ? ((Types.MapType) icebergFileType).valueType() : null;
            Type mergedKey = readType(tableMap.getKeyType(), fileKey, typeManager, keyIdentity, icebergFileKey);
            Type mergedValue = readType(tableMap.getValueType(), fileValue, typeManager, valueIdentity, icebergFileValue);
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
     *
     * <p>When {@code tableIdentity} and {@code icebergFileStruct} are both non-null, struct children
     * are matched by Iceberg field ID first, which correctly handles renamed fields. Name-based
     * matching is used as a fallback for files that lack embedded Iceberg field IDs.
     */
    private static RowType mergeRowReadType(
            RowType tableType,
            RowType fileType,
            TypeManager typeManager,
            ColumnIdentity tableIdentity,
            Types.StructType icebergFileStruct)
    {
        List<Field> tableFields = tableType.getFields();
        List<Field> fileFields = fileType.getFields();

        // Build file field lookup maps. ID-based lookup is preferred when the file's Iceberg
        // schema is available: it survives renames because Iceberg field IDs are stable across
        // schema evolution. Name-based lookup is a fallback for files written before IDs were
        // embedded (e.g. Hive-migrated tables).
        Map<Integer, Field> fileFieldsById = new HashMap<>();
        Map<Integer, Types.NestedField> icebergFileFieldsById = new HashMap<>();
        if (icebergFileStruct != null) {
            List<Types.NestedField> icebergFields = icebergFileStruct.fields();
            // fileType.getFields() and icebergFileStruct.fields() are derived from the same
            // Parquet/Iceberg schema in the same order, so positional pairing is correct.
            for (int j = 0; j < icebergFields.size() && j < fileFields.size(); j++) {
                int fieldId = icebergFields.get(j).fieldId();
                fileFieldsById.put(fieldId, fileFields.get(j));
                icebergFileFieldsById.put(fieldId, icebergFields.get(j));
            }
        }

        Map<String, Field> fileFieldsByName = new HashMap<>();
        for (Field field : fileFields) {
            field.getName().ifPresent(name -> fileFieldsByName.put(name.toLowerCase(Locale.ENGLISH), field));
        }
        Map<String, Types.NestedField> icebergFileFieldsByName = new HashMap<>();
        if (icebergFileStruct != null) {
            for (Types.NestedField f : icebergFileStruct.fields()) {
                icebergFileFieldsByName.put(f.name().toLowerCase(Locale.ENGLISH), f);
            }
        }

        boolean changed = false;
        int filePosition = 0;
        List<Field> resultFields = new ArrayList<>(tableFields.size());

        // tableIdentity.getChildren() is in the same order as tableType.getFields() — both are
        // derived from the Iceberg table schema's field list in order.
        for (int i = 0; i < tableFields.size(); i++) {
            Field tableField = tableFields.get(i);
            if (tableField.getType().equals(UNKNOWN)) {
                resultFields.add(tableField);
                changed = true;
                continue;
            }

            ColumnIdentity tableFieldIdentity = (tableIdentity != null && i < tableIdentity.getChildren().size())
                    ? tableIdentity.getChildren().get(i) : null;

            Field fileField = null;
            Types.NestedField icebergFileField = null;
            boolean matchedById = false;
            boolean matchedViaAvroEncoding = false;

            // Prefer ID-based matching: stable across renames
            if (tableFieldIdentity != null && !fileFieldsById.isEmpty()) {
                fileField = fileFieldsById.get(tableFieldIdentity.getId());
                if (fileField != null) {
                    icebergFileField = icebergFileFieldsById.get(tableFieldIdentity.getId());
                    matchedById = true;
                }
            }

            // Name-based fallback for files without embedded Iceberg IDs
            if (fileField == null) {
                String key = tableField.getName().map(n -> n.toLowerCase(Locale.ENGLISH)).orElse(null);
                if (key != null) {
                    fileField = fileFieldsByName.get(key);
                    icebergFileField = icebergFileFieldsByName.get(key);
                    if (fileField == null) {
                        // Parquet stores field names Avro-encoded (e.g. "field-one" → "field_x2done"),
                        // so also try the encoded form when the plain name isn't found.
                        String encodedKey = AvroSchemaUtil.makeCompatibleName(key).toLowerCase(Locale.ENGLISH);
                        fileField = fileFieldsByName.get(encodedKey);
                        icebergFileField = icebergFileFieldsByName.get(encodedKey);
                        matchedViaAvroEncoding = (fileField != null);
                    }
                }
                else {
                    // Anonymous field: fall back to positional matching against the file's fields.
                    // Iceberg schemas always name their fields, so this branch handles only edge cases
                    // (e.g. a RowType constructed without names). Unknown fields are not stored in the
                    // file, so the position is the index among the non-unknown table fields seen so far.
                    fileField = filePosition < fileFields.size() ? fileFields.get(filePosition) : null;
                    if (fileField != null && icebergFileStruct != null && filePosition < icebergFileStruct.fields().size()) {
                        icebergFileField = icebergFileStruct.fields().get(filePosition);
                    }
                }
            }

            if (fileField == null) {
                // Field was added to the table after the file was written; reader will produce null
                resultFields.add(tableField);
                changed = true;
            }
            else {
                filePosition++;
                org.apache.iceberg.types.Type icebergFileFieldType = (icebergFileField != null)
                        ? icebergFileField.type() : null;
                Type mergedType = readType(
                        tableField.getType(),
                        fileField.getType(),
                        typeManager,
                        tableFieldIdentity,
                        icebergFileFieldType);
                // When matched via Avro encoding or by ID (rename), the file field carries the
                // physical name stored in Parquet (e.g. the encoded or old name). Use that name
                // in the result so constructField can locate the column in the Parquet GroupColumnIO.
                Optional<String> resultName = (matchedViaAvroEncoding || matchedById)
                        ? fileField.getName() : tableField.getName();
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

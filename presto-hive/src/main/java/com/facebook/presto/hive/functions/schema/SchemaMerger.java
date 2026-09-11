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
package com.facebook.presto.hive.functions.schema;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeUtils.isApproximateNumericType;
import static com.facebook.presto.common.type.TypeUtils.isExactNumericType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SchemaMerger
{
    private static final int DECIMAL_MAX_PRECISION = 38;

    private SchemaMerger() {}

    /**
     * Column key is lowercased name; output order follows first-seen ordering.
     * Incompatible types throw {@code INVALID_FUNCTION_ARGUMENT}; no VARCHAR fallback.
     *
     * @param filePaths parallel to {@code perFileSchemas}; used only in error messages.
     */
    public static List<ColumnMetadata> merge(List<List<ColumnMetadata>> perFileSchemas, List<String> filePaths, TypeManager typeManager)
    {
        requireNonNull(perFileSchemas, "perFileSchemas is null");
        requireNonNull(filePaths, "filePaths is null");
        requireNonNull(typeManager, "typeManager is null");
        if (perFileSchemas.size() != filePaths.size()) {
            throw new IllegalArgumentException(format("perFileSchemas has %d entries but filePaths has %d", perFileSchemas.size(), filePaths.size()));
        }
        if (perFileSchemas.isEmpty()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot merge schemas: no files");
        }

        int fileCount = perFileSchemas.size();
        LinkedHashMap<String, MergedColumn> byKey = new LinkedHashMap<>();
        for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
            String filePath = filePaths.get(fileIndex);
            for (ColumnMetadata column : perFileSchemas.get(fileIndex)) {
                String key = column.getName().toLowerCase(Locale.ROOT);
                MergedColumn existing = byKey.get(key);
                if (existing == null) {
                    byKey.put(key, new MergedColumn(column.getName(), column.getType(), column.isNullable(), filePath));
                }
                else {
                    existing.type = widen(existing.type, column.getType(), column.getName(), existing.firstSeenPath, filePath, typeManager);
                    existing.nullable = existing.nullable || column.isNullable();
                    existing.seenCount++;
                }
            }
        }

        ImmutableList.Builder<ColumnMetadata> result = ImmutableList.builder();
        for (MergedColumn merged : byKey.values()) {
            boolean nullable = merged.nullable || merged.seenCount < fileCount;
            result.add(ColumnMetadata.builder()
                    .setName(merged.name)
                    .setType(merged.type)
                    .setNullable(nullable)
                    .build());
        }
        return result.build();
    }

    private static Type widen(Type a, Type b, String columnPath, String fileA, String fileB, TypeManager typeManager)
    {
        if (a.equals(b)) {
            return a;
        }
        if (isExactNumericType(a) && isExactNumericType(b)) {
            return ((FixedWidthType) a).getFixedSize() >= ((FixedWidthType) b).getFixedSize() ? a : b;
        }
        if ((isExactNumericType(a) || isApproximateNumericType(a)) && (isExactNumericType(b) || isApproximateNumericType(b))) {
            return DOUBLE;
        }
        if (a instanceof DecimalType && b instanceof DecimalType) {
            return widerDecimal((DecimalType) a, (DecimalType) b, columnPath, fileA, fileB);
        }
        if (a instanceof VarcharType && b instanceof VarcharType) {
            VarcharType va = (VarcharType) a;
            VarcharType vb = (VarcharType) b;
            if (va.isUnbounded() || vb.isUnbounded()) {
                return VarcharType.createUnboundedVarcharType();
            }
            return VarcharType.createVarcharType(max(va.getLengthSafe(), vb.getLengthSafe()));
        }
        if (a instanceof CharType && b instanceof CharType) {
            return CharType.createCharType(max(((CharType) a).getLength(), ((CharType) b).getLength()));
        }
        if (a instanceof RowType && b instanceof RowType) {
            return mergeRow((RowType) a, (RowType) b, columnPath, fileA, fileB, typeManager);
        }
        if (a instanceof ArrayType && b instanceof ArrayType) {
            Type element = widen(((ArrayType) a).getElementType(), ((ArrayType) b).getElementType(), columnPath + ".element", fileA, fileB, typeManager);
            return new ArrayType(element);
        }
        if (a instanceof MapType && b instanceof MapType) {
            Type key = widen(((MapType) a).getKeyType(), ((MapType) b).getKeyType(), columnPath + ".key", fileA, fileB, typeManager);
            Type value = widen(((MapType) a).getValueType(), ((MapType) b).getValueType(), columnPath + ".value", fileA, fileB, typeManager);
            return typeManager.getParameterizedType(
                    StandardTypes.MAP,
                    ImmutableList.of(TypeSignatureParameter.of(key.getTypeSignature()), TypeSignatureParameter.of(value.getTypeSignature())));
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format(
                "Incompatible types for column '%s': %s in %s vs %s in %s",
                columnPath, a, fileA, b, fileB));
    }

    private static RowType mergeRow(RowType a, RowType b, String columnPath, String fileA, String fileB, TypeManager typeManager)
    {
        LinkedHashMap<String, RowType.Field> mergedFields = new LinkedHashMap<>();
        List<RowType.Field> aFields = a.getFields();
        for (RowType.Field field : aFields) {
            String fieldKey = fieldKey(field);
            mergedFields.put(fieldKey, field);
        }
        for (RowType.Field bField : b.getFields()) {
            String fieldKey = fieldKey(bField);
            RowType.Field existing = mergedFields.get(fieldKey);
            if (existing == null) {
                mergedFields.put(fieldKey, bField);
            }
            else {
                String nestedPath = columnPath + "." + bField.getName().orElse(fieldKey);
                Type merged = widen(existing.getType(), bField.getType(), nestedPath, fileA, fileB, typeManager);
                mergedFields.put(fieldKey, new RowType.Field(existing.getName(), merged));
            }
        }
        return RowType.from(new ArrayList<>(mergedFields.values()));
    }

    private static String fieldKey(RowType.Field field)
    {
        Optional<String> name = field.getName();
        if (!name.isPresent()) {
            return "$anonymous_" + System.identityHashCode(field);
        }
        return name.get().toLowerCase(Locale.ROOT);
    }

    private static DecimalType widerDecimal(DecimalType a, DecimalType b, String columnPath, String fileA, String fileB)
    {
        int integerDigits = max(a.getPrecision() - a.getScale(), b.getPrecision() - b.getScale());
        int scale = max(a.getScale(), b.getScale());
        int precision = integerDigits + scale;
        if (precision > DECIMAL_MAX_PRECISION) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format(
                    "Cannot merge DECIMAL types for column '%s': widened to precision %d which exceeds max %d. %s in %s vs %s in %s",
                    columnPath, precision, DECIMAL_MAX_PRECISION, a, fileA, b, fileB));
        }
        return DecimalType.createDecimalType(precision, scale);
    }

    private static final class MergedColumn
    {
        final String name;
        Type type;
        boolean nullable;
        final String firstSeenPath;
        int seenCount = 1;

        MergedColumn(String name, Type type, boolean nullable, String firstSeenPath)
        {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.firstSeenPath = firstSeenPath;
        }
    }
}

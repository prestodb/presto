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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_TYPE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Converts the raw DuckLake catalog type strings recorded on {@link DuckLakeColumnIdentity} nodes
 * (spec &sect;4.4) into Presto {@link Type}s. Unsupported DuckLake types fail the whole table
 * (rather than silently hiding the offending column) with {@link
 * DuckLakeErrorCode#DUCKLAKE_UNSUPPORTED_TYPE}, naming both the column and the DuckLake type.
 */
public final class TypeConverter
{
    private static final Pattern UNQUOTED_IDENTIFIER = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    private static final Pattern DECIMAL_TYPE = Pattern.compile("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);

    private TypeConverter() {}

    /**
     * Recursively converts a column identity (and its children, for {@code list}/{@code
     * struct}/{@code map}) to a Presto type.
     */
    public static Type toPrestoType(DuckLakeColumnIdentity column, TypeManager typeManager)
    {
        requireNonNull(column, "column is null");
        requireNonNull(typeManager, "typeManager is null");

        switch (column.getTypeCategory()) {
            case PRIMITIVE:
                return toPrestoType(column.getDuckLakeType(), column.getName());
            case ARRAY:
                checkChildCount(column);
                return new ArrayType(toPrestoType(column.getChildren().get(0), typeManager));
            case STRUCT:
                checkChildCount(column);
                List<RowType.Field> fields = column.getChildren().stream()
                        .map(child -> RowType.field(child.getName(), toPrestoType(child, typeManager), needsDelimiting(child.getName())))
                        .collect(toImmutableList());
                return RowType.from(fields);
            case MAP:
                checkChildCount(column);
                DuckLakeColumnIdentity keyColumn = column.getChildren().get(0);
                DuckLakeColumnIdentity valueColumn = column.getChildren().get(1);
                TypeSignature keySignature = toPrestoType(keyColumn, typeManager).getTypeSignature();
                TypeSignature valueSignature = toPrestoType(valueColumn, typeManager).getTypeSignature();
                return typeManager.getParameterizedType(
                        StandardTypes.MAP,
                        ImmutableList.of(TypeSignatureParameter.of(keySignature), TypeSignatureParameter.of(valueSignature)));
            default:
                throw new IllegalStateException("Unexpected type category: " + column.getTypeCategory());
        }
    }

    /**
     * Converts a primitive DuckLake type string to a Presto type. {@code columnName} is used only
     * for the error message thrown for an unsupported type.
     */
    public static Type toPrestoType(String duckLakeType, String columnName)
    {
        requireNonNull(duckLakeType, "duckLakeType is null");
        requireNonNull(columnName, "columnName is null");

        String normalized = duckLakeType.trim().toLowerCase(Locale.ENGLISH);
        switch (normalized) {
            case "boolean":
                return BooleanType.BOOLEAN;
            case "int8":
                return TinyintType.TINYINT;
            case "int16":
                return SmallintType.SMALLINT;
            case "int32":
                return IntegerType.INTEGER;
            case "int64":
                return BigintType.BIGINT;
            case "uint8":
                // Widened: an unsigned 8-bit value does not fit TINYINT.
                return SmallintType.SMALLINT;
            case "uint16":
                // Widened: an unsigned 16-bit value does not fit SMALLINT.
                return IntegerType.INTEGER;
            case "uint32":
                // Widened: an unsigned 32-bit value does not fit INTEGER.
                return BigintType.BIGINT;
            case "float32":
                return RealType.REAL;
            case "float64":
                return DoubleType.DOUBLE;
            case "date":
                return DateType.DATE;
            case "time":
                return TimeType.TIME;
            case "timestamp":
            case "timestamp_s":
            case "timestamp_ms":
            case "timestamp_ns":
                return TimestampType.TIMESTAMP;
            case "timestamptz":
                return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
            case "varchar":
                return VarcharType.createUnboundedVarcharType();
            case "json":
                return JsonType.JSON;
            case "uuid":
                return UuidType.UUID;
            case "blob":
                return VarbinaryType.VARBINARY;
            default:
                Matcher decimalMatcher = DECIMAL_TYPE.matcher(normalized);
                if (decimalMatcher.matches()) {
                    int precision = Integer.parseInt(decimalMatcher.group(1));
                    int scale = Integer.parseInt(decimalMatcher.group(2));
                    return DecimalType.createDecimalType(precision, scale);
                }
                throw new PrestoException(DUCKLAKE_UNSUPPORTED_TYPE, format(
                        "Unsupported DuckLake type '%s' for column '%s'", duckLakeType, columnName));
        }
    }

    private static void checkChildCount(DuckLakeColumnIdentity column)
    {
        int actualChildCount = column.getChildren().size();
        int expected = expectedChildCount(column.getTypeCategory());
        if (expected >= 0 && actualChildCount != expected) {
            throw new PrestoException(DUCKLAKE_INVALID_METADATA, format(
                    "Expected %s column '%s' to have %s child(ren) but found %s",
                    column.getTypeCategory(), column.getName(), expected, actualChildCount));
        }
        if (expected < 0 && actualChildCount < 1) {
            throw new PrestoException(DUCKLAKE_INVALID_METADATA, format(
                    "Expected %s column '%s' to have at least one child", column.getTypeCategory(), column.getName()));
        }
    }

    private static int expectedChildCount(DuckLakeColumnIdentity.TypeCategory typeCategory)
    {
        switch (typeCategory) {
            case ARRAY:
                return 1;
            case MAP:
                return 2;
            case STRUCT:
                return -1; // any positive number of fields
            default:
                throw new IllegalStateException("Unexpected type category: " + typeCategory);
        }
    }

    private static boolean needsDelimiting(String name)
    {
        return !UNQUOTED_IDENTIFIER.matcher(name).matches();
    }
}

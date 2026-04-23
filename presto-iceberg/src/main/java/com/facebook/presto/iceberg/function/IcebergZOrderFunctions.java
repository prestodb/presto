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
package com.facebook.presto.iceberg.function;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.util.ZOrderByteUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

/**
 * Presto scalar functions for Z-Order operations.
 * These functions convert values to lexicographically ordered byte representations
 * and interleave bits for Z-Order curve computation.
 *
 * Based on Apache Iceberg Spark's Z-Order UDF implementation:
 * https://github.com/apache/iceberg/blob/main/spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/SparkZOrderUDF.java
 */
public final class IcebergZOrderFunctions
{
    private IcebergZOrderFunctions() {}

    private static final byte[] PRIMITIVE_EMPTY = new byte[ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE];

    // Private helper functions for type conversion
    private static Slice zorderTinyintBytes(Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.tinyintToOrderedBytes(value.byteValue());
    }

    private static Slice zorderSmallintBytes(Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.shortToOrderedBytes(value.shortValue());
    }

    private static Slice zorderIntegerBytes(Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.intToOrderedBytes(value.intValue());
    }

    private static Slice zorderBigintBytes(Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.longToOrderedBytes(value);
    }

    private static Slice zorderRealBytes(Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.floatToOrderedBytes(Float.intBitsToFloat(value.intValue()));
    }

    private static Slice zorderDoubleBytes(Double value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.doubleToOrderedBytes(value);
    }

    private static Slice zorderBooleanBytes(Boolean value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.booleanToOrderedBytes(value);
    }

    private static Slice zorderDateBytes(Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.intToOrderedBytes(value.intValue());
    }

    private static Slice zorderTimestampBytes(Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.longToOrderedBytes(value);
    }

    /**
     * Computes Z-order value from a ROW of columns.
     * Usage: zorder(ROW(col1, col2, col3, ...))
     *
     * @param rowType The type information for the row
     * @param rowBlock The row block containing the column values
     * @return The interleaved Z-order binary value
     */
    @ScalarFunction("zorder")
    @TypeParameter("T")
    @SqlType(StandardTypes.VARBINARY)
    @SqlNullable
    public static Slice zorder(@TypeParameter("T") Type rowType, @SqlType("T") Block rowBlock)
    {
        if (!(rowType instanceof RowType)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "zorder function requires a ROW type");
        }

        RowType row = (RowType) rowType;
        List<RowType.Field> fields = row.getFields();
        int fieldCount = fields.size();

        if (fieldCount == 0) {
            return Slices.wrappedBuffer(new byte[0]);
        }

        // Convert each field to ordered bytes based on its type
        Slice[] columnBytes = new Slice[fieldCount];
        int totalSize = 0;

        for (int i = 0; i < fieldCount; i++) {
            Type fieldType = fields.get(i).getType();

            if (rowBlock.isNull(i)) {
                // Use zero-filled bytes for null values
                columnBytes[i] = Slices.wrappedBuffer(PRIMITIVE_EMPTY);
                totalSize += PRIMITIVE_EMPTY.length;
            }
            else {
                columnBytes[i] = convertFieldToOrderedBytes(fieldType, rowBlock, i);
                totalSize += columnBytes[i].length();
            }
        }

        return ZOrderByteUtils.interleaveBits(columnBytes, totalSize);
    }

    /**
     * Converts a field from a row block to ordered bytes based on its type.
     */
    private static Slice convertFieldToOrderedBytes(Type fieldType, Block rowBlock, int fieldIndex)
    {
        String typeName = fieldType.getTypeSignature().getBase();

        switch (typeName) {
            case StandardTypes.TINYINT:
                return zorderTinyintBytes(fieldType.getLong(rowBlock, fieldIndex));
            case StandardTypes.SMALLINT:
                return zorderSmallintBytes(fieldType.getLong(rowBlock, fieldIndex));
            case StandardTypes.INTEGER:
                return zorderIntegerBytes(fieldType.getLong(rowBlock, fieldIndex));
            case StandardTypes.BIGINT:
                return zorderBigintBytes(fieldType.getLong(rowBlock, fieldIndex));
            case StandardTypes.REAL:
                return zorderRealBytes(fieldType.getLong(rowBlock, fieldIndex));
            case StandardTypes.DOUBLE:
                return zorderDoubleBytes(fieldType.getDouble(rowBlock, fieldIndex));
            case StandardTypes.BOOLEAN:
                return zorderBooleanBytes(fieldType.getBoolean(rowBlock, fieldIndex));
            case StandardTypes.DATE:
                return zorderDateBytes(fieldType.getLong(rowBlock, fieldIndex));
            case StandardTypes.TIMESTAMP:
                return zorderTimestampBytes(fieldType.getLong(rowBlock, fieldIndex));
            case StandardTypes.VARCHAR:
                // Use a default length of 32 bytes for VARCHAR fields
                Slice varcharValue = fieldType.getSlice(rowBlock, fieldIndex);
                return ZOrderByteUtils.stringToOrderedBytes(varcharValue, 32);
            case StandardTypes.VARBINARY:
                // Use a default length of 32 bytes for VARBINARY fields
                Slice varbinaryValue = fieldType.getSlice(rowBlock, fieldIndex);
                return ZOrderByteUtils.byteTruncateOrFill(varbinaryValue, 32);
            default:
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        format("Cannot use column of type %s in ZOrdering, the type is unsupported. Supported types are: TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN, DATE, TIMESTAMP, VARCHAR, VARBINARY", typeName));
        }
    }
}

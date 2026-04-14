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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.iceberg.util.ZOrderByteUtils;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

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

    @ScalarFunction(value = "zorder_tinyint_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderTinyintBytes(@SqlNullable @SqlType(StandardTypes.TINYINT) Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.tinyintToOrderedBytes(value.byteValue());
    }

    @ScalarFunction(value = "zorder_smallint_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderSmallintBytes(@SqlNullable @SqlType(StandardTypes.SMALLINT) Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.shortToOrderedBytes(value.shortValue());
    }

    @ScalarFunction(value = "zorder_integer_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderIntegerBytes(@SqlNullable @SqlType(StandardTypes.INTEGER) Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.intToOrderedBytes(value.intValue());
    }

    @ScalarFunction(value = "zorder_bigint_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderBigintBytes(@SqlNullable @SqlType(StandardTypes.BIGINT) Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.longToOrderedBytes(value);
    }

    @ScalarFunction(value = "zorder_real_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderRealBytes(@SqlNullable @SqlType(StandardTypes.REAL) Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.floatToOrderedBytes(Float.intBitsToFloat(value.intValue()));
    }

    @ScalarFunction(value = "zorder_double_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderDoubleBytes(@SqlNullable @SqlType(StandardTypes.DOUBLE) Double value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.doubleToOrderedBytes(value);
    }

    @ScalarFunction(value = "zorder_boolean_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderBooleanBytes(@SqlNullable @SqlType(StandardTypes.BOOLEAN) Boolean value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.booleanToOrderedBytes(value);
    }

    @ScalarFunction(value = "zorder_varchar_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderVarcharBytes(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(StandardTypes.INTEGER) long length)
    {
        // stringToOrderedBytes already handles null by returning zero-filled array
        return ZOrderByteUtils.stringToOrderedBytes(value, (int) length);
    }

    @ScalarFunction(value = "zorder_varbinary_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderVarbinaryBytes(@SqlNullable @SqlType(StandardTypes.VARBINARY) Slice value, @SqlType(StandardTypes.INTEGER) long length)
    {
        // byteTruncateOrFill already handles null by returning zero-filled array
        return ZOrderByteUtils.byteTruncateOrFill(value, (int) length);
    }

    @ScalarFunction(value = "zorder_date_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderDateBytes(@SqlNullable @SqlType(StandardTypes.DATE) Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.intToOrderedBytes(value.intValue());
    }

    @ScalarFunction(value = "zorder_timestamp_bytes", calledOnNullInput = true)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zorderTimestampBytes(@SqlNullable @SqlType(StandardTypes.TIMESTAMP) Long value)
    {
        if (value == null) {
            return Slices.wrappedBuffer(PRIMITIVE_EMPTY);
        }
        return ZOrderByteUtils.longToOrderedBytes(value);
    }

    /**
     * Interleaves bits from an array of binary values to create a Z-order value.
     *
     * @param array Array of binary values (each value is the output of a zorder_*_bytes function)
     * @param interleavedSize The size of the output in bytes
     * @return The interleaved binary value
     */
    @ScalarFunction("zorder")
    @SqlType(StandardTypes.VARBINARY)
    @SqlNullable
    public static Slice zorder(@SqlType("array(varbinary)") Block array, @SqlType(StandardTypes.INTEGER) long interleavedSize)
    {
        if (array == null || array.getPositionCount() == 0) {
            // Return zero-filled array for empty input
            byte[] bytes = new byte[(int) interleavedSize];
            return Slices.wrappedBuffer(bytes);
        }

        Slice[] columnsBinary = new Slice[array.getPositionCount()];
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                // Return zero-filled array if any column is null
                byte[] bytes = new byte[(int) interleavedSize];
                return Slices.wrappedBuffer(bytes);
            }
            columnsBinary[i] = array.getSlice(i, 0, array.getSliceLength(i));
        }

        return ZOrderByteUtils.interleaveBits(columnsBinary, (int) interleavedSize);
    }
}

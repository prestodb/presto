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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorType.ANY;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;

public final class ArrayFunctions
{
    private ArrayFunctions()
    {
    }

    @ScalarFunction(hidden = true)
    @SqlType("array(unknown)")
    public static Block arrayConstructor()
    {
        BlockBuilder blockBuilder = new ArrayType(UNKNOWN).createBlockBuilder(new BlockBuilderStatus(), 0);
        return blockBuilder.build();
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyBigint(@SqlType(StandardTypes.BIGINT) long left, @SqlType("array<bigint>") Block right)
    {
        return contains(BigintType.BIGINT, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyBigint(@SqlType("array<bigint>") Block left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return contains(BigintType.BIGINT, left, right);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyBoolean(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType("array<boolean>") Block right)
    {
        return contains(BooleanType.BOOLEAN, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyBoolean(@SqlType("array<boolean>") Block left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return contains(BooleanType.BOOLEAN, left, right);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyDate(@SqlType(StandardTypes.DATE) long left, @SqlType("array<date>") Block right)
    {
        return contains(DateType.DATE, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyDate(@SqlType("array<date>") Block left, @SqlType(StandardTypes.DATE) long right)
    {
        return contains(DateType.DATE, left, right);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyDouble(@SqlType(StandardTypes.DOUBLE) double left, @SqlType("array<double>") Block right)
    {
        return contains(DoubleType.DOUBLE, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyDouble(@SqlType("array<double>") Block left, @SqlType(StandardTypes.DOUBLE) double right)
    {
        return contains(DoubleType.DOUBLE, left, right);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyTime(@SqlType(StandardTypes.TIME) long left, @SqlType("array<time>") Block right)
    {
        return contains(TimeType.TIME, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyTime(@SqlType("array<time>") Block left, @SqlType(StandardTypes.TIME) long right)
    {
        return contains(TimeType.TIME, left, right);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyTimestamp(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType("array<timestamp>") Block right)
    {
        return contains(TimestampType.TIMESTAMP, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyTimestamp(@SqlType("array<timestamp>") Block left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return contains(TimestampType.TIMESTAMP, left, right);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyVarbinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType("array<varbinary>") Block right)
    {
        return contains(VarbinaryType.VARBINARY, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyVarbinary(@SqlType("array<varbinary>") Block left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        return contains(VarbinaryType.VARBINARY, left, right);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType("array<varchar>") Block right)
    {
        return contains(VarcharType.VARCHAR, right, left);
    }

    @ScalarOperator(ANY)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean anyVarchar(@SqlType("array<varchar>") Block left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        return contains(VarcharType.VARCHAR, left, right);
    }

    private static Boolean contains(Type elementType, Block arrayBlock, boolean value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if (elementType.getBoolean(arrayBlock, i) == value) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    private static Boolean contains(Type elementType, Block arrayBlock, double value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if (elementType.getDouble(arrayBlock, i) == value) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    private static Boolean contains(Type elementType, Block arrayBlock, long value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if (elementType.getLong(arrayBlock, i) == value) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }

    private static Boolean contains(Type elementType, Block arrayBlock, Slice value)
    {
        boolean foundNull = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            if (arrayBlock.isNull(i)) {
                foundNull = true;
                continue;
            }
            try {
                if (elementType.getSlice(arrayBlock, i).equals(value)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return false;
    }
}

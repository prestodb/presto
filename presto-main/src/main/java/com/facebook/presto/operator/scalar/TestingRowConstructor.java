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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.google.common.base.Preconditions.checkArgument;

public final class TestingRowConstructor
{
    private TestingRowConstructor() {}

    @ScalarFunction("test_row")
    @SqlType("row<bigint,bigint>('col0','col1')")
    public static Block testRowBigintBigint(@Nullable @SqlType(StandardTypes.BIGINT) Long arg1, @Nullable @SqlType(StandardTypes.BIGINT) Long arg2)
    {
        return toStackRepresentation(ImmutableList.of(BIGINT, BIGINT), arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<bigint,double>('col0','col1')")
    public static Block testRowBigintBigint(@Nullable @SqlType(StandardTypes.BIGINT) Long arg1, @Nullable @SqlType(StandardTypes.DOUBLE) Double arg2)
    {
        return toStackRepresentation(ImmutableList.of(BIGINT, DOUBLE), arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<bigint,double,boolean,varchar,timestamp>('col0','col1','col2','col3','col4')")
    public static Block testRowBigintDoubleBooleanVarcharTimestamp(@Nullable @SqlType(StandardTypes.BIGINT) Long arg1, @Nullable @SqlType(StandardTypes.DOUBLE) Double arg2,
                                                          @Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg3, @Nullable @SqlType(StandardTypes.VARCHAR) Slice arg4,
                                                          @Nullable @SqlType(StandardTypes.TIMESTAMP) Long arg5)
    {
        return toStackRepresentation(ImmutableList.of(BIGINT, DOUBLE, BOOLEAN, VARCHAR, TIMESTAMP), arg1, arg2, arg3, arg4, arg5);
    }

    @ScalarFunction("test_row")
    @SqlType("row<HyperLogLog>('col0')")
    public static Block testRowHyperLogLog(@Nullable @SqlType(StandardTypes.HYPER_LOG_LOG) Slice arg1)
    {
        return toStackRepresentation(ImmutableList.of(HYPER_LOG_LOG), arg1);
    }

    @ScalarFunction("test_row")
    @SqlType("row<double,row<timestamp with time zone,timestamp with time zone>('col0','col1')>('col2','col3')")
    public static Block testNestedRowsWithTimestampsWithTimeZones(@Nullable @SqlType(StandardTypes.DOUBLE) Double arg1,
                                                                  @Nullable @SqlType("row<timestamp with time zone,timestamp with time zone>('col0','col1')") Block arg2)
    {
        List<Type> parameterTypes = ImmutableList.of(
                DOUBLE,
                new RowType(ImmutableList.of(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE), Optional.of(ImmutableList.of("col0", "col1"))));
        return toStackRepresentation(parameterTypes, arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<timestamp with time zone,timestamp with time zone>('col0','col1')")
    public static Block testRowTimestampsWithTimeZones(@Nullable @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) Long arg1,
                                                                   @Nullable @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) Long arg2)
    {
        return toStackRepresentation(ImmutableList.of(TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE), arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<double,double>('col0','col1')")
    public static Block testRowDoubleDouble(@Nullable @SqlType(StandardTypes.DOUBLE) Double arg1, @Nullable @SqlType(StandardTypes.DOUBLE) Double arg2)
    {
        return toStackRepresentation(ImmutableList.of(DOUBLE, DOUBLE), arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<double,varchar>('col0','col1')")
    public static Block testRowDoubleBigint(@Nullable @SqlType(StandardTypes.DOUBLE) Double arg1, @Nullable @SqlType(StandardTypes.VARCHAR) Slice arg2)
    {
        return toStackRepresentation(ImmutableList.of(DOUBLE, VARCHAR), arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<boolean,boolean>('col0','col1')")
    public static Block testRowBigintBigint(@Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg1, @Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg2)
    {
        return toStackRepresentation(ImmutableList.of(BOOLEAN, BOOLEAN), arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<boolean,boolean,boolean,boolean>('col0','col1','col2','col3')")
    public static Block testRowFourBooleans(@Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg1, @Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg2,
                                              @Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg3, @Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg4)
    {
        return toStackRepresentation(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN), arg1, arg2, arg3, arg4);
    }

    @ScalarFunction("test_row")
    @SqlType("row<boolean,array(bigint)>('col0','col1')")
    public static Block testRowBooleanArray(@Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg1, @Nullable @SqlType("array(bigint)") Block arg2)
    {
        List<Type> parameterTypes = ImmutableList.of(BOOLEAN, new ArrayType(BIGINT));
        return toStackRepresentation(parameterTypes, arg1, arg2);
    }

    @ScalarFunction("test_row")
    @SqlType("row<boolean,array(bigint),map(bigint,double)>('col0','col1','col2')")
    public static Block testRowBooleanArrayMap(@Nullable @SqlType(StandardTypes.BOOLEAN) Boolean arg1, @Nullable @SqlType("array(bigint)") Block arg2,
                                               @Nullable @SqlType("map(bigint,double)") Block arg3)
    {
        List<Type> parameterTypes = ImmutableList.of(BOOLEAN, new ArrayType(BIGINT), new MapType(BIGINT, DOUBLE));
        return toStackRepresentation(parameterTypes, arg1, arg2, arg3);
    }

    @ScalarFunction("test_row")
    @SqlType("row<double,array(bigint),row<bigint,double>('col0','col1')>('col0','col1','col2')")
    public static Block testNestedRow(@Nullable @SqlType(StandardTypes.DOUBLE) Double arg1, @Nullable @SqlType("array(bigint)") Block arg2,
                                               @Nullable @SqlType("row<bigint,double>('col0','col1')") Block arg3)
    {
        List<Type> parameterTypes = ImmutableList.of(
                DOUBLE, new ArrayType(BIGINT),
                new RowType(ImmutableList.of(BIGINT, DOUBLE), Optional.of(ImmutableList.of("col0", "col1"))));
        return toStackRepresentation(parameterTypes, arg1, arg2, arg3);
    }

    @ScalarFunction("test_row")
    @SqlType("row<double,array(row<bigint,double>('col0','col1')),row<bigint,double>('col0','col1')>('col0','col1','col2')")
    public static Block testNestedRowWithArray(
            @Nullable @SqlType(StandardTypes.DOUBLE) Double arg1,
            @Nullable @SqlType("array(row<bigint,double>('col0','col1'))") Block arg2,
            @Nullable @SqlType("row<bigint,double>('col0','col1')") Block arg3)
    {
        List<Type> parameterTypes = ImmutableList.of(
                DOUBLE,
                new ArrayType(new RowType(ImmutableList.of(BIGINT, DOUBLE), Optional.of(ImmutableList.of("col0", "col1")))),
                new RowType(ImmutableList.of(BIGINT, DOUBLE), Optional.of(ImmutableList.of("col0", "col1"))));
        return toStackRepresentation(parameterTypes, arg1, arg2, arg3);
    }

    @ScalarFunction("test_row")
    @SqlType("row<timestamp>('col0')")
    public static Block testRowBigintBigint(@Nullable @SqlType(StandardTypes.TIMESTAMP) Long arg1)
    {
        return toStackRepresentation(ImmutableList.of(TIMESTAMP), arg1);
    }

    @ScalarFunction("test_non_lowercase_row")
    @SqlType("row<bigint>('Col0')")
    public static Block testNonLowercaseRowBigint(@Nullable @SqlType(StandardTypes.BIGINT) Long arg1)
    {
        return toStackRepresentation(ImmutableList.of(BIGINT), arg1);
    }

    public static Block toStackRepresentation(List<Type> parameterTypes, Object... values)
    {
        checkArgument(parameterTypes.size() == values.length, "parameterTypes.size(" + parameterTypes.size() + ") does not equal to values.length(" + values.length + ")");

        BlockBuilder blockBuilder =  new InterleavedBlockBuilder(parameterTypes, new BlockBuilderStatus(), 1024);
        for (int i = 0; i < values.length; i++) {
            appendToBlockBuilder(parameterTypes.get(i), values[i], blockBuilder);
        }
        return blockBuilder.build();
    }
}

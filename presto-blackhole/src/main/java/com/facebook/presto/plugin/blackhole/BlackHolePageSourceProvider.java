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
package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.math.BigDecimal.ZERO;
import static java.util.Objects.requireNonNull;

public final class BlackHolePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ListeningScheduledExecutorService executorService;

    public BlackHolePageSourceProvider(ListeningScheduledExecutorService executorService)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        BlackHoleSplit blackHoleSplit = (BlackHoleSplit) split;

        ImmutableList.Builder<Type> builder = ImmutableList.builder();

        for (ColumnHandle column : columns) {
            builder.add(((BlackHoleColumnHandle) column).getColumnType());
        }
        List<Type> types = builder.build();

        Page page = generateZeroPage(types, blackHoleSplit.getRowsPerPage(), blackHoleSplit.getFieldsLength());
        return new BlackHolePageSource(page, blackHoleSplit.getPagesCount(), executorService, blackHoleSplit.getPageProcessingDelay());
    }

    private Page generateZeroPage(List<Type> types, int rowsCount, int fieldLength)
    {
        byte[] constantBytes = new byte[fieldLength];
        Arrays.fill(constantBytes, (byte) 42);
        Slice constantSlice = Slices.wrappedBuffer(constantBytes);

        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = createZeroBlock(types.get(i), rowsCount, constantSlice);
        }

        return new Page(rowsCount, blocks);
    }

    private Block createZeroBlock(Type type, int rowsCount, Slice constantSlice)
    {
        checkArgument(isSupportedType(type), "Unsupported type [%s]", type);

        Slice slice;
        // do not exceed varchar limit
        if (isVarcharType(type)) {
            slice = constantSlice.slice(0, Math.min(((VarcharType) type).getLength(), constantSlice.length()));
        }
        else if (isLongDecimal(type)) {
            slice = encodeScaledValue(ZERO);
        }
        else {
            slice = constantSlice;
        }

        BlockBuilder builder;
        if (type instanceof FixedWidthType) {
            builder = type.createBlockBuilder(null, rowsCount);
        }
        else {
            builder = type.createBlockBuilder(null, rowsCount, slice.length());
        }

        for (int i = 0; i < rowsCount; i++) {
            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
                type.writeBoolean(builder, false);
            }
            else if (javaType == long.class) {
                type.writeLong(builder, 0);
            }
            else if (javaType == double.class) {
                type.writeDouble(builder, 0.0);
            }
            else if (javaType == Slice.class) {
                requireNonNull(slice, "slice is null");
                type.writeSlice(builder, slice, 0, slice.length());
            }
            else {
                throw new UnsupportedOperationException("Unknown javaType: " + javaType.getName());
            }
        }
        return builder.build();
    }

    private boolean isSupportedType(Type type)
    {
        return ImmutableSet.<Type>of(TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, BOOLEAN, DATE, TIMESTAMP, VARBINARY).contains(type)
                || isVarcharType(type) || type instanceof DecimalType;
    }
}

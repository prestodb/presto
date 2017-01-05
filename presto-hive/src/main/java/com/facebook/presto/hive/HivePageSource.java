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
package com.facebook.presto.hive;

import com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.charPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.floatPartitionKey;
import static com.facebook.presto.hive.HiveUtil.integerPartitionKey;
import static com.facebook.presto.hive.HiveUtil.longDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.shortDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.smallintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.HiveUtil.tinyintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.varcharPartitionKey;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HivePageSource
        implements ConnectorPageSource
{
    private List<ColumnMapping> columnMappings;
    private final Object[] prefilledValues;
    private final Type[] types;
    private final Function<Block, Block>[] coercers;

    private final ConnectorPageSource delegate;

    public HivePageSource(
            List<ColumnMapping> columnMappings,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ConnectorPageSource delegate)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;

        int size = columnMappings.size();

        prefilledValues = new Object[size];
        types = new Type[size];
        coercers = new Function[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;

            if (columnMapping.getCoercionFrom().isPresent()) {
                coercers[columnIndex] = createCoercer(typeManager, columnMapping.getCoercionFrom().get(), columnMapping.getHiveColumnHandle().getHiveType());
            }

            if (columnMapping.isPrefilled()) {
                String columnValue = columnMapping.getPrefilledValue();
                byte[] bytes = columnValue.getBytes(UTF_8);

                Object prefilledValue;
                if (HiveUtil.isHiveNull(bytes)) {
                    prefilledValue = null;
                }
                else if (type.equals(BOOLEAN)) {
                    prefilledValue = booleanPartitionKey(columnValue, name);
                }
                else if (type.equals(BIGINT)) {
                    prefilledValue = bigintPartitionKey(columnValue, name);
                }
                else if (type.equals(INTEGER)) {
                    prefilledValue = integerPartitionKey(columnValue, name);
                }
                else if (type.equals(SMALLINT)) {
                    prefilledValue = smallintPartitionKey(columnValue, name);
                }
                else if (type.equals(TINYINT)) {
                    prefilledValue = tinyintPartitionKey(columnValue, name);
                }
                else if (type.equals(REAL)) {
                    prefilledValue = floatPartitionKey(columnValue, name);
                }
                else if (type.equals(DOUBLE)) {
                    prefilledValue = doublePartitionKey(columnValue, name);
                }
                else if (isVarcharType(type)) {
                    prefilledValue = varcharPartitionKey(columnValue, name, type);
                }
                else if (isCharType(type)) {
                    prefilledValue = charPartitionKey(columnValue, name, type);
                }
                else if (type.equals(DATE)) {
                    prefilledValue = datePartitionKey(columnValue, name);
                }
                else if (type.equals(TIMESTAMP)) {
                    prefilledValue = timestampPartitionKey(columnValue, hiveStorageTimeZone, name);
                }
                else if (isShortDecimal(type)) {
                    prefilledValue = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else if (isLongDecimal(type)) {
                    prefilledValue = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
                }

                prefilledValues[columnIndex] = prefilledValue;
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return delegate.getTotalBytes();
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Block[] blocks = new Block[columnMappings.size()];
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }
            int batchSize = dataPage.getPositionCount();
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                ColumnMapping columnMapping = columnMappings.get(fieldId);
                if (columnMapping.isPrefilled()) {
                    blocks[fieldId] = RunLengthEncodedBlock.create(types[fieldId], prefilledValues[fieldId], batchSize);
                }
                else {
                    blocks[fieldId] = dataPage.getBlock(columnMapping.getIndex());
                    if (coercers[fieldId] != null) {
                        blocks[fieldId] = new LazyBlock(batchSize, new CoercionLazyBlockLoader(blocks[fieldId], coercers[fieldId]));
                    }
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    public ConnectorPageSource getPageSource()
    {
        return delegate;
    }

    private static Function<Block, Block> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return new IntegerNumberToVarcharCoercer(fromType, toType);
        }
        else if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return new VarcharToIntegerNumberCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_BYTE) && toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_SHORT) && toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return new FloatToDoubleCoercer();
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }

    private static class IntegerNumberUpscaleCoercer
            implements Function<Block, Block>
    {
        private final Type fromType;
        private final Type toType;

        public IntegerNumberUpscaleCoercer(Type fromType, Type toType)
        {
            this.fromType = requireNonNull(fromType, "fromType is null");
            this.toType = requireNonNull(toType, "toType is null");
        }

        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = toType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                toType.writeLong(blockBuilder, fromType.getLong(block, i));
            }
            return blockBuilder.build();
        }
    }

    private static class IntegerNumberToVarcharCoercer
            implements Function<Block, Block>
    {
        private final Type fromType;
        private final Type toType;

        public IntegerNumberToVarcharCoercer(Type fromType, Type toType)
        {
            this.fromType = requireNonNull(fromType, "fromType is null");
            this.toType = requireNonNull(toType, "toType is null");
        }

        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = toType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                toType.writeSlice(blockBuilder, utf8Slice(valueOf(fromType.getLong(block, i))));
            }
            return blockBuilder.build();
        }
    }

    private static class VarcharToIntegerNumberCoercer
            implements Function<Block, Block>
    {
        private final Type fromType;
        private final Type toType;

        private final long minValue;
        private final long maxValue;

        public VarcharToIntegerNumberCoercer(Type fromType, Type toType)
        {
            this.fromType = requireNonNull(fromType, "fromType is null");
            this.toType = requireNonNull(toType, "toType is null");

            if (toType.equals(TINYINT)) {
                minValue = Byte.MIN_VALUE;
                maxValue = Byte.MAX_VALUE;
            }
            else if (toType.equals(SMALLINT)) {
                minValue = Short.MIN_VALUE;
                maxValue = Short.MAX_VALUE;
            }
            else if (toType.equals(INTEGER)) {
                minValue = Integer.MIN_VALUE;
                maxValue = Integer.MAX_VALUE;
            }
            else if (toType.equals(BIGINT)) {
                minValue = Long.MIN_VALUE;
                maxValue = Long.MAX_VALUE;
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format("Could not create Coercer from from varchar to %s", toType));
            }
        }

        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = toType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                try {
                    long value = Long.parseLong(fromType.getSlice(block, i).toStringUtf8());
                    if (minValue <= value && value <= maxValue) {
                        toType.writeLong(blockBuilder, value);
                    }
                    else {
                        blockBuilder.appendNull();
                    }
                }
                catch (NumberFormatException e) {
                    blockBuilder.appendNull();
                }
            }
            return blockBuilder.build();
        }
    }

    private static class FloatToDoubleCoercer
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                DOUBLE.writeDouble(blockBuilder, intBitsToFloat((int) REAL.getLong(block, i)));
            }
            return blockBuilder.build();
        }
    }

    private final class CoercionLazyBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final Function<Block, Block> coercer;
        private Block block;

        public CoercionLazyBlockLoader(Block block, Function<Block, Block> coercer)
        {
            this.block = requireNonNull(block, "block is null");
            this.coercer = requireNonNull(coercer, "coercer is null");
        }

        @Override
        public void load(LazyBlock lazyBlock)
        {
            if (block == null) {
                return;
            }

            Block coercedBlock = coercer.apply(block);
            lazyBlock.setBlock(coercedBlock);

            // clear reference to loader to free resources, since load was successful
            block = null;
        }
    }
}

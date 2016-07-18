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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.integerPartitionKey;
import static com.facebook.presto.hive.HiveUtil.longDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.shortDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.smallintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.HiveUtil.tinyintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.varcharPartitionKey;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class OrcPageSource
        implements ConnectorPageSource
{
    private static final int NULL_ENTRY_SIZE = 0;
    private final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;

    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private int batchId;
    private boolean closed;

    private final AggregatedMemoryContext systemMemoryContext;

    public OrcPageSource(
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");

        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(requireNonNull(partitionKeys, "partitionKeys is null"), HivePartitionKey::getName);

        int size = requireNonNull(columns, "columns is null").size();

        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(name);
                checkArgument(partitionKey != null, "No value provided for partition key %s", name);

                byte[] bytes = partitionKey.getValue().getBytes(UTF_8);

                BlockBuilder blockBuilder;
                if (type instanceof FixedWidthType) {
                    blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_BATCH_SIZE);
                }
                else {
                    blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_BATCH_SIZE, bytes.length);
                }

                if (HiveUtil.isHiveNull(bytes)) {
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        blockBuilder.appendNull();
                    }
                }
                else if (type.equals(BOOLEAN)) {
                    boolean value = booleanPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        BOOLEAN.writeBoolean(blockBuilder, value);
                    }
                }
                else if (type.equals(BIGINT)) {
                    long value = bigintPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        BIGINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(INTEGER)) {
                    long value = integerPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        INTEGER.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(SMALLINT)) {
                    long value = smallintPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        SMALLINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(TINYINT)) {
                    long value = tinyintPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        TINYINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(DOUBLE)) {
                    double value = doublePartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        DOUBLE.writeDouble(blockBuilder, value);
                    }
                }
                else if (isVarcharType(type)) {
                    Slice value = varcharPartitionKey(partitionKey.getValue(), name, type);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        type.writeSlice(blockBuilder, value);
                    }
                }
                else if (type.equals(DATE)) {
                    long value = datePartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        DATE.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(TIMESTAMP)) {
                    long value = timestampPartitionKey(partitionKey.getValue(), hiveStorageTimeZone, name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        TIMESTAMP.writeLong(blockBuilder, value);
                    }
                }
                else if (isShortDecimal(type)) {
                    long value = shortDecimalPartitionKey(partitionKey.getValue(), (DecimalType) type, name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        type.writeLong(blockBuilder, value);
                    }
                }
                else if (isLongDecimal(type)) {
                    Slice value = longDecimalPartitionKey(partitionKey.getValue(), (DecimalType) type, name);
                    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                        type.writeSlice(blockBuilder, value);
                    }
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for partition key: %s", type.getDisplayName(), name));
                }

                constantBlocks[columnIndex] = blockBuilder.build();
            }
            else if (!recordReader.isColumnPresent(column.getHiveColumnIndex())) {
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_BATCH_SIZE, NULL_ENTRY_SIZE);
                for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                    blockBuilder.appendNull();
                }
                constantBlocks[columnIndex] = blockBuilder.build();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();

        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public long getTotalBytes()
    {
        return recordReader.getSplitLength();
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            int batchSize = recordReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return null;
            }

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Type type = types.get(fieldId);
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else {
                    blocks[fieldId] = new LazyBlock(batchSize, new OrcBlockLoader(hiveColumnIndexes[fieldId], type));
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnNames)
                .add("types", types)
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            throwable.addSuppressed(e);
        }
    }

    private final class OrcBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private final Type type;
        private boolean loaded;

        public OrcBlockLoader(int columnIndex, Type type)
        {
            this.columnIndex = columnIndex;
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = recordReader.readBlock(type, columnIndex);
                lazyBlock.setBlock(block);
            }
            catch (IOException e) {
                if (e instanceof OrcCorruptionException) {
                    throw new PrestoException(HIVE_BAD_DATA, e);
                }
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }

            loaded = true;
        }
    }
}

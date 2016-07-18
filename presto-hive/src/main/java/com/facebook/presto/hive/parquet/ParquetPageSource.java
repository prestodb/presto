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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.hive.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import parquet.column.ColumnDescriptor;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetType;
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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

class ParquetPageSource
        implements ConnectorPageSource
{
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final long GUESSED_MEMORY_USAGE = new DataSize(16, DataSize.Unit.MEGABYTE).toBytes();

    private final ParquetReader parquetReader;
    private final ParquetDataSource dataSource;
    private final MessageType requestedSchema;
    // for debugging heap dump
    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private final long totalBytes;
    private int batchId;
    private boolean closed;
    private long readTimeNanos;

    public ParquetPageSource(
            ParquetReader parquetReader,
            ParquetDataSource dataSource,
            MessageType fileSchema,
            MessageType requestedSchema,
            long totalBytes,
            Properties splitSchema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            boolean useParquetColumnNames)
    {
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(effectivePredicate, "effectivePredicate is null");

        this.parquetReader = parquetReader;
        this.dataSource = dataSource;
        this.requestedSchema = requestedSchema;
        this.totalBytes = totalBytes;

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
                    blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_VECTOR_LENGTH);
                }
                else {
                    blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_VECTOR_LENGTH, bytes.length);
                }

                if (HiveUtil.isHiveNull(bytes)) {
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        blockBuilder.appendNull();
                    }
                }
                else if (type.equals(BOOLEAN)) {
                    boolean value = booleanPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        BOOLEAN.writeBoolean(blockBuilder, value);
                    }
                }
                else if (type.equals(TINYINT)) {
                    long value = tinyintPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        TINYINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(SMALLINT)) {
                    long value = smallintPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        SMALLINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(INTEGER)) {
                    long value = integerPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        INTEGER.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(BIGINT)) {
                    long value = bigintPartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        BIGINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(DOUBLE)) {
                    double value = doublePartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        DOUBLE.writeDouble(blockBuilder, value);
                    }
                }
                else if (isVarcharType(type)) {
                    Slice value = varcharPartitionKey(partitionKey.getValue(), name, type);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        type.writeSlice(blockBuilder, value);
                    }
                }
                else if (type.equals(TIMESTAMP)) {
                    long value = timestampPartitionKey(partitionKey.getValue(), hiveStorageTimeZone, name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        TIMESTAMP.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(DATE)) {
                    long value = datePartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        DATE.writeLong(blockBuilder, value);
                    }
                }
                else if (isShortDecimal(type)) {
                    long value = shortDecimalPartitionKey(partitionKey.getValue(), (DecimalType) type, name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        type.writeLong(blockBuilder, value);
                    }
                }
                else if (isLongDecimal(type)) {
                    Slice value = longDecimalPartitionKey(partitionKey.getValue(), (DecimalType) type, name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        type.writeSlice(blockBuilder, value);
                    }
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for partition key: %s", type.getDisplayName(), name));
                }

                constantBlocks[columnIndex] = blockBuilder.build();
            }
            else if (getParquetType(column, fileSchema, useParquetColumnNames) == null) {
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_VECTOR_LENGTH);
                for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                    blockBuilder.appendNull();
                }
                constantBlocks[columnIndex] = blockBuilder.build();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return dataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return GUESSED_MEMORY_USAGE;
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            long start = System.nanoTime();

            int batchSize = parquetReader.nextBatch();

            readTimeNanos += System.nanoTime() - start;

            if (closed || batchSize <= 0) {
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
                    int fieldIndex = requestedSchema.getFieldIndex(columnNames.get(fieldId));
                    ColumnDescriptor columnDescriptor = requestedSchema.getColumns().get(fieldIndex);
                    blocks[fieldId] = new LazyBlock(batchSize, new ParquetBlockLoader(columnDescriptor, type));
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (IOException | RuntimeException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private final class ParquetBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final ColumnDescriptor columnDescriptor;
        private final Type type;
        private boolean loaded;

        public ParquetBlockLoader(ColumnDescriptor columnDescriptor, Type type)
        {
            this.columnDescriptor = columnDescriptor;
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
                Block block = parquetReader.readBlock(columnDescriptor, type);
                lazyBlock.setBlock(block);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
            loaded = true;
        }
    }
}

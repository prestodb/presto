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

import com.facebook.presto.hive.parquet.BooleanColumnVector;
import com.facebook.presto.hive.parquet.DoubleColumnVector;
import com.facebook.presto.hive.parquet.LongColumnVector;
import com.facebook.presto.hive.parquet.SliceColumnVector;
import com.facebook.presto.hive.parquet.ParquetBatch;
import com.facebook.presto.hive.parquet.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.LazyFixedWidthBlock;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

class ParquetPageSource
        implements ConnectorPageSource
{
    private static final int MAX_VECTOR_LENGTH = 1024;

    private final ParquetReader parquetReader;
    private final MessageType requestedSchema;
    private final TupleDomain<HiveColumnHandle> effectivePredicate;
    private final List<String> columnNames;
    private final List<Type> types;
    private final boolean[] isStructuralType;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final boolean[] nulls;
    private final boolean[] nullsRowDefault;

    private final long totalBytes;
    private long completedBytes;
    private int batchId;
    private boolean closed;
    private long readTimeNanos;

    public ParquetPageSource(
            ParquetReader parquetReader,
            MessageType requestedSchema,
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties splitSchema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            TypeManager typeManager)
    {
        checkNotNull(path, "path is null");
        checkArgument(length >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(columns, "columns is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(effectivePredicate, "effectivePredicate is null");

        this.parquetReader = parquetReader;
        this.requestedSchema = requestedSchema;
        this.totalBytes = length;
        this.effectivePredicate = effectivePredicate;

        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(checkNotNull(partitionKeys, "partitionKeys is null"), HivePartitionKey::getName);

        int size = checkNotNull(columns, "columns is null").size();

        this.isStructuralType = new boolean[size];

        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.nulls = new boolean[size];
        this.nullsRowDefault = new boolean[size];

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

            String typeBase = column.getTypeSignature().getBase();
            isStructuralType[columnIndex] = StandardTypes.ARRAY.equals(typeBase)
                                            || StandardTypes.MAP.equals(typeBase)
                                            || StandardTypes.ROW.equals(typeBase);

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
                else if (type.equals(VARCHAR)) {
                    Slice value = Slices.wrappedBuffer(bytes);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        VARCHAR.writeSlice(blockBuilder, value);
                    }
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for partition key: %s", type.getDisplayName(), name));
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
        return completedBytes;
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
    public Page getNextPage()
    {
        try {
            batchId++;
            ParquetBatch batch = new ParquetBatch(this.requestedSchema, types);

            long start = System.nanoTime();

            batch = parquetReader.nextBatch(batch);

            readTimeNanos += System.nanoTime() - start;

            if (closed || batch == null) {
                close();
                return null;
            }

            int batchSize = parquetReader.getBatchSize();
            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Type type = types.get(fieldId);
                int hiveColumnIndex = hiveColumnIndexes[fieldId];
                PrimitiveTypeName parquetTypeName = requestedSchema.getType(fieldId).asPrimitiveType().getPrimitiveTypeName();
                MessageType columnSchema = new MessageType(requestedSchema.getFieldName(fieldId),
                                                            requestedSchema.getType(fieldId));
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else if (BOOLEAN.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(BOOLEAN.getFixedSize(),
                                                                batchSize,
                                                                new LazyBooleanBlockLoader(columnSchema, batchSize));
                }
                else if (BIGINT.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(((FixedWidthType) type).getFixedSize(),
                                                                batchSize,
                                                                new LazyLongBlockLoader(columnSchema,
                                                                                        batchSize,
                                                                                        parquetTypeName));
                }
                else if (DOUBLE.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(DOUBLE.getFixedSize(),
                                                                batchSize,
                                                                new LazyDoubleBlockLoader(columnSchema,
                                                                                            batchSize,
                                                                                            parquetTypeName));
                }
                else if (VARCHAR.equals(type)) {
                    blocks[fieldId] = new LazySliceArrayBlock(batchSize, new LazySliceBlockLoader(columnSchema, batchSize));
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type);
                }
            }
            Page page = new Page(batchSize, blocks);

            long newCompletedBytes = (long) (totalBytes * parquetReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
            return page;
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
        checkNotNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            throwable.addSuppressed(e);
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

    private final class LazyBooleanBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final MessageType columnSchema;

        public LazyBooleanBlockLoader(MessageType columnSchema, int batchSize)
        {
            this.batchSize = batchSize;
            this.columnSchema = columnSchema;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                BooleanColumnVector columnVector = new BooleanColumnVector();
                parquetReader.readColumn(columnVector, columnSchema);
                block.setNullVector(columnVector.getIsNull());
                block.setRawSlice(wrappedBooleanArray(columnVector.getValues(), 0, batchSize));
            }
            catch (Exception e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
        }
    }

    private final class LazyLongBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final MessageType columnSchema;
        private final PrimitiveTypeName parquetTypeName;

        public LazyLongBlockLoader(MessageType columnSchema, int batchSize, PrimitiveTypeName parquetTypeName)
        {
            this.batchSize = batchSize;
            this.columnSchema = columnSchema;
            this.parquetTypeName = parquetTypeName;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                LongColumnVector columnVector = new LongColumnVector(parquetTypeName);
                parquetReader.readColumn(columnVector, columnSchema);
                block.setNullVector(columnVector.getIsNull());
                block.setRawSlice(wrappedLongArray(columnVector.getValues(), 0, batchSize));
            }
            catch (Exception e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
        }
    }

    private final class LazyDoubleBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final MessageType columnSchema;
        private final int batchSize;
        private final PrimitiveTypeName parquetTypeName;

        public LazyDoubleBlockLoader(MessageType columnSchema, int batchSize, PrimitiveTypeName parquetTypeName)
        {
            this.batchSize = batchSize;
            this.columnSchema = columnSchema;
            this.parquetTypeName = parquetTypeName;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                DoubleColumnVector columnVector = new DoubleColumnVector(parquetTypeName);
                parquetReader.readColumn(columnVector, columnSchema);
                block.setNullVector(columnVector.getIsNull());
                block.setRawSlice(wrappedDoubleArray(columnVector.getValues(), 0, batchSize));
            }
            catch (Exception e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
        }
    }

    private final class LazySliceBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final int expectedBatchId = batchId;
        private final MessageType columnSchema;
        private final int batchSize;

        public LazySliceBlockLoader(MessageType columnSchema, int batchSize)
        {
            this.batchSize = batchSize;
            this.columnSchema = columnSchema;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                SliceColumnVector columnVector = new SliceColumnVector();
                parquetReader.readColumn(columnVector, columnSchema);
                block.setValues(columnVector.getValues());
            }
            catch (Exception e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
        }
    }
}

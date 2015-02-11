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
import com.facebook.presto.orc.BooleanVector;
import com.facebook.presto.orc.DoubleVector;
import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.LazyFixedWidthBlock;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.orc.Vector.MAX_VECTOR_LENGTH;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OrcPageSource
        implements ConnectorPageSource
{
    private final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;

    private final List<String> columnNames;
    private final List<Type> types;
    private final boolean[] isStructuralType;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private long completedBytes;

    private int batchId;
    private boolean closed;

    public OrcPageSource(
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        this.recordReader = checkNotNull(recordReader, "recordReader is null");
        this.orcDataSource = checkNotNull(orcDataSource, "orcDataSource is null");

        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(checkNotNull(partitionKeys, "partitionKeys is null"), HivePartitionKey::getName);

        int size = checkNotNull(columns, "columns is null").size();

        this.isStructuralType = new boolean[size];

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
            isStructuralType[columnIndex] = ARRAY.equals(typeBase) || MAP.equals(typeBase) || ROW.equals(typeBase);

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(name);
                checkArgument(partitionKey != null, "No value provided for partition key %s", name);

                byte[] bytes = partitionKey.getValue().getBytes(UTF_8);

                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

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
                else if (type.equals(DATE)) {
                    long value = datePartitionKey(partitionKey.getValue(), name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        DATE.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(TIMESTAMP)) {
                    long value = timestampPartitionKey(partitionKey.getValue(), hiveStorageTimeZone, name);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        TIMESTAMP.writeLong(blockBuilder, value);
                    }
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for partition key: %s", type.getDisplayName(), name));
                }

                constantBlocks[columnIndex] = blockBuilder.build();
            }
            else if (!recordReader.isColumnPresent(column.getHiveColumnIndex())) {
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
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
        return recordReader.getSplitLength();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
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
                else if (BOOLEAN.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(BOOLEAN.getFixedSize(), batchSize, new LazyBooleanBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (DATE.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(DATE.getFixedSize(), batchSize, new LazyDateBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (BIGINT.equals(type) || TIMESTAMP.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(((FixedWidthType) type).getFixedSize(), batchSize, new LazyLongBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (DOUBLE.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(DOUBLE.getFixedSize(), batchSize, new LazyDoubleBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (VARCHAR.equals(type) || VARBINARY.equals(type) || isStructuralType[fieldId]) {
                    blocks[fieldId] = new LazySliceArrayBlock(batchSize, new LazySliceBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type);
                }
            }
            Page page = new Page(batchSize, blocks);

            long newCompletedBytes = (long) (recordReader.getSplitLength() * recordReader.getProgress());
            completedBytes = min(recordReader.getSplitLength(), max(completedBytes, newCompletedBytes));

            return page;
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

    private final class LazyBooleanBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyBooleanBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                BooleanVector vector = new BooleanVector(batchSize);
                recordReader.readVector(hiveColumnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedBooleanArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw propagateException(e);
            }
        }
    }

    private final class LazyDateBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyDateBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                LongVector vector = new LongVector(batchSize);
                recordReader.readVector(hiveColumnIndex, vector);
                block.setNullVector(vector.isNull);

                // Presto stores dates as ints in memory, so convert to int array
                // TODO to add an ORC int vector
                int[] days = new int[batchSize];
                for (int i = 0; i < batchSize; i++) {
                    days[i] = (int) vector.vector[i];
                }

                block.setRawSlice(wrappedIntArray(days, 0, batchSize));
            }
            catch (IOException e) {
                throw propagateException(e);
            }
        }
    }

    private final class LazyLongBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyLongBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                LongVector vector = new LongVector(batchSize);
                recordReader.readVector(hiveColumnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedLongArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw propagateException(e);
            }
        }
    }

    private final class LazyDoubleBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;

        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyDoubleBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                DoubleVector vector = new DoubleVector(batchSize);
                recordReader.readVector(hiveColumnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedDoubleArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw propagateException(e);
            }
        }
    }

    private final class LazySliceBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final int expectedBatchId = batchId;

        private final int batchSize;
        private final int hiveColumnIndex;

        public LazySliceBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                SliceVector vector = new SliceVector(batchSize);
                recordReader.readVector(hiveColumnIndex, vector);
                block.setValues(vector.vector);
            }
            catch (IOException e) {
                throw propagateException(e);
            }
        }
    }

    private static RuntimeException propagateException(IOException e)
    {
        if (e instanceof OrcCorruptionException) {
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        throw new PrestoException(HIVE_CURSOR_ERROR, e);
    }
}
